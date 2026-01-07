# app/repo_finanzas_db.py
from datetime import datetime, timedelta
from decimal import Decimal
from sqlalchemy import func

from .database import SessionLocal
from app.models import FacturaCXC, FacturaCXP, Entidad


def _month_bounds(year: int, month: int):
    start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0)
    return start, end


def _window_bounds(end: datetime, window_days: int) -> tuple[datetime, datetime]:
    window_days = int(window_days or 0)
    if window_days <= 0:
        window_days = 90
    return end - timedelta(days=window_days), end


def _saldo(monto, pagado) -> Decimal:
    m = Decimal(monto or 0)
    p = Decimal(pagado or 0)
    s = m - p
    return s if s > 0 else Decimal("0")


class FinanzasRepoDB:
    # ---------- Helpers internos ----------
    def _ar_end(self, db, end: datetime) -> Decimal:
        ar = Decimal("0")
        q = db.query(FacturaCXC).filter(FacturaCXC.fecha_emision < end)
        for f in q:
            ar += _saldo(f.monto, f.monto_pagado)
        return ar

    def _ap_end(self, db, end: datetime) -> Decimal:
        ap = Decimal("0")
        q = db.query(FacturaCXP).filter(FacturaCXP.fecha_emision < end)
        for f in q:
            ap += _saldo(f.monto, f.monto_pagado)
        return ap

    def _sales_between(self, db, start: datetime, end: datetime) -> Decimal:
        s = Decimal("0")
        q = db.query(FacturaCXC).filter(
            FacturaCXC.fecha_emision >= start,
            FacturaCXC.fecha_emision < end
        )
        for f in q:
            s += Decimal(f.monto or 0)
        return s

    def _purchases_between(self, db, start: datetime, end: datetime) -> Decimal:
        p = Decimal("0")
        q = db.query(FacturaCXP).filter(
            FacturaCXP.fecha_emision >= start,
            FacturaCXP.fecha_emision < end
        )
        for f in q:
            p += Decimal(f.monto or 0)
        return p

    # ---------- Helpers de robustez ----------
    def _required_denom(self, end_balance: Decimal, min_abs_denom: Decimal, min_ratio: Decimal) -> Decimal:
        """
        Umbral mínimo requerido para que el denominador (ventas/compras) sea "representativo".
        required = max(min_abs_denom, end_balance * min_ratio)
        """
        try:
            min_abs = Decimal(min_abs_denom)
        except Exception:
            min_abs = Decimal("1")
        try:
            ratio = Decimal(min_ratio)
        except Exception:
            ratio = Decimal("0")

        if ratio < 0:
            ratio = Decimal("0")

        rel = (end_balance * ratio) if end_balance and ratio else Decimal("0")
        return max(min_abs, rel)

    # ---------- DSO ----------
    def dso(
        self,
        year: int,
        month: int,
        window_days: int = 90,
        min_denominator: Decimal = Decimal("1"),          # compat (si alguien lo usa)
        min_abs_denom: Decimal = Decimal("10000"),        # ✅ nuevo: umbral absoluto
        min_ratio: Decimal = Decimal("0.10"),             # ✅ nuevo: umbral relativo vs AR_end
    ) -> dict:
        """
        1) Intenta DSO mensual si ventas del mes son suficientes (no solo >0).
        2) Si ventas del mes son 0 o "muy pequeñas", fallback a trailing window_days (default 90d).

        Retorna dict:
          { value, method, reason, window:{start,end,days}, denom, ar_end, required_denom }
        """
        m_start, m_end = _month_bounds(year, month)
        t_start, t_end = _window_bounds(m_end, window_days)

        db = SessionLocal()
        try:
            ar_end = self._ar_end(db, m_end)

            required = self._required_denom(ar_end, min_abs_denom, min_ratio)
            try:
                required = max(required, Decimal(min_denominator))
            except Exception:
                pass

            sales_month = self._sales_between(db, m_start, m_end)
            if sales_month >= required:
                days = (m_end - m_start).days
                value = float((ar_end / sales_month) * Decimal(days))
                return {
                    "value": value,
                    "method": "month",
                    "reason": None,
                    "window": {"start": m_start.isoformat(), "end": m_end.isoformat(), "days": int(days)},
                    "denom": float(sales_month),
                    "ar_end": float(ar_end),
                    "required_denom": float(required),
                }

            sales_trailing = self._sales_between(db, t_start, t_end)
            if sales_trailing < required:
                return {
                    "value": None,
                    "method": "trailing_90d",
                    "reason": (
                        "Ventas insuficientes para estimar DSO con confianza "
                        f"(mes={float(sales_month):.2f}, trailing={float(sales_trailing):.2f}, "
                        f"required={float(required):.2f})."
                    ),
                    "window": {"start": t_start.isoformat(), "end": t_end.isoformat(), "days": int(window_days)},
                    "denom": float(sales_trailing),
                    "ar_end": float(ar_end),
                    "required_denom": float(required),
                }

            value = float((ar_end / sales_trailing) * Decimal(int(window_days)))
            return {
                "value": value,
                "method": "trailing_90d",
                "reason": (
                    "Ventas del mes insuficientes; se usó ventana trailing para estimar DSO "
                    f"(mes={float(sales_month):.2f} < required={float(required):.2f})."
                ),
                "window": {"start": t_start.isoformat(), "end": t_end.isoformat(), "days": int(window_days)},
                "denom": float(sales_trailing),
                "ar_end": float(ar_end),
                "required_denom": float(required),
            }
        finally:
            db.close()

    # ---------- DPO ----------
    def dpo(
        self,
        year: int,
        month: int,
        window_days: int = 90,
        min_denominator: Decimal = Decimal("1"),          # compat
        min_abs_denom: Decimal = Decimal("10000"),        # ✅ nuevo: umbral absoluto
        min_ratio: Decimal = Decimal("0.10"),             # ✅ nuevo: umbral relativo vs AP_end
    ) -> dict:
        """
        Igual que DSO pero para compras.
        Retorna dict:
          { value, method, reason, window:{start,end,days}, denom, ap_end, required_denom }
        """
        m_start, m_end = _month_bounds(year, month)
        t_start, t_end = _window_bounds(m_end, window_days)

        db = SessionLocal()
        try:
            ap_end = self._ap_end(db, m_end)

            required = self._required_denom(ap_end, min_abs_denom, min_ratio)
            try:
                required = max(required, Decimal(min_denominator))
            except Exception:
                pass

            purchases_month = self._purchases_between(db, m_start, m_end)
            if purchases_month >= required:
                days = (m_end - m_start).days
                value = float((ap_end / purchases_month) * Decimal(days))
                return {
                    "value": value,
                    "method": "month",
                    "reason": None,
                    "window": {"start": m_start.isoformat(), "end": m_end.isoformat(), "days": int(days)},
                    "denom": float(purchases_month),
                    "ap_end": float(ap_end),
                    "required_denom": float(required),
                }

            purchases_trailing = self._purchases_between(db, t_start, t_end)
            if purchases_trailing < required:
                return {
                    "value": None,
                    "method": "trailing_90d",
                    "reason": (
                        "Compras insuficientes para estimar DPO con confianza "
                        f"(mes={float(purchases_month):.2f}, trailing={float(purchases_trailing):.2f}, "
                        f"required={float(required):.2f})."
                    ),
                    "window": {"start": t_start.isoformat(), "end": t_end.isoformat(), "days": int(window_days)},
                    "denom": float(purchases_trailing),
                    "ap_end": float(ap_end),
                    "required_denom": float(required),
                }

            value = float((ap_end / purchases_trailing) * Decimal(int(window_days)))
            return {
                "value": value,
                "method": "trailing_90d",
                "reason": (
                    "Compras del mes insuficientes; se usó ventana trailing para estimar DPO "
                    f"(mes={float(purchases_month):.2f} < required={float(required):.2f})."
                ),
                "window": {"start": t_start.isoformat(), "end": t_end.isoformat(), "days": int(window_days)},
                "denom": float(purchases_trailing),
                "ap_end": float(ap_end),
                "required_denom": float(required),
            }
        finally:
            db.close()

    # ---------- CXC-03: vencimientos en rango ----------
    def cxc_due_between(self, start: datetime, end: datetime) -> dict:
        """
        Cuenta facturas CxC cuyo vencimiento (fecha_limite) cae dentro de [start, end]
        y calcula el saldo pendiente total.
        """
        if start.tzinfo:
            start = start.replace(tzinfo=None)
        if end.tzinfo:
            end = end.replace(tzinfo=None)

        db = SessionLocal()
        try:
            q = (
                db.query(FacturaCXC)
                .filter(
                    FacturaCXC.fecha_limite >= start,
                    FacturaCXC.fecha_limite <= end,
                    FacturaCXC.pagada == False,
                )
            )

            count = 0
            total = Decimal("0")

            for f in q:
                saldo = _saldo(f.monto, f.monto_pagado)
                if saldo > 0:
                    count += 1
                    total += saldo

            return {
                "count": count,
                "total": float(total),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "source": "db",
            }
        finally:
            db.close()

    # ---------- CXC-04: Top N clientes por saldo CxC abierto a una fecha ----------
    def cxc_top_clients_open(self, as_of: datetime, limit: int = 5) -> dict:
        """
        Top N clientes por saldo abierto CxC (saldo pendiente > 0) al 'as_of'.

        Nota: como la BD no tiene historial de pagos por fecha, esto usa:
          - facturas emitidas <= as_of
          - pagada = false
          - saldo actual (monto - coalesce(monto_pagado,0))
        Agrupa por id_entidad_cliente y devuelve nombre desde tabla entidad.
        """
        if as_of.tzinfo:
            as_of = as_of.replace(tzinfo=None)

        limit = int(limit or 5)
        if limit <= 0:
            limit = 5

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0), 0)

            # nombre: preferí nombre_comercial, si no nombre_legal
            nombre_expr = func.coalesce(Entidad.nombre_comercial, Entidad.nombre_legal).label("cliente_nombre")

            q = (
                db.query(
                    FacturaCXC.id_entidad_cliente.label("id_entidad_cliente"),
                    nombre_expr,
                    func.count(FacturaCXC.id_cxc).label("count"),
                    func.sum(saldo_expr).label("saldo_total"),
                )
                .join(Entidad, Entidad.id_entidad == FacturaCXC.id_entidad_cliente)
                .filter(
                    FacturaCXC.fecha_emision <= as_of,
                    FacturaCXC.pagada == False,
                    saldo_expr > 0,
                )
                .group_by(FacturaCXC.id_entidad_cliente, nombre_expr)
                .order_by(func.sum(saldo_expr).desc())
                .limit(limit)
            )

            rows = []
            for cid, nombre, cnt, saldo_total in q.all():
                rows.append({
                    "id_entidad_cliente": int(cid or 0),
                    "cliente_nombre": str(nombre or "").strip() or f"Cliente #{int(cid or 0)}",
                    "count": int(cnt or 0),
                    "saldo_total": float(saldo_total or 0),
                })

            return {
                "as_of": as_of.isoformat(),
                "limit": limit,
                "rows": rows,
                "source": "db",
            }
        finally:
            db.close()

    # ---------- ✅ CXC-06: Facturas CxC que vencen en una fecha (lista + saldos) ----------
    def cxc_invoices_due_on(self, day: datetime) -> dict:
        """
        Lista facturas CxC cuyo vencimiento (fecha_limite) cae en el día indicado (00:00:00 - 23:59:59),
        con su saldo pendiente y el nombre del cliente (Entidad).

        Devuelve:
          {
            "date": "YYYY-MM-DD",
            "count": int,
            "total": float,
            "rows": [
              {
                "id_cxc": int,
                "id_entidad_cliente": int,
                "cliente_nombre": str,
                "fecha_limite": "YYYY-MM-DD",
                "saldo_total": float
              }, ...
            ],
            "source": "db"
          }
        """
        if day.tzinfo:
            day = day.replace(tzinfo=None)

        day_start = datetime(day.year, day.month, day.day, 0, 0, 0)
        day_end = datetime(day.year, day.month, day.day, 23, 59, 59)

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0), 0).label("saldo_total")
            nombre_expr = func.coalesce(Entidad.nombre_comercial, Entidad.nombre_legal).label("cliente_nombre")

            q = (
                db.query(
                    FacturaCXC.id_cxc.label("id_cxc"),
                    FacturaCXC.id_entidad_cliente.label("id_entidad_cliente"),
                    nombre_expr,
                    FacturaCXC.fecha_limite.label("fecha_limite"),
                    saldo_expr,
                )
                .join(Entidad, Entidad.id_entidad == FacturaCXC.id_entidad_cliente)
                .filter(
                    FacturaCXC.pagada == False,
                    FacturaCXC.fecha_limite >= day_start,
                    FacturaCXC.fecha_limite <= day_end,
                    saldo_expr > 0,
                )
                .order_by(saldo_expr.desc())
            )

            rows = []
            total = Decimal("0")
            for id_cxc, cid, nombre, fecha_limite, saldo_total in q.all():
                st = Decimal(saldo_total or 0)
                total += st
                rows.append({
                    "id_cxc": int(id_cxc or 0),
                    "id_entidad_cliente": int(cid or 0),
                    "cliente_nombre": str(nombre or "").strip() or f"Cliente #{int(cid or 0)}",
                    "fecha_limite": (fecha_limite.date().isoformat() if isinstance(fecha_limite, datetime) else str(fecha_limite)[:10]),
                    "saldo_total": float(st),
                })

            return {
                "date": day_start.date().isoformat(),
                "count": int(len(rows)),
                "total": float(total),
                "rows": rows,
                "source": "db",
            }
        finally:
            db.close()

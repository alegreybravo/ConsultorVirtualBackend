# app/repo_finanzas_db.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import func, cast, literal, or_, text
from sqlalchemy.sql import case as sql_case
from sqlalchemy.types import String

from .database import SessionLocal
from app.models import FacturaCXC, FacturaCXP, Entidad


def _to_date(v: Any) -> date:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    # strings ISO: "2025-10-29" o "2025-10-29T23:59:59..."
    s = str(v or "").strip()
    if not s:
        raise ValueError("as_of vacío")
    return datetime.fromisoformat(s[:19]).date() if "T" in s else date.fromisoformat(s[:10])


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
            FacturaCXC.fecha_emision < end,
        )
        for f in q:
            s += Decimal(f.monto or 0)
        return s

    def _purchases_between(self, db, start: datetime, end: datetime) -> Decimal:
        p = Decimal("0")
        q = db.query(FacturaCXP).filter(
            FacturaCXP.fecha_emision >= start,
            FacturaCXP.fecha_emision < end,
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
        min_abs_denom: Decimal = Decimal("10000"),        # ✅ umbral absoluto
        min_ratio: Decimal = Decimal("0.10"),             # ✅ umbral relativo vs AR_end
    ) -> dict:
        """
        1) Intenta DSO mensual si ventas del mes son suficientes.
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
        min_abs_denom: Decimal = Decimal("10000"),        # ✅ umbral absoluto
        min_ratio: Decimal = Decimal("0.10"),             # ✅ umbral relativo vs AP_end
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
        if start and start.tzinfo:
            start = start.replace(tzinfo=None)
        if end and end.tzinfo:
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
        """
        if as_of and as_of.tzinfo:
            as_of = as_of.replace(tzinfo=None)

        limit = int(limit or 5)
        if limit <= 0:
            limit = 5

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0), 0)
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

    # ---------- CXC-06: Facturas CxC que vencen en una fecha ----------
    def cxc_invoices_due_on(self, day: datetime) -> dict:
        """
        Lista facturas CxC cuyo vencimiento (fecha_limite) cae en el día indicado.
        """
        if day and day.tzinfo:
            day = day.replace(tzinfo=None)

        day_start = datetime(day.year, day.month, day.day, 0, 0, 0)
        day_end = datetime(day.year, day.month, day.day, 23, 59, 59)

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0),
                0
            ).label("saldo_total")
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
                    "fecha_limite": (
                        fecha_limite.date().isoformat()
                        if isinstance(fecha_limite, datetime)
                        else str(fecha_limite)[:10]
                    ),
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

    # ---------- CXC-07: Facturas CxC con pago parcial ----------
    def cxc_partial_payments(self, start: datetime, end: datetime) -> dict:
        """
        Facturas CxC con pago parcial:
        monto_pagado > 0 AND saldo > 0
        """
        if start and start.tzinfo:
            start = start.replace(tzinfo=None)
        if end and end.tzinfo:
            end = end.replace(tzinfo=None)

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0),
                0
            )

            rows = (
                db.query(
                    FacturaCXC.id_cxc,
                    FacturaCXC.id_entidad_cliente,
                    Entidad.nombre_comercial,
                    FacturaCXC.fecha_emision,
                    FacturaCXC.fecha_limite,
                    FacturaCXC.monto,
                    FacturaCXC.monto_pagado,
                    saldo_expr.label("saldo_pendiente"),
                )
                .join(Entidad, Entidad.id_entidad == FacturaCXC.id_entidad_cliente)
                .filter(
                    FacturaCXC.pagada == False,
                    FacturaCXC.monto_pagado > 0,
                    saldo_expr > 0,
                    FacturaCXC.fecha_emision >= start,
                    FacturaCXC.fecha_emision <= end,
                )
                .order_by(saldo_expr.desc())
                .all()
            )

            total_saldo = sum(r.saldo_pendiente for r in rows)

            return {
                "count": len(rows),
                "total_saldo_pendiente": float(total_saldo),
                "rows": [
                    {
                        "id_cxc": int(r.id_cxc or 0),
                        "id_entidad_cliente": int(r.id_entidad_cliente or 0),
                        "cliente": (r.nombre_comercial or "").strip(),
                        "monto_original": float(r.monto or 0),
                        "monto_pagado": float(r.monto_pagado or 0),
                        "saldo_pendiente": float(r.saldo_pendiente or 0),
                        "fecha_emision": r.fecha_emision.date().isoformat() if r.fecha_emision else None,
                        "fecha_limite": r.fecha_limite.date().isoformat() if r.fecha_limite else None,
                    }
                    for r in rows
                ],
                "source": "db",
            }
        finally:
            db.close()

    # ---------- ✅ CXC-08: Saldo abierto CxC de un cliente al corte (as_of) ----------
    def cxc_customer_open_balance_on(self, customer_name: str, as_of: datetime) -> dict:
        """
        CXC-08: Saldo abierto CxC de un cliente al corte (as_of).
        """
        if not customer_name:
            return {"error": "customer_name requerido", "source": "db"}

        if as_of and as_of.tzinfo:
            as_of = as_of.replace(tzinfo=None)

        name_raw = str(customer_name).strip()
        if not name_raw:
            return {"error": "customer_name vacío", "source": "db"}

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                FacturaCXC.monto - func.coalesce(FacturaCXC.monto_pagado, 0),
                0
            )

            cust_id = None
            cust = None

            try:
                cust_id = int(name_raw)
            except Exception:
                cust_id = None

            if cust_id is None:
                needle = func.unaccent(func.lower(name_raw))
                legal = func.unaccent(func.lower(Entidad.nombre_legal))
                comercial = func.unaccent(func.lower(Entidad.nombre_comercial))

                cust = (
                    db.query(Entidad)
                    .filter(
                        or_(
                            legal.like(func.concat('%', needle, '%')),
                            comercial.like(func.concat('%', needle, '%')),
                        )
                    )
                    .order_by(Entidad.id_entidad.asc())
                    .first()
                )
                cust_id = cust.id_entidad if cust else None
            else:
                cust = db.query(Entidad).filter(Entidad.id_entidad == cust_id).first()

            if not cust_id:
                return {
                    "as_of": as_of.isoformat(),
                    "customer": name_raw,
                    "id_entidad": 0,
                    "saldo": 0.0,
                    "count_facturas": 0,
                    "source": "db",
                    "warning": "Cliente no encontrado (match por nombre/id falló).",
                }

            customer_display = (cust.nombre_legal or cust.nombre_comercial or name_raw) if cust else name_raw

            row = (
                db.query(
                    func.sum(saldo_expr).label("saldo"),
                    func.count(FacturaCXC.id_cxc).label("count_facturas"),
                )
                .filter(
                    FacturaCXC.pagada == False,
                    FacturaCXC.id_entidad_cliente == cust_id,
                    FacturaCXC.fecha_emision <= as_of,
                    saldo_expr > 0,
                )
                .first()
            )

            saldo_total = float((row.saldo or 0) if row else 0)
            count_facturas = int((row.count_facturas or 0) if row else 0)

            return {
                "as_of": as_of.isoformat(),
                "customer": str(customer_display),
                "id_entidad": int(cust_id or 0),
                "saldo": saldo_total,
                "count_facturas": count_facturas,
                "source": "db",
            }
        finally:
            db.close()

    # ---------- CXP-02: Aging CxP al corte ----------
    def cxp_aging_as_of(self, as_of: Any) -> Dict[str, Any]:
        """
        Aging CxP al corte (as_of) con buckets:
        No vencido, 1-30, 31-60, 61-90, 90+
        """
        as_of_date = _to_date(as_of)

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                (FacturaCXP.monto - func.coalesce(FacturaCXP.monto_pagado, 0)),
                0
            )

            days_over_expr = (literal(as_of_date) - func.date(FacturaCXP.fecha_limite))

            bucket_expr = sql_case(
                (days_over_expr <= 0, "No vencido"),
                (days_over_expr <= 30, "1-30"),
                (days_over_expr <= 60, "31-60"),
                (days_over_expr <= 90, "61-90"),
                else_="90+",
            ).label("bucket")

            q = (
                db.query(
                    bucket_expr,
                    func.sum(saldo_expr).label("total")
                )
                .filter(FacturaCXP.pagada.is_(False))
                .filter(saldo_expr > 0)
                .group_by(bucket_expr)
            )

            rows = [{"bucket": b, "total": float(t or 0)} for (b, t) in q.all()]

            buckets = {"No vencido": 0.0, "1-30": 0.0, "31-60": 0.0, "61-90": 0.0, "90+": 0.0}
            for r in rows:
                b = r.get("bucket")
                if b in buckets:
                    buckets[b] = float(r.get("total") or 0.0)

            total = sum(buckets.values())
            overdue = buckets["1-30"] + buckets["31-60"] + buckets["61-90"] + buckets["90+"]

            return {
                "as_of": as_of_date.isoformat(),
                "buckets": buckets,
                "total_open": float(total),
                "total_overdue": float(overdue),
                "rows": rows,
                "source": "db",
            }
        finally:
            db.close()

    # ---------- CXP-03: Top proveedores por saldo abierto ----------
    def cxp_top_suppliers_open(self, as_of: datetime, limit: int = 5) -> dict:
        """
        CXP-03: Top N proveedores por saldo CxP abierto.
        """
        if as_of.tzinfo:
            as_of = as_of.replace(tzinfo=None)

        limit = int(limit or 5)
        if limit <= 0:
            limit = 5

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                FacturaCXP.monto - func.coalesce(FacturaCXP.monto_pagado, 0),
                0
            )

            nombre_legal_expr = Entidad.nombre_legal.label("nombre_legal")

            q = (
                db.query(
                    nombre_legal_expr,
                    func.sum(saldo_expr).label("saldo"),
                )
                .join(
                    Entidad,
                    cast(Entidad.id_entidad, String) == cast(FacturaCXP.id_entidad_proveedor, String)
                )
                .filter(
                    FacturaCXP.pagada == False,
                    saldo_expr > 0,
                )
                .group_by(nombre_legal_expr)
                .order_by(func.sum(saldo_expr).desc())
                .limit(limit)
            )

            rows = [{"proveedor": n, "saldo": float(s or 0)} for (n, s) in q.all()]

            return {
                "as_of": as_of.isoformat(),
                "limit": limit,
                "rows": rows,
                "source": "db",
            }
        finally:
            db.close()

    # ---------- CXP-05: saldo abierto de proveedor al corte ----------
    def cxp_supplier_open_balance_on(self, supplier_name: str, as_of: datetime) -> dict:
        """
        CXP-05: saldo abierto con proveedor X al corte.
        """
        if not supplier_name:
            return {"error": "supplier_name requerido", "source": "db"}

        if as_of.tzinfo:
            as_of = as_of.replace(tzinfo=None)

        name = str(supplier_name).strip()

        db = SessionLocal()
        try:
            saldo_expr = func.greatest(
                FacturaCXP.monto - func.coalesce(FacturaCXP.monto_pagado, 0),
                0
            )

            q = (
                db.query(func.sum(saldo_expr).label("saldo"))
                .join(
                    Entidad,
                    cast(Entidad.id_entidad, String) == cast(FacturaCXP.id_entidad_proveedor, String)
                )
                .filter(
                    FacturaCXP.pagada == False,
                    Entidad.nombre_legal == name,
                    saldo_expr > 0,
                )
            )

            row = q.first()
            saldo = float((row.saldo if row else 0) or 0)

            return {
                "as_of": as_of.date().isoformat(),
                "supplier": name,
                "saldo": saldo,
                "source": "db",
            }
        finally:
            db.close()

    # ---------- SQL helper ----------
    def _fetchone(self, sql: str, params: dict | None = None) -> dict | None:
        """
        Ejecuta SQL y retorna 1 fila como dict (mappings) o None.
        """
        db = SessionLocal()
        try:
            res = db.execute(text(sql), params or {})
            row = res.mappings().first()
            return dict(row) if row else None
        finally:
            db.close()

    # ---------- ✅ CXP-01: resumen de abiertas + saldo al corte ----------
    def cxp_open_summary_as_of(self, as_of: Any) -> dict:
        """
        CXP-01: ¿Cuántas facturas CxP están abiertas al corte y el saldo total?
        Nota: como no hay historial de pagos por fecha, se usa saldo actual.
        """
        as_of_date = _to_date(as_of)
        as_of_10 = as_of_date.isoformat()

        sql = """
        SELECT
            COUNT(*) AS abiertas,
            COALESCE(SUM(monto - monto_pagado), 0) AS saldo
        FROM agente_virtual.factura_cxp
        WHERE pagada = FALSE
          AND fecha_emision::date <= :as_of;
        """

        row = self._fetchone(sql, {"as_of": as_of_10}) or {"abiertas": 0, "saldo": 0}

        return {
            "as_of": as_of_10,
            "abiertas": int(row.get("abiertas") or 0),
            "saldo": float(row.get("saldo") or 0.0),
            "source": "db",
        }

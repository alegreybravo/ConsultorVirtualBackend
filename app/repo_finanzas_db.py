# app/repo_finanzas_db.py
from datetime import datetime, timedelta
from decimal import Decimal

from .database import SessionLocal
from app.models import FacturaCXC, FacturaCXP


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

            # Umbral requerido para considerar "mes" válido
            required = self._required_denom(ar_end, min_abs_denom, min_ratio)
            # Mantener compat: si min_denominator es mayor, respétalo
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

            # fallback trailing
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

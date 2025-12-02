# app/repo_finanzas_db.py
from datetime import datetime, date
from decimal import Decimal
from collections import defaultdict

from .database import SessionLocal
from app.models import FacturaCXC, FacturaCXP

def _month_bounds(year: int, month: int):
    """[inicio, fin) en datetime para comparar contra columnas timestamp sin perder registros por hora."""
    start = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0)
    return start, end

class FinanzasRepoDB:
    """Consultas contra factura_cxc / factura_cxp para CxC, CxP, DSO/DPO y aging."""

    # ---- CxC ----
    def cxc_balance_by_month(self, year: int, month: int) -> Decimal:
        start, end = _month_bounds(year, month)
        db = SessionLocal()
        try:
            q = (
                db.query(FacturaCXC)
                .filter(FacturaCXC.fecha_emision >= start,
                        FacturaCXC.fecha_emision < end)
            )
            total = Decimal("0")
            for f in q:
                monto = Decimal(f.monto or 0)
                pagado = Decimal(f.monto_pagado or 0)
                saldo = monto - pagado
                total += saldo
            return total
        finally:
            db.close()

    def cxc_aging(self, today: date | None = None) -> dict[str, float]:
        """Aging por buckets usando fecha_limite; suma saldos pendientes."""
        today = today or date.today()
        db = SessionLocal()
        buckets = defaultdict(Decimal)
        try:
            for f in db.query(FacturaCXC):
                monto = Decimal(f.monto or 0)
                pagado = Decimal(f.monto_pagado or 0)
                saldo = monto - pagado
                if saldo <= 0:
                    continue
                if not f.fecha_limite:
                    buckets["Sin vencimiento"] += saldo
                    continue
                # fecha_limite es DateTime → convierto a date
                days = (today - f.fecha_limite.date()).days
                if days <= 0:
                    buckets["No vencido"] += saldo
                elif days <= 30:
                    buckets["1-30"] += saldo
                elif days <= 60:
                    buckets["31-60"] += saldo
                elif days <= 90:
                    buckets["61-90"] += saldo
                else:
                    buckets["+90"] += saldo
            return {k: float(v) for k, v in buckets.items()}  # JSON-friendly
        finally:
            db.close()

    def dso(self, year: int, month: int, credit_sales: Decimal | None = None) -> float:
        """DSO ≈ (CxC promedio / ventas a crédito) * días del período.
        Si no pasas 'credit_sales', usamos sum(monto) del período como aproximación."""
        start, end = _month_bounds(year, month)
        db = SessionLocal()
        try:
            ar_end = Decimal("0")
            sales = Decimal("0")
            q = db.query(FacturaCXC).filter(FacturaCXC.fecha_emision >= start,
                                            FacturaCXC.fecha_emision < end)
            for f in q:
                monto = Decimal(f.monto or 0)
                pagado = Decimal(f.monto_pagado or 0)
                sales += monto
                ar_end += (monto - pagado)
            ar_avg = ar_end  # aproximación (si quieres, promedia con mes anterior)
            denom = Decimal(credit_sales) if credit_sales is not None else (sales or Decimal("1"))
            days = (end - start).days
            return float((ar_avg / denom) * days)
        finally:
            db.close()

    # ---- CxP ----
    def cxp_balance_by_month(self, year: int, month: int) -> Decimal:
        start, end = _month_bounds(year, month)
        db = SessionLocal()
        try:
            q = (
                db.query(FacturaCXP)
                .filter(FacturaCXP.fecha_emision >= start,
                        FacturaCXP.fecha_emision < end)
            )
            total = Decimal("0")
            for f in q:
                monto = Decimal(f.monto or 0)
                pagado = Decimal(f.monto_pagado or 0)
                saldo = monto - pagado
                total += saldo
            return total
        finally:
            db.close()

    def dpo(self, year: int, month: int, credit_purchases: Decimal | None = None) -> float:
        """DPO ≈ (CxP promedio / compras a crédito) * días del período."""
        start, end = _month_bounds(year, month)
        db = SessionLocal()
        try:
            ap_end = Decimal("0")
            purchases = Decimal("0")
            q = db.query(FacturaCXP).filter(FacturaCXP.fecha_emision >= start,
                                            FacturaCXP.fecha_emision < end)
            for f in q:
                monto = Decimal(f.monto or 0)
                pagado = Decimal(f.monto_pagado or 0)
                purchases += monto
                ap_end += (monto - pagado)
            ap_avg = ap_end
            denom = Decimal(credit_purchases) if credit_purchases is not None else (purchases or Decimal("1"))
            days = (end - start).days
            return float((ap_avg / denom) * days)
        finally:
            db.close()

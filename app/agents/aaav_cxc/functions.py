# app/agents/aaav_cxc/functions.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import re
import pandas as pd
from dateutil import parser as dateparser

from ...state import GlobalState
from ...tools.calc_kpis import month_window
from ...tools.schema_validate import validate_with

from app.database import SessionLocal
from app.models import FacturaCXC, Entidad
from app.repo_finanzas_db import FinanzasRepoDB

SCHEMA = "app/schemas/aaav_cxc_schema.json"


# ---------------------------------------------------------------------
# Período
# ---------------------------------------------------------------------
@dataclass
class PeriodWindow:
    text: str
    start: pd.Timestamp
    end: pd.Timestamp


def resolve_period(payload: Dict[str, Any], state: GlobalState) -> PeriodWindow:
    """
    Acepta:
      - payload["period_range"] dict con ISO start/end (preferido, del router)
      - payload["period"] en formato 'YYYY-MM'
      - state.period (dict del router)
    """
    pr = payload.get("period_range") or getattr(state, "period", None)
    if isinstance(pr, dict) and pr.get("start") and pr.get("end"):
        start = pd.Timestamp(dateparser.isoparse(pr["start"]))
        end = pd.Timestamp(dateparser.isoparse(pr["end"]))
        text = pr.get("text") or f"{start.year:04d}-{start.month:02d}"
        return PeriodWindow(text=text, start=start, end=end)

    p = payload.get("period") or getattr(state, "period_raw", None)
    if isinstance(p, str) and len(p) == 7 and p[4] == "-":
        s, e, _ = month_window(p)
        return PeriodWindow(text=p, start=s, end=e)

    # Fallback: mes actual (TZ CR ya aplicada en calc_kpis si corresponde)
    today = pd.Timestamp.today(tz="America/Costa_Rica")
    ym = today.strftime("%Y-%m")
    s, e, _ = month_window(ym)
    return PeriodWindow(text=ym, start=s, end=e)


def resolve_ref_date(win: PeriodWindow) -> date:
    """
    Usa 'fecha:YYYY-MM-DD' si viene en win.text; si no, usa win.end.date().
    """
    try:
        m = re.search(r"fecha:(\d{4}-\d{2}-\d{2})", str(win.text))
        if m:
            from datetime import date as _date
            return _date.fromisoformat(m.group(1))
    except Exception:
        pass
    return win.end.date()


# ---------------------------------------------------------------------
# Helpers DB (CXC)
# ---------------------------------------------------------------------
def _saldo_cxc(f: FacturaCXC) -> Decimal:
    # saldo = monto - monto_pagado
    return Decimal((f.monto or 0) - (f.monto_pagado or 0))


def _aging_and_totals_db(ref_date: date) -> Tuple[Dict[str, float], float, float, int]:
    """
    Devuelve:
      - aging SOLO vencido con llaves normalizadas: 0_30, 31_60, 61_90, 90_plus
      - total_por_cobrar (saldo abierto)
      - por_vencer (no vencido, incluye 'sin fecha')
      - open_count (número de facturas abiertas > 0)
    """
    db = SessionLocal()
    overdue = {
        "0_30": Decimal("0"),
        "31_60": Decimal("0"),
        "61_90": Decimal("0"),
        "90_plus": Decimal("0"),
    }
    current = Decimal("0")     # no vencido (<= 0 días)
    no_due = Decimal("0")      # sin fecha_limite
    open_count = 0
    try:
        for f in db.query(FacturaCXC):
            saldo = _saldo_cxc(f)
            if saldo <= 0:
                continue
            open_count += 1
            # FECHA DE VENCIMIENTO EN TU TABLA: fecha_limite
            if not f.fecha_limite:
                no_due += saldo
                continue
            days = (ref_date - f.fecha_limite.date()).days
            if days <= 0:
                current += saldo
            elif days <= 30:
                overdue["0_30"] += saldo
            elif days <= 60:
                overdue["31_60"] += saldo
            elif days <= 90:
                overdue["61_90"] += saldo
            else:
                overdue["90_plus"] += saldo

        total_por_cobrar = float(current + no_due + sum(overdue.values()))
        por_vencer = float(current + no_due)
        return (
            {k: float(v) for k, v in overdue.items()},
            total_por_cobrar,
            por_vencer,
            open_count,
        )
    finally:
        db.close()


def _list_top_overdue_db(limit_n: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXC):
            saldo = float(_saldo_cxc(f))
            if saldo <= 0:
                continue
            days_over = 0
            if f.fecha_limite:
                days_over = max((ref_date - f.fecha_limite.date()).days, 0)
            if days_over <= 0:
                continue
            cliente = (
                f.cliente.nombre_legal
                if getattr(f, "cliente", None)
                else str(getattr(f, "id_entidad_cliente", ""))
            )

            rows.append(
                {
                    "invoice_id": f.numero_factura,
                    "customer": cliente,
                    "due_date": f.fecha_limite.date() if f.fecha_limite else None,
                    "days_overdue": days_over,
                    "outstanding": saldo,
                }
            )
        rows.sort(key=lambda r: (r["days_overdue"], r["outstanding"]), reverse=True)
        return rows[: int(limit_n)]
    finally:
        db.close()


def _customer_balance_db(name_or_id: str, ref_date: date):
    target = str(name_or_id).strip()
    db = SessionLocal()
    try:
        cust = (
            db.query(Entidad)
            .filter(Entidad.nombre_legal.ilike(target))
            .first()
        )
        cust_id = cust.id_entidad if cust else None
        if not cust_id:
            try:
                cust_id = int(target)
            except Exception:
                cust_id = None

        total = 0.0
        rows: List[Dict[str, Any]] = []
        q = db.query(FacturaCXC)
        if cust_id:
            q = q.filter(FacturaCXC.id_entidad_cliente == cust_id)

        for f in q:
            saldo = float(_saldo_cxc(f))
            if saldo <= 0:
                continue
            days_over = 0
            if f.fecha_limite:
                days_over = max((ref_date - f.fecha_limite.date()).days, 0)
            rows.append(
                {
                    "invoice_id": f.numero_factura,
                    "issue_date": f.fecha_emision.date() if f.fecha_emision else None,
                    "due_date": f.fecha_limite.date() if f.fecha_limite else None,
                    "days_overdue": days_over,
                    "outstanding": saldo,
                }
            )
            total += saldo
        return total, rows
    finally:
        db.close()


def _list_open_db(ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXC):
            saldo = float(_saldo_cxc(f))
            if saldo <= 0:
                continue
            due = f.fecha_limite.date() if f.fecha_limite else None
            days_over = max((ref_date - due).days, 0) if due else 0
            status = "paid/zero"
            if (
                saldo > 0
                and days_over == 0
                and due
                and (due - ref_date).days >= 0
            ):
                status = "open_on_time"
            elif saldo > 0 and days_over > 0:
                status = "overdue"
            cliente = (
                f.cliente.nombre_legal
                if getattr(f, "cliente", None)
                else str(getattr(f, "id_entidad_cliente", ""))
            )

            rows.append(
                {
                    "invoice_id": f.numero_factura,
                    "customer": cliente,
                    "due_date": due,
                    "status": status,
                    "days_overdue": days_over,
                    "outstanding": saldo,
                }
            )
        rows.sort(
            key=lambda r: (r["status"], -r["days_overdue"], -r["outstanding"])
        )
        return rows
    finally:
        db.close()


# ---------------------------------------------------------------------
# Construcción de contexto base (KPI + aging + totales)
# ---------------------------------------------------------------------
def build_context(win: PeriodWindow, ref_date: date) -> Dict[str, Any]:
    # 2) KPI base DSO
    repo = FinanzasRepoDB()
    try:
        kpi_dso = repo.dso(win.start.year, win.start.month)
    except Exception:
        kpi_dso = None

    # 3) Aging SOLO vencido + totales (con open_count)
    try:
        aging_overdue, total_por_cobrar, por_vencer, open_count = _aging_and_totals_db(
            ref_date
        )
    except Exception as e:
        return {"error": f"Error leyendo CxC DB: {e}"}

    # 4) Paquete normalizado
    data_norm = {
        "period": win.text,
        "kpi": {"DSO": kpi_dso},
        "aging": {
            "0_30": float(aging_overdue.get("0_30", 0.0)),
            "31_60": float(aging_overdue.get("31_60", 0.0)),
            "61_90": float(aging_overdue.get("61_90", 0.0)),
            "90_plus": float(aging_overdue.get("90_plus", 0.0)),
        },
        "total_por_cobrar": float(total_por_cobrar),
        "por_vencer": float(por_vencer),
        "current": float(por_vencer),  # alias
        "open_invoices": int(open_count),
    }

    # 5) Validación (no bloqueante)
    try:
        validate_with(SCHEMA, data_norm)
    except Exception:
        pass

    return {
        "period_window": win,
        "ref_date": ref_date,
        "kpi_dso": kpi_dso,
        "aging_overdue": aging_overdue,
        "total_por_cobrar": total_por_cobrar,
        "por_vencer": por_vencer,
        "open_count": open_count,
        "data_norm": data_norm,
    }


# ---------------------------------------------------------------------
# Acciones
# ---------------------------------------------------------------------
def action_metrics(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "summary": f"CxC calculado (DB) — {ctx['open_count']} facturas abiertas",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
    }


def action_top_overdue(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    n = params.get("n", 10)
    table = _list_top_overdue_db(n, ref_date)
    return {
        "summary": "Top facturas por cobrar vencidas (más urgentes)",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
        "result": {"action": "top_overdue", "table": table},
    }


def action_customer_balance(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    cust = params.get("customer")
    if not cust:
        return {"error": "Falta 'customer' en params"}

    total, table = _customer_balance_db(cust, ref_date)
    return {
        "summary": f"Saldo pendiente con el cliente '{cust}': {total:.2f}",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
        "result": {
            "action": "customer_balance",
            "total_outstanding": total,
            "table": table,
        },
    }


def action_list_open(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    table = _list_open_db(ref_date)
    return {
        "summary": "Cuentas por cobrar abiertas",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
        "result": {"action": "list_open", "table": table},
    }


def action_list_overdue(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]

    # Reutilizamos list_open y filtramos por vencidas
    table_all = _list_open_db(ref_date)
    overdue = [r for r in table_all if r.get("status") == "overdue"]

    p_min = int(params.get("min_days", 1))
    p_max = params.get("max_days")
    if p_max is not None:
        p_max = int(p_max)
        overdue = [
            r
            for r in overdue
            if p_min <= r.get("days_overdue", 0) <= p_max
        ]
    else:
        overdue = [r for r in overdue if r.get("days_overdue", 0) >= p_min]

    overdue.sort(
        key=lambda r: (r.get("days_overdue", 0), r.get("outstanding", 0.0)),
        reverse=True,
    )

    # Serializar fecha
    for r in overdue:
        d = r.get("due_date")
        if hasattr(d, "isoformat"):
            r["due_date"] = d.isoformat()

    # Agrupado por cliente
    by_customer_map: Dict[str, Dict[str, Any]] = {}
    for r in overdue:
        cust = r.get("customer") or "N/D"
        if cust not in by_customer_map:
            by_customer_map[cust] = {
                "customer": cust,
                "invoices": 0,
                "total_outstanding": 0.0,
            }
        by_customer_map[cust]["invoices"] += 1
        by_customer_map[cust]["total_outstanding"] += float(
            r.get("outstanding", 0.0)
        )
    by_customer = sorted(
        by_customer_map.values(),
        key=lambda x: x["total_outstanding"],
        reverse=True,
    )

    total_overdue = float(sum(r.get("outstanding", 0.0) for r in overdue))
    return {
        "summary": "Facturas CxC vencidas (detalle)",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
        "result": {
            "action": "list_overdue",
            "total_overdue": total_overdue,
            "count_overdue": len(overdue),
            "by_customer": by_customer,
            "table": overdue,
        },
    }


ACTIONS = {
    "metrics": action_metrics,
    "top_overdue": action_top_overdue,
    "customer_balance": action_customer_balance,
    "list_open": action_list_open,
    "list_overdue": action_list_overdue,
}


def run_action(
    action: str,
    payload: Dict[str, Any],
    params: Dict[str, Any],
    state: GlobalState,
) -> Dict[str, Any]:
    """
    Orquesta la ejecución de una acción del agente CxC:
      - Resuelve período
      - Construye contexto base (KPI, aging, totales)
      - Ejecuta la acción pedida
    """
    win = resolve_period(payload, state)
    ref_date = resolve_ref_date(win)
    ctx = build_context(win, ref_date)

    if isinstance(ctx, dict) and ctx.get("error"):
        # Devolvemos el error tal cual; el Agent añadirá el nombre del agente
        return {"error": ctx["error"]}

    handler = ACTIONS.get(action, action_metrics)
    return handler(ctx, params)

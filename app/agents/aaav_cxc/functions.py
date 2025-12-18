# app/agents/aaav_cxc/functions.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
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

    # Fallback: mes actual
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
# Helpers de serialización
# ---------------------------------------------------------------------
def _iso_date(d: Any) -> Optional[str]:
    """
    Normaliza fechas a 'YYYY-MM-DD' para que la API sea JSON-friendly.
    Acepta date/datetime/pandas timestamp/string; devuelve str o None.
    """
    if d is None:
        return None
    try:
        if hasattr(d, "date") and not isinstance(d, date):
            d = d.date()
        if hasattr(d, "isoformat"):
            return d.isoformat()
    except Exception:
        pass
    try:
        s = str(d).strip()
        if s:
            return s[:10]
    except Exception:
        pass
    return None


def _norm_open_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aplica normalizaciones comunes:
      - due_date -> iso string
      - issue_date -> iso string (si existe)
      - days_overdue -> int o None
    """
    out = dict(row)

    if "due_date" in out:
        out["due_date"] = _iso_date(out.get("due_date"))

    if "issue_date" in out:
        out["issue_date"] = _iso_date(out.get("issue_date"))

    d = out.get("days_overdue")
    if d is None:
        out["days_overdue"] = None
    else:
        try:
            out["days_overdue"] = int(d)
        except Exception:
            out["days_overdue"] = None

    return out


# ---------------------------------------------------------------------
# Helpers DB (CXC)
# ---------------------------------------------------------------------
def _saldo_cxc(f: FacturaCXC) -> Decimal:
    # saldo = monto - monto_pagado
    return Decimal((f.monto or 0) - (f.monto_pagado or 0))


def build_aging_cxc_db(ref_date: date) -> Dict[str, Any]:
    """
    Construye un aging NO ambiguo:
      - aging_overdue: solo vencido (ref_date > fecha_limite) por buckets de días VENCIDOS
      - aging_current: no vencido (ref_date <= fecha_limite) por buckets de días PARA VENCER
      - sin_fecha_limite: saldo sin fecha de vencimiento
      - totales y conteos

    Importante:
      - overdue_1_30 significa: vencido entre 1 y 30 días
      - current_0_7 significa: vence en 0 a 7 días (incluye hoy)
    """
    db = SessionLocal()

    aging_overdue = {
        "overdue_1_30": Decimal("0"),
        "overdue_31_60": Decimal("0"),
        "overdue_61_90": Decimal("0"),
        "overdue_90_plus": Decimal("0"),
    }

    aging_current = {
        "current_0_7": Decimal("0"),
        "current_8_15": Decimal("0"),
        "current_16_30": Decimal("0"),
        "current_31_plus": Decimal("0"),
    }

    sin_fecha_limite = Decimal("0")
    total_outstanding = Decimal("0")
    total_overdue = Decimal("0")
    total_current = Decimal("0")
    open_invoices = 0

    try:
        for f in db.query(FacturaCXC):
            saldo = _saldo_cxc(f)
            if saldo <= 0:
                continue

            open_invoices += 1
            total_outstanding += saldo

            if not f.fecha_limite:
                sin_fecha_limite += saldo
                continue

            due = f.fecha_limite.date()
            if ref_date > due:
                # vencido
                days_overdue = (ref_date - due).days
                total_overdue += saldo

                if 1 <= days_overdue <= 30:
                    aging_overdue["overdue_1_30"] += saldo
                elif 31 <= days_overdue <= 60:
                    aging_overdue["overdue_31_60"] += saldo
                elif 61 <= days_overdue <= 90:
                    aging_overdue["overdue_61_90"] += saldo
                else:
                    aging_overdue["overdue_90_plus"] += saldo
            else:
                # por vencer / vigente
                days_to_due = (due - ref_date).days
                total_current += saldo

                if 0 <= days_to_due <= 7:
                    aging_current["current_0_7"] += saldo
                elif 8 <= days_to_due <= 15:
                    aging_current["current_8_15"] += saldo
                elif 16 <= days_to_due <= 30:
                    aging_current["current_16_30"] += saldo
                else:
                    aging_current["current_31_plus"] += saldo

        return {
            "aging_overdue": {k: float(v) for k, v in aging_overdue.items()},
            "aging_current": {k: float(v) for k, v in aging_current.items()},
            "sin_fecha_limite": float(sin_fecha_limite),
            "total_outstanding": float(total_outstanding),
            "total_overdue": float(total_overdue),
            "total_current": float(total_current),
            "open_invoices": int(open_invoices),
        }
    finally:
        db.close()


def _aging_and_totals_db(ref_date: date) -> Tuple[Dict[str, float], float, float, float, int]:
    """
    Legacy helper (lo dejo por compatibilidad interna si algo más lo usa):
      - aging SOLO vencido: 0_30, 31_60, 61_90, 90_plus  (VENCIDOS)
      - total_por_cobrar (saldo abierto)
      - current_not_due (NO vencido)
      - no_due_date (saldo sin fecha_limite)
      - open_count
    """
    pack = build_aging_cxc_db(ref_date)
    overdue_legacy = {
        "0_30": pack["aging_overdue"]["overdue_1_30"],
        "31_60": pack["aging_overdue"]["overdue_31_60"],
        "61_90": pack["aging_overdue"]["overdue_61_90"],
        "90_plus": pack["aging_overdue"]["overdue_90_plus"],
    }
    total_por_cobrar = pack["total_outstanding"]
    current_not_due = pack["total_current"]
    no_due_date = pack["sin_fecha_limite"]
    open_count = pack["open_invoices"]
    return overdue_legacy, float(total_por_cobrar), float(current_not_due), float(no_due_date), int(open_count)


def _list_top_overdue_db(limit_n: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXC):
            saldo = float(_saldo_cxc(f))
            if saldo <= 0:
                continue

            due = f.fecha_limite.date() if f.fecha_limite else None
            if not due:
                continue
            days_over = (ref_date - due).days
            if days_over <= 0:
                continue

            cliente = (
                f.cliente.nombre_legal
                if getattr(f, "cliente", None)
                else str(getattr(f, "id_entidad_cliente", ""))
            )

            rows.append(
                _norm_open_row(
                    {
                        "invoice_id": f.numero_factura,
                        "customer": cliente,
                        "due_date": due,
                        "days_overdue": days_over,
                        "outstanding": saldo,
                    }
                )
            )

        rows.sort(key=lambda r: (r.get("days_overdue") or 0, r.get("outstanding") or 0.0), reverse=True)
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

            due = f.fecha_limite.date() if f.fecha_limite else None
            issue = f.fecha_emision.date() if f.fecha_emision else None
            days_over = None if not due else max((ref_date - due).days, 0)

            rows.append(
                _norm_open_row(
                    {
                        "invoice_id": f.numero_factura,
                        "issue_date": issue,
                        "due_date": due,
                        "days_overdue": days_over,
                        "outstanding": saldo,
                    }
                )
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
            if not due:
                status = "no_due_date"
                days_over = None
            else:
                days_over_raw = (ref_date - due).days
                if days_over_raw > 0:
                    status = "overdue"
                    days_over = days_over_raw
                else:
                    status = "open_on_time"
                    days_over = 0

            cliente = (
                f.cliente.nombre_legal
                if getattr(f, "cliente", None)
                else str(getattr(f, "id_entidad_cliente", ""))
            )

            rows.append(
                _norm_open_row(
                    {
                        "invoice_id": f.numero_factura,
                        "customer": cliente,
                        "due_date": due,
                        "status": status,
                        "days_overdue": days_over,
                        "outstanding": saldo,
                    }
                )
            )

        order_rank = {"overdue": 0, "open_on_time": 1, "no_due_date": 2}
        rows.sort(
            key=lambda r: (
                order_rank.get(r.get("status") or "", 9),
                -(r.get("days_overdue") or 0),
                -(r.get("outstanding") or 0.0),
            )
        )
        return rows
    finally:
        db.close()


# ---------------------------------------------------------------------
# Construcción de contexto base (KPI + aging + totales)
# ---------------------------------------------------------------------
def build_context(win: PeriodWindow, ref_date: date) -> Dict[str, Any]:
    repo = FinanzasRepoDB()
    try:
        dso_pack = repo.dso(win.start.year, win.start.month, window_days=90)
        kpi_dso = dso_pack.get("value")
    except Exception as e:
        dso_pack = {"value": None, "method": None, "reason": "Error calculando DSO."}
        kpi_dso = None
        

    try:
        pack = build_aging_cxc_db(ref_date)
    except Exception as e:
        return {"error": f"Error leyendo CxC DB: {e}"}

    # Legacy overdue (solo vencido) para compatibilidad con tu Gerente actual
    aging_legacy = {
        "0_30": float(pack["aging_overdue"].get("overdue_1_30", 0.0)),
        "31_60": float(pack["aging_overdue"].get("overdue_31_60", 0.0)),
        "61_90": float(pack["aging_overdue"].get("overdue_61_90", 0.0)),
        "90_plus": float(pack["aging_overdue"].get("overdue_90_plus", 0.0)),
    }

    data_norm = {
        "period": win.text,
        "kpi": {"DSO": kpi_dso},
        "calc_basis": {
            "DSO": dso_pack
        },
        "kpi": {"DSO": kpi_dso},

        # ✅ NUEVO: explícito y no ambiguo
        "aging_overdue": pack["aging_overdue"],
        "aging_current": pack["aging_current"],

        "current": float(pack["total_current"]),
        "por_vencer": float(pack["total_current"]),

        # ✅ LEGACY: mantenido, pero SOLO vencido
        "aging": aging_legacy,

        "total_por_cobrar": float(pack["total_outstanding"]),
        "total_overdue": float(pack["total_overdue"]),
        "total_current": float(pack["total_current"]),
        "sin_fecha_limite": float(pack["sin_fecha_limite"]),
        "open_invoices": int(pack["open_invoices"]),

        # ✅ anti-confusión
        "aging_explain": {
            "ref_date": ref_date.isoformat(),
            "aging_overdue": "Montos VENCIDOS (ref_date > fecha_limite) por rangos de días vencidos.",
            "aging_current": "Montos NO vencidos (ref_date <= fecha_limite) por rangos de días para vencer.",
            "aging_legacy": "Compatibilidad: SOLO vencido mapeado a 0_30/31_60/61_90/90_plus.",
        },
    }

    # Validación (no bloqueante)
    try:
        validate_with(SCHEMA, data_norm)
    except Exception:
        pass

    return {
        "period_window": win,
        "ref_date": ref_date,
        "kpi_dso": kpi_dso,
        "data_norm": data_norm,

        # también exponemos el pack por si alguna acción lo quiere directo
        "aging_pack": pack,
    }


# ---------------------------------------------------------------------
# Acciones
# ---------------------------------------------------------------------
def action_metrics(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    open_count = int(ctx.get("aging_pack", {}).get("open_invoices") or 0)
    return {
        "summary": f"CxC calculado (DB) — {open_count} facturas abiertas",
        "data": ctx["data_norm"],
        "dso": ctx["kpi_dso"],
    }


def action_top_overdue(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    n = params.get("n", 10)
    table = _list_top_overdue_db(int(n), ref_date)
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

    table_all = _list_open_db(ref_date)
    overdue = [r for r in table_all if r.get("status") == "overdue"]

    p_min = int(params.get("min_days", 1))
    p_max = params.get("max_days")
    if p_max is not None:
        p_max = int(p_max)
        overdue = [r for r in overdue if p_min <= int(r.get("days_overdue") or 0) <= p_max]
    else:
        overdue = [r for r in overdue if int(r.get("days_overdue") or 0) >= p_min]

    overdue.sort(
        key=lambda r: (int(r.get("days_overdue") or 0), float(r.get("outstanding") or 0.0)),
        reverse=True,
    )

    by_customer_map: Dict[str, Dict[str, Any]] = {}
    for r in overdue:
        cust = r.get("customer") or "N/D"
        by_customer_map.setdefault(cust, {"customer": cust, "invoices": 0, "total_outstanding": 0.0})
        by_customer_map[cust]["invoices"] += 1
        by_customer_map[cust]["total_outstanding"] += float(r.get("outstanding") or 0.0)

    by_customer = sorted(by_customer_map.values(), key=lambda x: x["total_outstanding"], reverse=True)
    total_overdue = float(sum(float(r.get("outstanding") or 0.0) for r in overdue))

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
        return {"error": ctx["error"]}

    handler = ACTIONS.get(action, action_metrics)
    return handler(ctx, params)

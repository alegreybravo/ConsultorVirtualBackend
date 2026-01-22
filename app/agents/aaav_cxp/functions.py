# app/agents/aaav_cxp/functions.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import re

import pandas as pd
from dateutil import parser as dateparser

from ...state import GlobalState
from ...tools.calc_kpis import month_window
from ...tools.schema_validate import validate_with

from app.database import SessionLocal
from app.models import FacturaCXP, Entidad
from app.repo_finanzas_db import FinanzasRepoDB

SCHEMA = "app/schemas/aaav_cxp_schema.json"


# ---------------------------------------------------------------------
# Período (igual a CxC)
# ---------------------------------------------------------------------
@dataclass
class PeriodWindow:
    text: str
    start: pd.Timestamp
    end: pd.Timestamp


def resolve_period(payload: Dict[str, Any], state: GlobalState) -> PeriodWindow:
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

    today = pd.Timestamp.today(tz="America/Costa_Rica")
    ym = today.strftime("%Y-%m")
    s, e, _ = month_window(ym)
    return PeriodWindow(text=ym, start=s, end=e)


def resolve_ref_date(win: PeriodWindow) -> date:
    """
    Usa 'fecha:YYYY-MM-DD' si viene en win.text; si no, usa win.end.date()
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


def _norm_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)

    if "due_date" in out:
        out["due_date"] = _iso_date(out.get("due_date"))

    d = out.get("days_overdue")
    if d is None:
        out["days_overdue"] = None
    else:
        try:
            out["days_overdue"] = int(d)
        except Exception:
            out["days_overdue"] = None

    if "days_to_due" in out and out["days_to_due"] is not None:
        try:
            out["days_to_due"] = int(out["days_to_due"])
        except Exception:
            out["days_to_due"] = None

    return out


# ---------------------------------------------------------------------
# Helpers DB (CxP) estilo CxC (NO ambiguo)
# ---------------------------------------------------------------------
def _saldo_cxp(f: FacturaCXP) -> Decimal:
    return Decimal((f.monto or 0) - (f.monto_pagado or 0))


def build_aging_cxp_db(ref_date: date) -> Dict[str, Any]:
    """
    Construye aging NO ambiguo:
      - aging_overdue: vencido (ref_date > fecha_limite) en buckets 1-30, 31-60, 61-90, 90+
      - aging_current: no vencido (ref_date <= fecha_limite) en buckets para vencer (0-7, 8-15, 16-30, 31+)
      - sin_fecha_limite
      - totales y conteo
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
        # ✅ consistencia: solo no pagadas
        for f in db.query(FacturaCXP).filter(FacturaCXP.pagada == False):
            saldo = _saldo_cxp(f)
            if saldo <= 0:
                continue

            open_invoices += 1
            total_outstanding += saldo

            if not f.fecha_limite:
                sin_fecha_limite += saldo
                continue

            due = f.fecha_limite.date()
            if ref_date > due:
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


def _list_top_overdue_db(limit_n: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXP).filter(FacturaCXP.pagada == False):
            saldo = float(_saldo_cxp(f))
            if saldo <= 0:
                continue

            due = f.fecha_limite.date() if f.fecha_limite else None
            if not due:
                continue
            days_over = (ref_date - due).days
            if days_over <= 0:
                continue

            proveedor = (
                f.proveedor.nombre_legal
                if getattr(f, "proveedor", None)
                else str(getattr(f, "id_entidad_proveedor", ""))
            )
            rows.append(_norm_row({
                "invoice_id": f.numero_factura,
                "supplier": proveedor,
                "due_date": due,
                "days_overdue": days_over,
                "outstanding": saldo,
            }))

        rows.sort(
            key=lambda r: (r.get("days_overdue") or 0, r.get("outstanding") or 0.0),
            reverse=True,
        )
        return rows[: int(limit_n)]
    finally:
        db.close()


def _list_due_soon_db(max_days: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXP).filter(FacturaCXP.pagada == False):
            saldo = float(_saldo_cxp(f))
            if saldo <= 0 or not f.fecha_limite:
                continue
            due = f.fecha_limite.date()
            days_to = (due - ref_date).days
            if 0 <= days_to <= int(max_days):
                proveedor = (
                    f.proveedor.nombre_legal
                    if getattr(f, "proveedor", None)
                    else str(getattr(f, "id_entidad_proveedor", ""))
                )
                rows.append(_norm_row({
                    "invoice_id": f.numero_factura,
                    "supplier": proveedor,
                    "due_date": due,
                    "days_to_due": days_to,
                    "outstanding": saldo,
                }))

        rows.sort(key=lambda r: ((r.get("days_to_due") or 10**9), -(r.get("outstanding") or 0.0)))
        return rows
    finally:
        db.close()


def _supplier_balance_db(name_or_id: str, ref_date: date) -> Tuple[float, List[Dict[str, Any]]]:
    target = str(name_or_id).strip()
    db = SessionLocal()
    try:
        # match flexible como CxC
        prov = db.query(Entidad).filter(Entidad.nombre_legal.ilike(f"%{target}%")).first()
        prov_id = prov.id_entidad if prov else None
        if not prov_id:
            try:
                prov_id = int(target)
            except Exception:
                prov_id = None

        # ✅ evita universo completo si no hay match
        if not prov_id:
            return 0.0, []

        total = 0.0
        rows: List[Dict[str, Any]] = []
        q = (
            db.query(FacturaCXP)
            .filter(FacturaCXP.pagada == False)
            .filter(FacturaCXP.id_entidad_proveedor == prov_id)
        )

        for f in q:
            saldo = float(_saldo_cxp(f))
            if saldo <= 0:
                continue
            due = f.fecha_limite.date() if f.fecha_limite else None
            days_over = None if not due else max((ref_date - due).days, 0)

            rows.append(_norm_row({
                "invoice_id": f.numero_factura,
                "due_date": due,
                "days_overdue": days_over,
                "outstanding": saldo,
            }))
            total += saldo

        return total, rows
    finally:
        db.close()


def _list_open_db(ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXP).filter(FacturaCXP.pagada == False):
            saldo = float(_saldo_cxp(f))
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

            proveedor = (
                f.proveedor.nombre_legal
                if getattr(f, "proveedor", None)
                else str(getattr(f, "id_entidad_proveedor", ""))
            )
            rows.append(_norm_row({
                "invoice_id": f.numero_factura,
                "supplier": proveedor,
                "due_date": due,
                "status": status,
                "days_overdue": days_over,
                "outstanding": saldo,
            }))

        order_rank = {"overdue": 0, "open_on_time": 1, "no_due_date": 2}
        rows.sort(key=lambda r: (
            order_rank.get(r.get("status") or "", 9),
            -(r.get("days_overdue") or 0),
            -(r.get("outstanding") or 0.0),
        ))
        return rows
    finally:
        db.close()


# ---------------------------------------------------------------------
# Construcción de contexto base (KPI + aging + totales) estilo CxC
# ---------------------------------------------------------------------
def build_context(win: PeriodWindow, ref_date: date) -> Dict[str, Any]:
    repo = FinanzasRepoDB()
    try:
        dpo_pack = repo.dpo(win.start.year, win.start.month, window_days=90)
        kpi_dpo = dpo_pack.get("value")
    except Exception:
        dpo_pack = {"value": None, "method": None, "reason": "Error calculando DPO."}
        kpi_dpo = None

    try:
        pack = build_aging_cxp_db(ref_date)
    except Exception as e:
        return {"error": f"Error leyendo CxP DB: {e}"}

    # legacy (solo vencido) por compatibilidad
    aging_legacy = {
        "0_30": float(pack["aging_overdue"].get("overdue_1_30", 0.0)),
        "31_60": float(pack["aging_overdue"].get("overdue_31_60", 0.0)),
        "61_90": float(pack["aging_overdue"].get("overdue_61_90", 0.0)),
        "90_plus": float(pack["aging_overdue"].get("overdue_90_plus", 0.0)),
    }

    data_norm = {
        "period": win.text,
        "kpi": {"DPO": kpi_dpo},
        "calc_basis": {"DPO": dpo_pack},

        "aging_overdue": pack["aging_overdue"],
        "aging_current": pack["aging_current"],

        "current": float(pack["total_current"]),
        "por_vencer": float(pack["total_current"]),

        "aging": aging_legacy,

        "total_por_pagar": float(pack["total_outstanding"]),
        "total_overdue": float(pack["total_overdue"]),
        "total_current": float(pack["total_current"]),
        "sin_fecha_limite": float(pack["sin_fecha_limite"]),
        "open_invoices": int(pack["open_invoices"]),

        "aging_explain": {
            "ref_date": ref_date.isoformat(),
            "aging_overdue": "Montos VENCIDOS (ref_date > fecha_limite) por rangos de días vencidos.",
            "aging_current": "Montos NO vencidos (ref_date <= fecha_limite) por rangos de días para vencer.",
            "aging_legacy": "Compatibilidad: SOLO vencido mapeado a 0_30/31_60/61_90/90_plus.",
        },
    }

    try:
        validate_with(SCHEMA, data_norm)
    except Exception:
        pass

    return {
        "period_window": win,
        "ref_date": ref_date,
        "kpi_dpo": kpi_dpo,
        "data_norm": data_norm,
        "aging_pack": pack,
    }


# ---------------------------------------------------------------------
# Acciones "locales" (igual estilo CxC)
# ---------------------------------------------------------------------
def action_metrics(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    open_count = int(ctx.get("aging_pack", {}).get("open_invoices") or 0)
    return {
        "summary": f"CxP calculado (DB) — {open_count} facturas abiertas",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
    }


def action_aging_snapshot(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    d = ctx["data_norm"]
    snap = [
        {"bucket": "no_vencido", "amount": d["total_current"]},
        {"bucket": "vencido_1_30", "amount": d["aging"]["0_30"]},
        {"bucket": "vencido_31_60", "amount": d["aging"]["31_60"]},
        {"bucket": "vencido_61_90", "amount": d["aging"]["61_90"]},
        {"bucket": "vencido_90_plus", "amount": d["aging"]["90_plus"]},
        {"bucket": "sin_fecha_limite", "amount": d["sin_fecha_limite"]},
    ]
    return {
        "summary": "Aging CxP (snapshot)",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "result": {"action": "aging_snapshot", "rows": snap},
    }


def action_top_overdue(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    n = int(params.get("n", 10))
    table = _list_top_overdue_db(n, ref_date)
    return {
        "summary": "Top facturas CxP vencidas (más urgentes)",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "result": {"action": "top_overdue", "table": table},
    }


def action_due_soon(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    days = int(params.get("days", 7))
    table = _list_due_soon_db(days, ref_date)
    return {
        "summary": f"Facturas CxP por vencer en ≤{days} días",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "result": {"action": "due_soon", "table": table},
    }


def action_supplier_balance(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    supplier = (params.get("supplier") or "").strip()
    if not supplier:
        return {"error": "Falta 'supplier' en params"}

    total, table = _supplier_balance_db(supplier, ref_date)
    return {
        "summary": f"Saldo pendiente con proveedor '{supplier}': {total:.2f}",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "result": {"action": "supplier_balance", "total_outstanding": total, "table": table},
    }


def action_list_open(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    ref_date: date = ctx["ref_date"]
    table = _list_open_db(ref_date)
    return {
        "summary": "Cuentas por pagar abiertas",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "result": {"action": "list_open", "table": table},
    }


# ---------------------------------------------------------------------
# Acciones "repo" (para CXP-02/03/05/01)
# ---------------------------------------------------------------------
def _parse_iso_dt(s: Any) -> Optional[datetime]:
    if not s:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        pass
    try:
        return dateparser.parse(raw, dayfirst=True)
    except Exception:
        return None


def action_cxp_aging_as_of(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    as_of = _parse_iso_dt(params.get("as_of"))
    if not as_of:
        return {"error": "Falta param 'as_of' (ISO) para cxp_aging_as_of"}

    repo = FinanzasRepoDB()
    summary = repo.cxp_aging_as_of(as_of)
    if isinstance(summary.get("as_of"), str):
        summary["as_of"] = summary["as_of"][:10]

    return {
        "summary": "Aging CxP (buckets estándar) — repo",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "executive_context_patch": {"cxp_aging": summary},
        "result": {"action": "cxp_aging_as_of", "cxp_aging": summary},
    }


def action_cxp_top_suppliers_open(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    as_of = _parse_iso_dt(params.get("as_of"))
    if not as_of:
        return {"error": "Falta param 'as_of' (ISO) para cxp_top_suppliers_open"}

    limit = int(params.get("limit") or 5)

    repo = FinanzasRepoDB()
    summary = repo.cxp_top_suppliers_open(as_of, limit=limit)
    if isinstance(summary.get("as_of"), str):
        summary["as_of"] = summary["as_of"][:10]

    # (no cambia la data, pero hace el "summary" consistente con el límite)
    rows = summary.get("rows") or []
    shown = rows[:limit] if isinstance(rows, list) else []

    return {
        "summary": f"Top {limit} proveedores por saldo CxP abierto ({len(shown)} filas)",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "executive_context_patch": {"cxp_top_suppliers": summary},
        "result": {"action": "cxp_top_suppliers_open", "cxp_top_suppliers": summary},
    }


def action_cxp_supplier_open_balance_on(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    supplier = (params.get("supplier") or "").strip()
    if not supplier:
        return {"error": "Falta param 'supplier' para cxp_supplier_open_balance_on"}

    as_of = _parse_iso_dt(params.get("as_of"))
    if not as_of:
        return {"error": "Falta param 'as_of' (ISO) para cxp_supplier_open_balance_on"}

    repo = FinanzasRepoDB()
    summary = repo.cxp_supplier_open_balance_on(supplier, as_of)
    if isinstance(summary.get("as_of"), str):
        summary["as_of"] = summary["as_of"][:10]

    return {
        "summary": f"Saldo CxP abierto de '{supplier}' al {summary.get('as_of')}: {summary.get('saldo')}",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "executive_context_patch": {"cxp_saldo_proveedor": summary},
        "result": {"action": "cxp_supplier_open_balance_on", "cxp_saldo_proveedor": summary},
    }


def action_cxp_open_summary_as_of(ctx: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    CXP-01: ¿Cuántas facturas CxP están abiertas al corte y el saldo total?
    SELECT COUNT(*) AS abiertas, SUM(monto - monto_pagado) AS saldo
    FROM agente_virtual.factura_cxp
    WHERE pagada = FALSE;
    """
    as_of = _parse_iso_dt(params.get("as_of"))
    if not as_of:
        return {"error": "Falta param 'as_of' (ISO) para cxp_open_summary_as_of"}

    repo = FinanzasRepoDB()
    summary = repo.cxp_open_summary_as_of(as_of)

    # normalizar fecha
    if isinstance(summary.get("as_of"), str):
        summary["as_of"] = summary["as_of"][:10]
    else:
        try:
            summary["as_of"] = as_of.date().isoformat()
        except Exception:
            pass

    abiertas = summary.get("abiertas")
    saldo = summary.get("saldo")

    return {
        "summary": f"CxP abiertas al {summary.get('as_of')}: {abiertas} facturas; saldo {saldo}",
        "data": ctx["data_norm"],
        "dpo": ctx["kpi_dpo"],
        "executive_context_patch": {"cxp_open_summary": summary},
        "result": {"action": "cxp_open_summary_as_of", "cxp_open_summary": summary},
    }


# ---------------------------------------------------------------------
# Registry + run_action (igual a CxC)
# ---------------------------------------------------------------------
ACTIONS = {
    "metrics": action_metrics,
    "aging": action_aging_snapshot,
    "top_overdue": action_top_overdue,
    "due_soon": action_due_soon,
    "supplier_balance": action_supplier_balance,
    "list_open": action_list_open,

    # repo / pruebas
    "cxp_aging_as_of": action_cxp_aging_as_of,                 # CXP-02
    "cxp_top_suppliers_open": action_cxp_top_suppliers_open,   # CXP-03
    "cxp_supplier_open_balance_on": action_cxp_supplier_open_balance_on,  # CXP-05
    "cxp_open_summary_as_of": action_cxp_open_summary_as_of,   # CXP-01
}


def run_action(
    action: str,
    payload: Dict[str, Any],
    params: Dict[str, Any],
    state: GlobalState,
) -> Dict[str, Any]:
    win = resolve_period(payload, state)
    ref_date = resolve_ref_date(win)

    # ✅ bridge: as_of para acciones "as_of" (CORREGIDO)
    if action in (
        "cxp_aging_as_of",
        "cxp_top_suppliers_open",
        "cxp_supplier_open_balance_on",
        "cxp_open_summary_as_of",
    ):
        if not params.get("as_of"):
            params["as_of"] = datetime(ref_date.year, ref_date.month, ref_date.day, 23, 59, 59).isoformat()

        if action == "cxp_top_suppliers_open" and not params.get("limit"):
            params["limit"] = 5

    ctx = build_context(win, ref_date)
    if isinstance(ctx, dict) and ctx.get("error"):
        return {"error": ctx["error"]}

    handler = ACTIONS.get(action, action_metrics)
    return handler(ctx, params)


# ---------------------------------------------------------------------
# Wrapper opcional: si tu sistema aún llama run_agent(...)
# ---------------------------------------------------------------------
def run_agent(payload: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
    forced_action: str = (payload.get("action") or "").strip() or "metrics"
    params_in: Dict[str, Any] = payload.get("params", {}) or {}
    return run_action(forced_action, payload, params_in, state)

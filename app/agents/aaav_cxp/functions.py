# app/agents/aaav_cxp/functions.py
from __future__ import annotations
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import re

import pandas as pd
from dateutil import parser as dateparser

from ...state import GlobalState
from ...tools.calc_kpis import month_window  # fallback si sólo llega "YYYY-MM"
from ...tools.schema_validate import validate_with  # opcional (no bloquea)
from app.tools.llm_json import call_llm_json

from app.database import SessionLocal
from app.models import FacturaCXP, Entidad
from app.repo_finanzas_db import FinanzasRepoDB


# ===================== Catálogo de skills CxP =====================
SKILLS_CXP = [
    {
        "id": "metrics",
        "description": "Calcular métricas base de CxP (DPO, total_por_pagar, por_vencer, aging vencido).",
        "params_schema": {}
    },
    {
        "id": "aging",
        "description": "Obtener snapshot de antigüedad de saldos vencidos en buckets 0-30, 31-60, 61-90 y 90+ días.",
        "params_schema": {}
    },
    {
        "id": "top_overdue",
        "description": "Top N facturas vencidas por monto y días de atraso.",
        "params_schema": {
            "n": {"type": "integer", "description": "Cantidad de facturas a listar.", "default": 10}
        }
    },
    {
        "id": "due_soon",
        "description": "Facturas que vencen en los próximos N días.",
        "params_schema": {
            "days": {"type": "integer", "description": "Cantidad máxima de días hasta el vencimiento.", "default": 7}
        }
    },
    {
        "id": "supplier_balance",
        "description": "Saldo pendiente de un proveedor específico.",
        "params_schema": {
            "supplier": {"type": "string", "description": "Nombre legal o ID del proveedor."}
        }
    },
    {
        "id": "list_open",
        "description": "Listado de todas las facturas abiertas (con saldo > 0).",
        "params_schema": {}
    },
]

SCHEMA = "app/schemas/aaav_cxp_schema.json"


# ===================== Período unificado =====================
@dataclass
class PeriodWindow:
    text: str
    start: pd.Timestamp
    end: pd.Timestamp


def _resolve_period(payload: Dict[str, Any], state: GlobalState) -> PeriodWindow:
    """
    Acepta:
      - payload["period_range"] dict (preferido, del router) {text,start,end,...}
      - payload["period"] 'YYYY-MM' (fallback)
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

    # Fallback: mes actual (forzado a America/Costa_Rica)
    today = pd.Timestamp.today(tz="America/Costa_Rica")
    ym = today.strftime("%Y-%m")
    s, e, _ = month_window(ym)
    return PeriodWindow(text=ym, start=s, end=e)


# ===================== Helpers de serialización =====================
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
        return s[:10] if s else None
    except Exception:
        return None


def _norm_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    if "due_date" in out:
        out["due_date"] = _iso_date(out.get("due_date"))

    # days_overdue (puede ser None si no hay fecha)
    d = out.get("days_overdue")
    if d is None:
        out["days_overdue"] = None
    else:
        try:
            out["days_overdue"] = int(d)
        except Exception:
            out["days_overdue"] = None

    # days_to_due (due_soon)
    if "days_to_due" in out and out["days_to_due"] is not None:
        try:
            out["days_to_due"] = int(out["days_to_due"])
        except Exception:
            out["days_to_due"] = None

    return out


# ===================== Mini-planner clásico (regex) =====================
@dataclass
class Plan:
    actions: List[Dict[str, Any]]
    reasons: List[str]


_RX_INT = re.compile(r"(?<!\d)(\d{1,4})(?!\d)")


def _to_int(s: str, default: int) -> int:
    m = _RX_INT.search(s or "")
    return int(m.group(1)) if m else default


def analyze_user_request(question: str) -> Plan:
    q = (question or "").lower()
    actions: List[Dict[str, Any]] = []
    reasons: List[str] = []

    if any(k in q for k in ["dpo", "cxp", "cuentas por pagar", "proveedores", "pagos", "liquidez"]):
        actions.append({"name": "metrics", "params": {}})
        reasons.append("Se solicita DPO/CxP → calcular métricas base.")

    if "aging" in q or "antigüedad" in q or "vencid" in q:
        actions.append({"name": "aging", "params": {}})
        reasons.append("Se solicita aging de proveedores (solo vencido).")

    if "top" in q and "vencid" in q:
        n = _to_int(q, 10)
        actions.append({"name": "top_overdue", "params": {"n": n}})
        reasons.append(f"Top de facturas vencidas (n={n}).")

    if "vencen" in q and ("dias" in q or "días" in q or "semana" in q):
        days = 7 if "semana" in q else _to_int(q, 7)
        actions.append({"name": "due_soon", "params": {"days": days}})
        reasons.append(f"Facturas por vencer en ≤{days} días.")

    m = re.search(r"(proveedor|supplier)\s*[:=]\s*([A-Za-z0-9\-\.\s]+)", q)
    if m:
        prov = m.group(2).strip()
        actions.append({"name": "supplier_balance", "params": {"supplier": prov}})
        reasons.append(f"Saldo por proveedor '{prov}'.")

    if "abiertas" in q or "pendientes" in q:
        actions.append({"name": "list_open", "params": {}})
        reasons.append("Listado de CxP abiertas.")

    if not actions:
        actions.append({"name": "metrics", "params": {}})
        reasons.append("Sin señales claras → métricas base.")
    return Plan(actions=actions, reasons=reasons)


# ===================== Planner con LLM =====================
def plan_with_llm(question: str, win: PeriodWindow) -> Plan:
    system = (
        "Eres un agente de planificación de consultas de Cuentas por Pagar (CxP).\n"
        "Tu tarea es decidir qué acciones (skills) se deben ejecutar en base a la pregunta del usuario.\n"
        "Debes devolver EXCLUSIVAMENTE un JSON con este formato:\n\n"
        "{\n"
        '  "actions": [\n'
        '    {"name": "metrics", "params": {}},\n'
        '    {"name": "aging", "params": {}},\n'
        '    {"name": "top_overdue", "params": {"n": 5}}\n'
        "  ],\n"
        '  "reasons": ["motivo 1", "motivo 2"]\n'
        "}\n\n"
        "Solo puedes usar estos skills:\n"
    )

    system += "".join(f"- {s['id']}: {s['description']}\n" for s in SKILLS_CXP)

    user = (
        f"Pregunta del usuario: {question}\n"
        f"Período de referencia: {win.text} ({win.start.date()} a {win.end.date()}).\n"
        "Devuelve solo el JSON pedido, sin comentarios adicionales."
    )

    raw = call_llm_json(
        system=system,
        user=user,
        model="gpt-4o-mini",
        temperature=0.0,
    )

    actions = raw.get("actions") or []
    reasons = raw.get("reasons") or []

    cleaned_actions: List[Dict[str, Any]] = []
    valid_ids = {s["id"] for s in SKILLS_CXP}

    for a in actions:
        if not isinstance(a, dict):
            continue
        name = a.get("name")
        if name not in valid_ids:
            continue
        params = a.get("params") or {}

        if name == "top_overdue":
            params.setdefault("n", 10)
        if name == "due_soon":
            params.setdefault("days", 7)

        cleaned_actions.append({"name": name, "params": params})

    if not cleaned_actions:
        cleaned_actions = [{"name": "metrics", "params": {}}]
        reasons = reasons + ["Fallback a 'metrics' por falta de acciones válidas."]

    return Plan(actions=cleaned_actions, reasons=reasons)


# ===================== Helpers DB CxP =====================
def _saldo_cxp(f: FacturaCXP) -> Decimal:
    return Decimal((f.monto or 0) - (f.monto_pagado or 0))


def _aging_and_totals_db(ref_date: date) -> Tuple[Dict[str, float], Dict[str, float], float, float, float]:
    """
    Devuelve:
      - aging_overdue (legacy vencido): 0_30, 31_60, 61_90, 90_plus  (VENCIDOS 1-30, 31-60, etc.)
      - aging_current: no vencido (ref_date <= due_date)
      - total_por_pagar (saldo abierto)
      - current_not_due (NO vencido: due_date >= ref_date)  (se mantiene)
      - no_due_date (saldo sin fecha_limite)
    """
    db = SessionLocal()
    overdue = {"0_30": Decimal("0"), "31_60": Decimal("0"), "61_90": Decimal("0"), "90_plus": Decimal("0")}
    # ✅ nuevo (simple, no ambiguo)
    aging_current = {"current_0_30": Decimal("0")}

    current_not_due = Decimal("0")
    no_due_date = Decimal("0")
    try:
        for f in db.query(FacturaCXP):
            saldo = _saldo_cxp(f)
            if saldo <= 0:
                continue

            if not f.fecha_limite:
                no_due_date += saldo
                continue

            days = (ref_date - f.fecha_limite.date()).days

            if days <= 0:
                current_not_due += saldo
                # ✅ clasifica como current (por vencer / vigente)
                aging_current["current_0_30"] += saldo

            elif days <= 30:
                overdue["0_30"] += saldo
            elif days <= 60:
                overdue["31_60"] += saldo
            elif days <= 90:
                overdue["61_90"] += saldo
            else:
                overdue["90_plus"] += saldo

        total_por_pagar = float(current_not_due + no_due_date + sum(overdue.values()))
        return (
            {k: float(v) for k, v in overdue.items()},
            {k: float(v) for k, v in aging_current.items()},
            total_por_pagar,
            float(current_not_due),
            float(no_due_date),
        )
    finally:
        db.close()



def _list_top_overdue_db(limit_n: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXP):
            saldo = float(_saldo_cxp(f))
            if saldo <= 0:
                continue

            due = f.fecha_limite.date() if f.fecha_limite else None
            if not due:
                continue
            days_over = (ref_date - due).days
            if days_over <= 0:
                continue

            proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
            rows.append(
                _norm_row(
                    {
                        "invoice_id": f.numero_factura,
                        "supplier": proveedor,
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


def _list_due_soon_db(max_days: int, ref_date: date) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        rows: List[Dict[str, Any]] = []
        for f in db.query(FacturaCXP):
            saldo = float(_saldo_cxp(f))
            if saldo <= 0 or not f.fecha_limite:
                continue
            due = f.fecha_limite.date()
            days_to = (due - ref_date).days
            if 0 <= days_to <= int(max_days):
                proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
                rows.append(
                    _norm_row(
                        {
                            "invoice_id": f.numero_factura,
                            "supplier": proveedor,
                            "due_date": due,
                            "days_to_due": days_to,
                            "outstanding": saldo,
                        }
                    )
                )

        rows.sort(key=lambda r: ((r.get("days_to_due") or 10**9), -(r.get("outstanding") or 0.0)))
        return rows
    finally:
        db.close()


def _supplier_balance_db(name_or_id: str, ref_date: date):
    target = str(name_or_id).strip()
    db = SessionLocal()
    try:
        prov = db.query(Entidad).filter(Entidad.nombre_legal.ilike(target)).first()
        prov_id = prov.id_entidad if prov else None
        if not prov_id:
            try:
                prov_id = int(target)
            except Exception:
                prov_id = None

        total = 0.0
        rows: List[Dict[str, Any]] = []
        q = db.query(FacturaCXP)
        if prov_id:
            q = q.filter(FacturaCXP.id_entidad_proveedor == prov_id)

        for f in q:
            saldo = float(_saldo_cxp(f))
            if saldo <= 0:
                continue

            due = f.fecha_limite.date() if f.fecha_limite else None
            days_over = None if not due else max((ref_date - due).days, 0)

            rows.append(
                _norm_row(
                    {
                        "invoice_id": f.numero_factura,
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
        for f in db.query(FacturaCXP):
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

            proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
            rows.append(
                _norm_row(
                    {
                        "invoice_id": f.numero_factura,
                        "supplier": proveedor,
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


def _count_open_db(ref_date: date) -> int:
    db = SessionLocal()
    try:
        cnt = 0
        for f in db.query(FacturaCXP):
            if _saldo_cxp(f) > 0:
                cnt += 1
        return cnt
    finally:
        db.close()


# ===================== Orquestador interno =====================
def run_agent(payload: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
    question = (payload.get("question") or "").strip()
    forced_action: str = (payload.get("action") or "").strip()
    params_in: Dict[str, Any] = payload.get("params", {}) or {}

    # 1) Período
    win = _resolve_period(payload, state)
    ref_date = win.end.date()

    # 2) KPI base (DPO) + aging vencido y totales
    repo = FinanzasRepoDB()
    try:
        dpo_pack = repo.dpo(win.start.year, win.start.month, window_days=90)
        kpi_dpo = dpo_pack.get("value")
    except Exception as e:
        dpo_pack = {"value": None, "method": None, "reason": "Error calculando DPO."}
        kpi_dpo = None
    try:
        aging_overdue, aging_current, total_por_pagar, current_not_due, no_due_date = _aging_and_totals_db(ref_date)
    except Exception as e:
        return {"error": f"Error leyendo CxP DB: {e}"}

    overdue_total = float(
        (aging_overdue.get("0_30", 0.0) or 0.0)
        + (aging_overdue.get("31_60", 0.0) or 0.0)
        + (aging_overdue.get("61_90", 0.0) or 0.0)
        + (aging_overdue.get("90_plus", 0.0) or 0.0)
    )
    open_count = _count_open_db(ref_date)

    por_vencer = float(current_not_due)  # alias histórico (NO vencido)

    data_norm = {
        "period": win.text,
        "kpi": {"DPO": kpi_dpo},
        "period": win.text,
        "kpi": {"DPO": kpi_dpo},
        "calc_basis": {"DPO": dpo_pack},

        # ✅ NUEVO: explícito y no ambiguo (para el Gerente)
        "aging_overdue": {
            "overdue_1_30": float(aging_overdue.get("0_30", 0.0)),
            "overdue_31_60": float(aging_overdue.get("31_60", 0.0)),
            "overdue_61_90": float(aging_overdue.get("61_90", 0.0)),
            "overdue_90_plus": float(aging_overdue.get("90_plus", 0.0)),
        },
        "aging_current": {
            "current_0_30": float(aging_current.get("current_0_30", 0.0)),
        },

        # ✅ LEGACY: tu formato actual, solo vencido
        "aging": {
            "0_30": float(aging_overdue.get("0_30", 0.0)),
            "31_60": float(aging_overdue.get("31_60", 0.0)),
            "61_90": float(aging_overdue.get("61_90", 0.0)),
            "90_plus": float(aging_overdue.get("90_plus", 0.0)),
        },

        "total_por_pagar": float(total_por_pagar),

        "current": float(current_not_due),
        "por_vencer": float(current_not_due),
        "sin_fecha_limite": float(no_due_date),

        "overdue_total": overdue_total,
        "open_invoices": int(open_count),

        "aging_explain": {
            "ref_date": ref_date.isoformat(),
            "aging_overdue": "Montos VENCIDOS (ref_date > fecha_limite) por rangos de días vencidos.",
            "aging_current": "Montos NO vencidos (ref_date <= fecha_limite).",
            "aging_legacy": "Compatibilidad: SOLO vencido mapeado a 0_30/31_60/61_90/90_plus.",
            "no_vencido_total": float(current_not_due),
            "overdue_total": overdue_total,
            "sin_fecha_limite": float(no_due_date),
        },
    }


    # 3) Validación (no bloqueante)
    try:
        validate_with(SCHEMA, data_norm)
    except Exception:
        pass

    # 4) Plan de ejecución (LLM + fallback regex)
    if forced_action:
        actions = [{"name": forced_action, "params": params_in}]
        reasons = ["Forced action"]
    else:
        try:
            plan = plan_with_llm(question, win)
        except Exception as e:
            plan = analyze_user_request(question)
            plan.reasons.append(f"Fallback a analyze_user_request por error planner LLM: {e}")
        actions = plan.actions
        reasons = plan.reasons

    # 5) Ejecutar acciones
    result_tables: List[Dict[str, Any]] = []
    for a in actions:
        name = a["name"]
        p = a.get("params", {})

        if name == "metrics":
            pass  # ya cubierto por data_norm

        elif name == "aging":
            snap = [
                {"bucket": "no_vencido", "amount": data_norm["current"]},
                {"bucket": "vencido_1_30", "amount": data_norm["aging"]["0_30"]},
                {"bucket": "vencido_31_60", "amount": data_norm["aging"]["31_60"]},
                {"bucket": "vencido_61_90", "amount": data_norm["aging"]["61_90"]},
                {"bucket": "vencido_90_plus", "amount": data_norm["aging"]["90_plus"]},
                {"bucket": "sin_fecha_limite", "amount": data_norm["sin_fecha_limite"]},
            ]
            result_tables.append({"action": "aging_snapshot", "rows": snap})

        elif name == "top_overdue":
            rows = _list_top_overdue_db(int(p.get("n", 10)), ref_date)
            result_tables.append({"action": "top_overdue", "rows": rows})

        elif name == "due_soon":
            rows = _list_due_soon_db(int(p.get("days", 7)), ref_date)
            result_tables.append({"action": "due_soon", "rows": rows})

        elif name == "supplier_balance":
            supp = p.get("supplier")
            if not supp:
                result_tables.append({"action": "supplier_balance", "error": "Falta 'supplier' en params"})
            else:
                total, rows = _supplier_balance_db(supp, ref_date)
                result_tables.append({"action": "supplier_balance", "total_outstanding": total, "rows": rows})

        elif name == "list_open":
            rows = _list_open_db(ref_date)
            result_tables.append({"action": "list_open", "rows": rows})

        else:
            result_tables.append({"action": name, "error": "Acción desconocida"})

    # 6) Salida normalizada + mirror top-level
    return {
        "summary": "CxP ejecutado: " + (forced_action or ", ".join(sorted({a["name"] for a in actions}))),
        "data": data_norm,
        "dpo": kpi_dpo,
        "result": {"actions": result_tables, "reasons": reasons},
    }

# app/agents/aaav_cxp/functions.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
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
            "n": {
                "type": "integer",
                "description": "Cantidad de facturas a listar.",
                "default": 10
            }
        }
    },
    {
        "id": "due_soon",
        "description": "Facturas que vencen en los próximos N días.",
        "params_schema": {
            "days": {
                "type": "integer",
                "description": "Cantidad máxima de días hasta el vencimiento.",
                "default": 7
            }
        }
    },
    {
        "id": "supplier_balance",
        "description": "Saldo pendiente de un proveedor específico.",
        "params_schema": {
            "supplier": {
                "type": "string",
                "description": "Nombre legal o ID del proveedor."
            }
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
    """
    Planner heurístico (sin LLM), por si queremos fallback.
    """
    q = (question or "").lower()
    actions: List[Dict[str, Any]] = []
    reasons: List[str] = []

    # métricas base
    if any(k in q for k in ["dpo", "cxp", "cuentas por pagar", "proveedores", "pagos", "liquidez"]):
        actions.append({"name": "metrics", "params": {}})
        reasons.append("Se solicita DPO/CxP → calcular métricas base.")

    # aging / vencidos (snapshot)
    if "aging" in q or "antigüedad" in q or "vencid" in q:
        actions.append({"name": "aging", "params": {}})
        reasons.append("Se solicita aging de proveedores (solo vencido).")

    # top vencidos
    if "top" in q and "vencid" in q:
        n = _to_int(q, 10)
        actions.append({"name": "top_overdue", "params": {"n": n}})
        reasons.append(f"Top de facturas vencidas (n={n}).")

    # por vencer pronto
    if "vencen" in q and ("dias" in q or "días" in q or "semana" in q):
        days = 7 if "semana" in q else _to_int(q, 7)
        actions.append({"name": "due_soon", "params": {"days": days}})
        reasons.append(f"Facturas por vencer en ≤{days} días.")

    # saldo por proveedor
    m = re.search(r"(proveedor|supplier)\s*[:=]\s*([A-Za-z0-9\-\.\s]+)", q)
    if m:
        prov = m.group(2).strip()
        actions.append({"name": "supplier_balance", "params": {"supplier": prov}})
        reasons.append(f"Saldo por proveedor '{prov}'.")

    # listado de abiertas
    if "abiertas" in q or "pendientes" in q:
        actions.append({"name": "list_open", "params": {}})
        reasons.append("Listado de CxP abiertas.")

    if not actions:
        actions.append({"name": "metrics", "params": {}})
        reasons.append("Sin señales claras → métricas base.")
    return Plan(actions=actions, reasons=reasons)


# ===================== Planner con LLM =====================
def plan_with_llm(question: str, win: PeriodWindow) -> Plan:
    """
    Usa el LLM para decidir qué skills ejecutar y con qué parámetros.
    Devuelve el mismo formato Plan(actions, reasons) que analyze_user_request.
    """
    # Prompt de sistema
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

    skills_txt = ""
    for s in SKILLS_CXP:
        skills_txt += f"- {s['id']}: {s['description']}\n"

    system += skills_txt

    # Mensaje de usuario
    user = (
        f"Pregunta del usuario: {question}\n"
        f"Período de referencia: {win.text} "
        f"({win.start.date()} a {win.end.date()}).\n"
        "Devuelve solo el JSON pedido, sin comentarios adicionales."
    )

    # 👇 ahora sí: usamos el helper central
    raw = call_llm_json(
        system=system,
        user=user,
        model="gpt-4o-mini",
        temperature=0.0,
    )

    actions = raw.get("actions") or []
    reasons = raw.get("reasons") or []

    # Validación mínima para evitar cosas raras
    cleaned_actions: List[Dict[str, Any]] = []
    valid_ids = {s["id"] for s in SKILLS_CXP}

    for a in actions:
        if not isinstance(a, dict):
            continue
        name = a.get("name")
        if name not in valid_ids:
            continue
        params = a.get("params") or {}

        # defaults simples
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


def _aging_and_totals_db(ref_date: date) -> Tuple[Dict[str, float], float, float]:
    """
    Devuelve:
      - aging SOLO vencido con llaves: 0_30, 31_60, 61_90, 90_plus
      - total_por_pagar (saldo abierto)
      - por_vencer (no vencido + sin fecha)
    """
    db = SessionLocal()
    overdue = {
        "0_30": Decimal("0"),
        "31_60": Decimal("0"),
        "61_90": Decimal("0"),
        "90_plus": Decimal("0"),
    }
    current = Decimal("0")
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
                current += saldo
            elif days <= 30:
                overdue["0_30"] += saldo
            elif days <= 60:
                overdue["31_60"] += saldo
            elif days <= 90:
                overdue["61_90"] += saldo
            else:
                overdue["90_plus"] += saldo

        total_por_pagar = float(current + no_due_date + sum(overdue.values()))
        por_vencer = float(current + no_due_date)
        return ({k: float(v) for k, v in overdue.items()}, total_por_pagar, por_vencer)
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
            days_over = max((ref_date - f.fecha_limite.date()).days, 0) if f.fecha_limite else 0
            if days_over <= 0:
                continue
            proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
            rows.append(
                {
                    "invoice_id": f.numero_factura,
                    "supplier": proveedor,
                    "due_date": f.fecha_limite.date() if f.fecha_limite else None,
                    "days_overdue": days_over,
                    "outstanding": saldo,
                }
            )
        rows.sort(key=lambda r: (r["days_overdue"], r["outstanding"]), reverse=True)
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
            days_to = (f.fecha_limite.date() - ref_date).days
            if 0 <= days_to <= int(max_days):
                proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
                rows.append(
                    {
                        "invoice_id": f.numero_factura,
                        "supplier": proveedor,
                        "due_date": f.fecha_limite.date(),
                        "days_to_due": days_to,
                        "outstanding": saldo,
                    }
                )
        rows.sort(key=lambda r: (r["days_to_due"], -r["outstanding"]))
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
            days_over = max((ref_date - f.fecha_limite.date()).days, 0) if f.fecha_limite else 0
            rows.append(
                {
                    "invoice_id": f.numero_factura,
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
        for f in db.query(FacturaCXP):
            saldo = float(_saldo_cxp(f))
            if saldo <= 0:
                continue
            due = f.fecha_limite.date() if f.fecha_limite else None
            days_over = max((ref_date - due).days, 0) if due else 0
            status = "open_on_time" if days_over == 0 else "overdue"
            proveedor = f.proveedor.nombre_legal if f.proveedor else str(f.id_entidad_proveedor)
            rows.append(
                {
                    "invoice_id": f.numero_factura,
                    "supplier": proveedor,
                    "due_date": due,
                    "status": status,
                    "days_overdue": days_over,
                    "outstanding": saldo,
                }
            )
        rows.sort(key=lambda r: (r["status"], -r["days_overdue"], -r["outstanding"]))
        return rows
    finally:
        db.close()


def _count_open_db(ref_date: date) -> int:
    """
    Conteo de facturas abiertas (saldo > 0) al ref_date.
    No distingue vencidas vs al día: sólo cuenta abiertas.
    """
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
    """
    Equivalente a lo que antes hacía Agent.handle, pero sin tocar 'self' ni poner 'agent'.
    """
    question = (payload.get("question") or "").strip()
    forced_action: str = (payload.get("action") or "").strip()
    params_in: Dict[str, Any] = payload.get("params", {}) or {}

    # 1) Período
    win = _resolve_period(payload, state)
    ref_date = win.end.date()

    # 2) KPI base (DPO) + aging vencido y totales
    repo = FinanzasRepoDB()
    try:
        kpi_dpo = repo.dpo(win.start.year, win.start.month)
    except Exception:
        kpi_dpo = None

    try:
        aging_overdue, total_por_pagar, por_vencer = _aging_and_totals_db(ref_date)
    except Exception as e:
        return {"error": f"Error leyendo CxP DB: {e}"}

    # Derivados útiles para el BSC/Resumen
    overdue_total = float(
        (aging_overdue.get("0_30", 0.0) or 0.0)
        + (aging_overdue.get("31_60", 0.0) or 0.0)
        + (aging_overdue.get("61_90", 0.0) or 0.0)
        + (aging_overdue.get("90_plus", 0.0) or 0.0)
    )
    open_count = _count_open_db(ref_date)

    data_norm = {
        "period": win.text,
        "kpi": {"DPO": kpi_dpo},
        "aging": {
            "0_30": float(aging_overdue.get("0_30", 0.0)),
            "31_60": float(aging_overdue.get("31_60", 0.0)),
            "61_90": float(aging_overdue.get("61_90", 0.0)),
            "90_plus": float(aging_overdue.get("90_plus", 0.0)),
        },
        "total_por_pagar": float(total_por_pagar),
        "por_vencer": float(por_vencer),
        "current": float(por_vencer),  # alias para UI
        # nuevos campos derivados
        "overdue_total": overdue_total,
        "open_invoices": int(open_count),
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
            # Si el planner LLM falla, usamos el planner clásico
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
            # ya cubierto con data_norm
            pass
        elif name == "aging":
            snap = [
                {"bucket": "0_30", "amount": data_norm["aging"]["0_30"]},
                {"bucket": "31_60", "amount": data_norm["aging"]["31_60"]},
                {"bucket": "61_90", "amount": data_norm["aging"]["61_90"]},
                {"bucket": "90_plus", "amount": data_norm["aging"]["90_plus"]},
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
                result_tables.append(
                    {
                        "action": "supplier_balance",
                        "error": "Falta 'supplier' en params",
                    }
                )
            else:
                total, rows = _supplier_balance_db(supp, ref_date)
                result_tables.append(
                    {
                        "action": "supplier_balance",
                        "total_outstanding": total,
                        "rows": rows,
                    }
                )
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

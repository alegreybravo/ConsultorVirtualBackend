# app/graph_lc.py
from __future__ import annotations

from typing import Dict, Any, Optional, List
from datetime import datetime
import re
from zoneinfo import ZoneInfo

from app.state import GlobalState
from app.router import Router
from app.utils.knowledge_base import (
    get_applicable_rules,
    get_agent_kb,   # por si luego quieres usar pathways/concepts
)
from app.agents.intent import route_intent
from app.repo_finanzas_db import FinanzasRepoDB


TZ = ZoneInfo("America/Costa_Rica")

# --- Regex para extraer 2 fechas del texto ---
_RX_DATE_DMY = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")    # 29/10/2025
_RX_DATE_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")      # 2025-10-29


def _extract_two_dates(question: str) -> tuple[Optional[datetime], Optional[datetime]]:
    """
    Extrae dos fechas explícitas del texto.
    Devuelve (start_dt, end_dt) en TZ America/Costa_Rica, con:
      start: 00:00:00
      end:   23:59:59
    Acepta:
      - 29/10/2025 ... 05/11/2025
      - 2025-10-29 ... 2025-11-05
    """
    text = question or ""

    dmy = _RX_DATE_DMY.findall(text)
    if len(dmy) >= 2:
        (d1, m1, y1), (d2, m2, y2) = dmy[0], dmy[1]
        start = datetime(int(y1), int(m1), int(d1), 0, 0, 0, tzinfo=TZ)
        end = datetime(int(y2), int(m2), int(d2), 23, 59, 59, tzinfo=TZ)
        return start, end

    iso = _RX_DATE_ISO.findall(text)
    if len(iso) >= 2:
        (y1, m1, d1), (y2, m2, d2) = iso[0], iso[1]
        start = datetime(int(y1), int(m1), int(d1), 0, 0, 0, tzinfo=TZ)
        end = datetime(int(y2), int(m2), int(d2), 23, 59, 59, tzinfo=TZ)
        return start, end

    return None, None


# --------------------------------------------------------------------
# ✅ CXC-06: Extraer UNA fecha (para "vencen hoy (29/10/2025)")
# --------------------------------------------------------------------
def _extract_one_date(question: str) -> Optional[datetime]:
    """
    Extrae una fecha explícita del texto y retorna datetime en TZ con corte 23:59:59.
    Acepta:
      - 29/10/2025
      - 2025-10-29
    """
    text = question or ""

    m = _RX_DATE_DMY.search(text)
    if m:
        try:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d, 23, 59, 59, tzinfo=TZ)
        except Exception:
            return None

    m = _RX_DATE_ISO.search(text)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            return datetime(y, mo, d, 23, 59, 59, tzinfo=TZ)
        except Exception:
            return None

    return None


# --------------------------------------------------------------------
# Clasificador de modo de datos: "db" vs "advisory"
# --------------------------------------------------------------------
def _classify_data_mode(
    metrics: Dict[str, Any],
    trace: List[Dict[str, Any]],
) -> str:
    metrics = metrics or {}
    trace = trace or []

    for key in ("dso", "dpo", "ccc", "cash"):
        val = metrics.get(key)
        if isinstance(val, (int, float)):
            return "db"

    for item in trace:
        if not isinstance(item, dict):
            continue
        agent_name = item.get("agent")
        if agent_name in ("aaav_cxc", "aaav_cxp", "aav_contable"):
            data = item.get("data")
            if isinstance(data, dict) and data:
                return "db"

    return "advisory"


def _norm_aging(aging: dict | None) -> dict:
    aging = aging or {}
    return {
        "0-30": float(aging.get("0_30") or aging.get("1-30") or aging.get("0-30") or 0),
        "31-60": float(aging.get("31_60") or aging.get("31-60") or 0),
        "61-90": float(aging.get("61_90") or aging.get("61-90") or 0),
        "90+": float(aging.get("90_plus") or aging.get("+90") or aging.get("90+") or 0),
    }


def _get_agent_data(result: Dict[str, Any], agent_name: str) -> Dict[str, Any]:
    for tr in result.get("trace") or []:
        if tr.get("agent") == agent_name:
            return tr.get("data") or {}
    return {}


def _build_metrics_global(result: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}

    base = result.get("metrics") or {}
    for k, v in base.items():
        metrics[k] = v
        metrics[k.lower()] = v

    exec_pack = (result.get("gerente") or {}).get("executive_decision_bsc") or {}
    kpis_exec = exec_pack.get("kpis") or {}
    for k, v in kpis_exec.items():
        metrics.setdefault(k, v)
        metrics.setdefault(k.lower(), v)

    return metrics


def _build_metrics_cxc(result: Dict[str, Any]) -> Dict[str, float]:
    data = _get_agent_data(result, "aaav_cxc")
    aging_norm = _norm_aging(data.get("aging") or {})
    vencido = aging_norm["31-60"] + aging_norm["61-90"] + aging_norm["90+"]

    metrics_global = _build_metrics_global(result)
    ratio_cxc_cxp = float(metrics_global.get("ratio_cxc_cxp") or metrics_global.get("ratio_cxc_cxp".lower()) or 0)

    return {
        "monto_cxc_vencidas": float(data.get("monto_cxc_vencidas") or vencido),
        "dias_envejecimiento_cxc": float(data.get("dias_envejecimiento_cxc") or 0.0),
        "ratio_cxc_cxp": ratio_cxc_cxp,
    }


def _build_metrics_cxp(result: Dict[str, Any]) -> Dict[str, float]:
    data = _get_agent_data(result, "aaav_cxp")
    aging_norm = _norm_aging(data.get("aging") or {})
    vencido = aging_norm["31-60"] + aging_norm["61-90"] + aging_norm["90+"]

    return {
        "monto_cxp_vencidas": float(data.get("monto_cxp_vencidas") or vencido),
        "dias_envejecimiento": float(data.get("dias_envejecimiento") or 0.0),
    }


def _attach_due_range_summary(result: Dict[str, Any]) -> None:
    """
    CXC-03: Adjunta un resumen de vencimientos en rango a:
      gerente.executive_decision_bsc.executive_context.due_range_summary

    Usa:
      result["_meta"]["intent"]["vencimientos_rango"] = True
      result["_meta"]["date_range"] = {start,end,...} (ISO strings)
    """
    meta = result.get("_meta") or {}
    intent = meta.get("intent") or {}
    dr = meta.get("date_range") or {}

    if not (intent.get("vencimientos_rango") is True and dr.get("start") and dr.get("end")):
        return

    try:
        start = datetime.fromisoformat(dr["start"])
        end = datetime.fromisoformat(dr["end"])
    except Exception:
        return

    repo = FinanzasRepoDB()
    summary = repo.cxc_due_between(start, end)

    if "total" in summary and "saldo_total" not in summary:
        summary["saldo_total"] = summary["total"]

    if isinstance(summary.get("start"), str) and len(summary["start"]) >= 10:
        summary["start"] = summary["start"][:10]
    if isinstance(summary.get("end"), str) and len(summary["end"]) >= 10:
        summary["end"] = summary["end"][:10]

    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}
    ctx = exec_pack.get("executive_context") or {}
    ctx["due_range_summary"] = summary
    exec_pack["executive_context"] = ctx
    gerente["executive_decision_bsc"] = exec_pack
    result["gerente"] = gerente


# --------------------------------------------------------------------
# ✅ CXC-04: Top 5 clientes por saldo CxC abierto al corte
# --------------------------------------------------------------------
def _attach_top_clientes_cxc(result: Dict[str, Any], question: str) -> None:
    meta = result.get("_meta") or {}
    intent = meta.get("intent") or {}
    if intent.get("top_clientes_cxc") is not True:
        return

    as_of: Optional[datetime] = None

    pr = meta.get("period_resolved") or {}
    txt = str(pr.get("text") or "")
    if "fecha:" in txt:
        try:
            date_str = txt.split("fecha:")[-1].strip()
            as_of = datetime.fromisoformat(f"{date_str}T23:59:59").replace(tzinfo=TZ)
        except Exception:
            as_of = None

    if as_of is None:
        m = _RX_DATE_ISO.search(question or "")
        if m:
            try:
                y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                as_of = datetime(y, mo, d, 23, 59, 59, tzinfo=TZ)
            except Exception:
                as_of = None

    if as_of is None:
        return

    repo = FinanzasRepoDB()
    summary = repo.cxc_top_clients_open(as_of, limit=5)

    if isinstance(summary.get("as_of"), str) and len(summary["as_of"]) >= 10:
        summary["as_of"] = summary["as_of"][:10]

    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}
    ctx = exec_pack.get("executive_context") or {}

    ctx["top_clientes_cxc"] = summary
    exec_pack["executive_context"] = ctx
    gerente["executive_decision_bsc"] = exec_pack
    result["gerente"] = gerente


# --------------------------------------------------------------------
# ✅ CXC-06: Facturas CxC que vencen en una fecha (hoy = fecha indicada)
# --------------------------------------------------------------------
def _attach_cxc_due_on(result: Dict[str, Any]) -> None:
    """
    Adjunta en:
      gerente.executive_decision_bsc.executive_context.cxc_due_on

    Requiere:
      result["_meta"]["intent"]["vencen_hoy_cxc"] = True
      result["_meta"]["due_on"]["date"] = ISO string
    """
    meta = result.get("_meta") or {}
    intent = meta.get("intent") or {}
    due_on = meta.get("due_on") or {}

    if intent.get("vencen_hoy_cxc") is not True:
        return
    if not due_on.get("date"):
        return

    try:
        as_of = datetime.fromisoformat(due_on["date"])
    except Exception:
        return

    repo = FinanzasRepoDB()
    # ✅ asumimos que ya existe en tu repo:
    # def cxc_invoices_due_on(self, due_on: datetime) -> dict
    summary = repo.cxc_invoices_due_on(as_of)

    # presentación: solo fecha
    if isinstance(summary.get("date"), str) and len(summary["date"]) >= 10:
        summary["date"] = summary["date"][:10]

    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}
    ctx = exec_pack.get("executive_context") or {}

    ctx["cxc_due_on"] = summary
    exec_pack["executive_context"] = ctx
    gerente["executive_decision_bsc"] = exec_pack
    result["gerente"] = gerente


def run_query(
    question: str,
    period: Optional[str] = None,
    company_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    state = GlobalState()
    state.period_raw = period
    state.company_context = company_context or {}

    intent = None
    date_range_meta = None
    due_on_meta = None  # ✅ CXC-06

    try:
        intent = route_intent(question)

        # ✅ CXC-03
        if getattr(intent, "vencimientos_rango", False):
            start_dt, end_dt = _extract_two_dates(question)
            if start_dt and end_dt:
                date_range_meta = {
                    "text": "rango_pregunta",
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                    "granularity": "range",
                    "source": "question",
                    "tz": str(TZ),
                }

        # ✅ CXC-06: vencen hoy (con fecha explícita)
        if getattr(intent, "vencen_hoy_cxc", False):
            one = _extract_one_date(question)

            # ✅ fallback: si dicen "hoy" pero no dan fecha, usar hoy real
            if one is None and re.search(r"\b(hoy|para hoy|del día)\b", (question or "").lower()):
                now = datetime.now(TZ)
                one = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=TZ)

            if one:
                due_on_meta = {
                    "text": "fecha_pregunta",
                    "date": one.isoformat(),
                    "source": "question",
                    "tz": str(TZ),
                }


    except Exception:
        intent = None
        date_range_meta = None
        due_on_meta = None

    payload: Dict[str, Any] = {"question": question, "period": period}

    if date_range_meta:
        payload["period_override"] = date_range_meta

    router = Router()
    result = router.dispatch({"payload": payload}, state)

    meta = result.get("_meta") or {}
    if intent is not None:
        meta["intent"] = intent.model_dump()
        if date_range_meta:
            meta["date_range"] = date_range_meta
        if due_on_meta:
            meta["due_on"] = due_on_meta  # ✅ CXC-06
    else:
        meta["intent"] = {
            "cxc": True,
            "cxp": True,
            "informe": False,
            "aging": False,
            "vencimientos_rango": False,
            "top_clientes_cxc": False,
            "vencen_hoy_cxc": False,  # ✅ ADD
            "reason": "intent error",
        }
    result["_meta"] = meta

    metrics_global = _build_metrics_global(result)
    metrics_cxc = _build_metrics_cxc(result)
    metrics_cxp = _build_metrics_cxp(result)

    trace = result.get("trace") or []
    data_mode = _classify_data_mode(metrics_global, trace)

    kb_rules: Dict[str, Any] = {
        "av_gerente": get_applicable_rules("av_gerente", metrics=metrics_global, text_query=question),
        "av_administrativo": get_applicable_rules("av_administrativo", metrics=metrics_global, text_query=question),
        "aaav_cxc": get_applicable_rules("aaav_cxc", metrics=metrics_cxc, text_query=question),
        "aaav_cxp": get_applicable_rules("aaav_cxp", metrics=metrics_cxp, text_query=question),
        "av_finanzas": get_applicable_rules("av_finanzas", metrics=metrics_global, text_query=question),
        "av_contador_financiero": get_applicable_rules("av_contador_financiero", metrics=metrics_global, text_query=question),
        "aav_contador": get_applicable_rules("aav_contador", metrics=metrics_global, text_query=question),
    }

    result["kb_rules"] = kb_rules
    meta = result.get("_meta") or {}
    meta["data_mode"] = data_mode
    result["_meta"] = meta

    _attach_due_range_summary(result)
    _attach_top_clientes_cxc(result, question=question)

    # ✅ CXC-06
    _attach_cxc_due_on(result)


    print("=== DEBUG GRAPH CXC-06 ===")
    print("INTENT:", result.get("_meta", {}).get("intent"))
    print("DUE_ON META:", result.get("_meta", {}).get("due_on"))
    print("EXEC CTX KEYS:",
        list(
            (
                result.get("gerente", {})
                .get("executive_decision_bsc", {})
                .get("executive_context", {})
            ).keys()
        )
    )
    print("==========================")

    return result

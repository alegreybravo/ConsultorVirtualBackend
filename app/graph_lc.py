# app/graph_lc.py
from __future__ import annotations

from typing import Dict, Any, Optional, List
from datetime import datetime
import re
from zoneinfo import ZoneInfo

from app.state import GlobalState
from app.router import Router
from app.utils.knowledge_base import get_applicable_rules
from app.agents.intent import route_intent
from app.utils.executive_summary import generate_executive_summary

TZ = ZoneInfo("America/Costa_Rica")

_RX_DATE_DMY = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")    # 29/10/2025
_RX_DATE_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")      # 2025-10-29


def _extract_two_dates(question: str) -> tuple[Optional[datetime], Optional[datetime]]:
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


def _extract_one_date(question: str) -> Optional[datetime]:
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


def _classify_data_mode(metrics: Dict[str, Any], trace: List[Dict[str, Any]]) -> str:
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


def _merge_executive_context_patches(result: Dict[str, Any]) -> None:
    """
    Busca en trace items que traigan executive_context_patch y los mergea en:
      result["gerente"]["executive_decision_bsc"]["executive_context"]
    """
    trace = result.get("trace") or []
    gerente = result.setdefault("gerente", {})
    exec_pack = gerente.setdefault("executive_decision_bsc", {})
    exec_ctx = exec_pack.setdefault("executive_context", {})

    for tr in trace:
        if not isinstance(tr, dict):
            continue
        patch = tr.get("executive_context_patch")
        if isinstance(patch, dict) and patch:
            exec_ctx.update(patch)

    exec_pack["executive_context"] = exec_ctx
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
    due_on_meta = None
    as_of_meta = None  # ✅ SIEMPRE local por request

    try:
        intent = route_intent(question)

        # rango para CXC-03 o CXC-07
        if getattr(intent, "vencimientos_rango", False) or getattr(intent, "cxc_pago_parcial", False):
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

        # fecha única para CXC-06
        if getattr(intent, "vencen_hoy_cxc", False):
            one = _extract_one_date(question)
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

        # ✅ fecha única para CXC-04 (top clientes) y CXC-08 (saldo cliente al corte)
        if getattr(intent, "top_clientes_cxc", False) or getattr(intent, "saldo_cliente_cxc", False):
            one = _extract_one_date(question)
            if one is None and re.search(r"\b(hoy|para hoy|del día)\b", (question or "").lower()):
                now = datetime.now(TZ)
                one = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=TZ)

            if one:
                as_of_meta = {
                    "text": "fecha_pregunta",
                    "as_of": one.isoformat(),
                    "source": "question",
                    "tz": str(TZ),
                }

    except Exception:
        intent = None
        date_range_meta = None
        due_on_meta = None
        as_of_meta = None

    # -------------------------
    # ✅ payload con _meta ANTES del dispatch
    # -------------------------
    payload: Dict[str, Any] = {"question": question, "period": period}
    payload["_meta"] = {
        "intent": intent.model_dump() if intent is not None else {},
        "date_range": date_range_meta,
        "due_on": due_on_meta,
        "as_of": as_of_meta,
    }

    # (si tu router usa period_override, lo mantenemos)
    if date_range_meta and intent is not None and (
        getattr(intent, "vencimientos_rango", False) or getattr(intent, "cxc_pago_parcial", False)
    ):
        payload["period_override"] = date_range_meta

    router = Router()
    result = router.dispatch({"payload": payload}, state)

    # -------------------------
    # _meta final en result
    # -------------------------
    meta = result.get("_meta") or {}
    if intent is not None:
        meta["intent"] = intent.model_dump()
        if date_range_meta:
            meta["date_range"] = date_range_meta
        if due_on_meta:
            meta["due_on"] = due_on_meta
        if as_of_meta:
            meta["as_of"] = as_of_meta
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
        "aav_contador_financiero": get_applicable_rules("aav_contador_financiero", metrics=metrics_global, text_query=question),
        "aav_contador": get_applicable_rules("aav_contador", metrics=metrics_global, text_query=question),
    }

    result["kb_rules"] = kb_rules
    meta = result.get("_meta") or {}
    meta["data_mode"] = data_mode
    result["_meta"] = meta

    # -------------------------
    # ✅ merge genérico de patches
    # -------------------------
    _merge_executive_context_patches(result)

    # -------------------------
    # resumen ejecutivo
    # -------------------------
    try:
        exec_pack = (result.get("gerente") or {}).get("executive_decision_bsc") or {}
        exec_ctx = exec_pack.get("executive_context") or {}
        intent_meta = (result.get("_meta") or {}).get("intent") or {}

        new_summary = generate_executive_summary(
            question=question,
            intent=intent_meta,
            period_resolved=(result.get("_meta") or {}).get("period_resolved") or {},
            kpis=(exec_pack.get("kpis") or (result.get("metrics") or {})),
            executive_context=exec_ctx,
        )

        if isinstance(new_summary, str) and new_summary.strip():
            exec_pack["resumen_ejecutivo"] = new_summary.strip()
            gerente = result.get("gerente") or {}
            gerente["executive_decision_bsc"] = exec_pack
            result["gerente"] = gerente

    except Exception:
        pass

    return result

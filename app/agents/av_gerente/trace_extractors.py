# app/agents/av_gerente/trace_extractors.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .utils import coerce_float, truncate


MAX_TRACE_ITEMS_DEFAULT = 30
MAX_FIELD_CHARS_DEFAULT = 2_000


def summarize_trace(
    trace: List[Dict[str, Any]],
    max_trace_items: int = MAX_TRACE_ITEMS_DEFAULT,
    max_field_chars: int = MAX_FIELD_CHARS_DEFAULT,
) -> Tuple[str, Dict[str, Any]]:
    if not trace:
        return "(sin resultados de subagentes)", {
            "dso": None,
            "dpo": None,
            "ccc": None,
            "cash": None,
        }

    trimmed = trace[:max_trace_items]
    lines: List[str] = []
    dso = dpo = ccc = cash = None

    for res in trimmed:
        agent_name = res.get("agent", "Agente")
        summary = res.get("summary")
        if not isinstance(summary, str):
            summary_candidates = []
            for k in ("status", "highlights", "top_issues", "notes"):
                if k in res:
                    summary_candidates.append(f"{k}: {res[k]}")
            summary = "; ".join(map(str, summary_candidates)) or str({k: res[k] for k in list(res)[:6]})

        lines.append(f"{agent_name}: {truncate(summary, max_field_chars)}")

        if dso is None and "dso" in res:
            dso = coerce_float(res.get("dso"))
        if dpo is None and "dpo" in res:
            dpo = coerce_float(res.get("dpo"))
        if ccc is None and "ccc" in res:
            ccc = coerce_float(res.get("ccc"))
        if cash is None and "cash" in res:
            cash = coerce_float(res.get("cash"))

    return "\n".join(lines), {"dso": dso, "dpo": dpo, "ccc": ccc, "cash": cash}


def extract_aging(trace: List[Dict[str, Any]], agent_name: str) -> Dict[str, Any]:
    """
    Devuelve:
    {
      "overdue": {...},
      "current": {...},
      "legacy": {...}
    }
    """
    for res in trace or []:
        if res.get("agent") == agent_name:
            data = res.get("data") or {}

            overdue = data.get("aging_overdue")
            current = data.get("aging_current")
            legacy = data.get("aging")

            out = {"overdue": {}, "current": {}, "legacy": {}}

            if isinstance(overdue, dict):
                out["overdue"] = overdue
            if isinstance(current, dict):
                out["current"] = current
            if isinstance(legacy, dict):
                out["legacy"] = legacy
            return out

    return {"overdue": {}, "current": {}, "legacy": {}}


def extract_context(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "kpis": {"DSO": None, "DPO": None, "DIO": None, "CCC": None},
        "aging_cxc_overdue": {},
        "aging_cxc_current": {},
        "aging_cxp_overdue": {},
        "aging_cxp_current": {},
        "aging_cxc": {},
        "aging_cxp": {},
        "balances": {},
    }

    cxc_pack = extract_aging(trace, "aaav_cxc")
    cxp_pack = extract_aging(trace, "aaav_cxp")

    ctx["aging_cxc_overdue"] = cxc_pack.get("overdue") or {}
    ctx["aging_cxc_current"] = cxc_pack.get("current") or {}
    ctx["aging_cxc"] = cxc_pack.get("legacy") or ctx["aging_cxc_overdue"] or {}

    ctx["aging_cxp_overdue"] = cxp_pack.get("overdue") or {}
    ctx["aging_cxp_current"] = cxp_pack.get("current") or {}
    ctx["aging_cxp"] = cxp_pack.get("legacy") or ctx["aging_cxp_overdue"] or {}

    for res in trace or []:
        data = res.get("data") or {}
        kpi = data.get("kpi") or {}
        if isinstance(kpi, dict):
            for k in ("DSO", "DPO", "DIO", "CCC"):
                if ctx["kpis"].get(k) is None and k in kpi:
                    ctx["kpis"][k] = coerce_float(kpi.get(k))

        bal = data.get("balances") or {}
        if isinstance(bal, dict) and not ctx["balances"]:
            ctx["balances"] = {str(k): coerce_float(v) for k, v in bal.items()}

    return ctx


def extract_operational_totals(trace: List[Dict[str, Any]], ctx: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "ar_outstanding": None,
        "ar_open_invoices": None,
        "ap_outstanding": None,
        "ap_open_invoices": None,
        "nwc_proxy": None,
    }

    balances = ctx.get("balances") or {}
    if isinstance(balances, dict):
        out["nwc_proxy"] = coerce_float(balances.get("NWC_proxy"))

    for res in trace or []:
        agent = res.get("agent")
        data = res.get("data") or {}
        if agent == "aaav_cxc":
            if out["ar_outstanding"] is None:
                out["ar_outstanding"] = coerce_float(data.get("total_por_cobrar"))
            if out["ar_open_invoices"] is None:
                oi = data.get("open_invoices")
                try:
                    out["ar_open_invoices"] = int(oi) if oi is not None else None
                except Exception:
                    pass

        elif agent == "aaav_cxp":
            if out["ap_outstanding"] is None:
                out["ap_outstanding"] = coerce_float(data.get("total_por_pagar"))
            if out["ap_open_invoices"] is None:
                oi = data.get("open_invoices")
                try:
                    out["ap_open_invoices"] = int(oi) if oi is not None else None
                except Exception:
                    pass

    return out

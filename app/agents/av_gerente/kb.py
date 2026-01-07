# app/agents/av_gerente/kb.py
from __future__ import annotations

from typing import Any, Dict, List

from ...utils.knowledge_base import get_applicable_rules  # KB


def rule_priority(rule: Dict[str, Any]) -> int:
    scopes = rule.get("scope") or []
    if not isinstance(scopes, list):
        scopes = [scopes]

    scopes_lower = {str(s).lower() for s in scopes}
    if "riesgo" in scopes_lower or "alerta" in scopes_lower:
        return 0
    if "operativo" in scopes_lower:
        return 1
    if "consultivo" in scopes_lower or "gerencial" in scopes_lower:
        return 2
    return 3


def associate_rules_with_kpis(rules: List[Dict[str, Any]], ctx: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    assoc: Dict[str, List[Dict[str, Any]]] = {
        "DSO": [],
        "DPO": [],
        "CCC": [],
        "CxC_vencidas": [],
        "CxP_vencidas": [],
        "generales": [],
    }

    aging_cxc = ctx.get("aging_cxc_overdue") or ctx.get("aging_cxc") or {}
    aging_cxp = ctx.get("aging_cxp_overdue") or ctx.get("aging_cxp") or {}

    legacy = ("0_30", "31_60", "61_90", "90_plus")
    new = ("overdue_1_30", "overdue_31_60", "overdue_61_90", "overdue_90_plus")

    def _has_any_overdue(a: Dict[str, Any]) -> bool:
        keys = legacy if any(k in a for k in legacy) else new
        return any(isinstance(a.get(k), (int, float)) and a.get(k) > 0 for k in keys)

    cxc_vencidas = _has_any_overdue(aging_cxc)
    cxp_vencidas = _has_any_overdue(aging_cxp)

    for r in rules or []:
        if not isinstance(r, dict):
            continue

        attached = False

        conds = r.get("conditions") or []
        if isinstance(conds, list):
            for c in conds:
                if not isinstance(c, dict):
                    continue
                metric_name = str(c.get("metric") or "").lower()
                if metric_name in ("dso", "dias_envejecimiento_cxc"):
                    assoc["DSO"].append(r); attached = True
                elif metric_name in ("dpo", "dias_envejecimiento", "dias_atraso_promedio"):
                    assoc["DPO"].append(r); attached = True
                elif metric_name in ("ccc", "ciclo_caja"):
                    assoc["CCC"].append(r); attached = True
                elif metric_name in ("monto_cxc_vencidas", "monto_cxc_vencida"):
                    assoc["CxC_vencidas"].append(r); attached = True
                elif metric_name in ("monto_cxp_vencidas", "monto_cxp_vencida"):
                    assoc["CxP_vencidas"].append(r); attached = True

        tags = {str(t).lower() for t in (r.get("tags") or [])}
        if "vencimientos" in tags or "cxc_vencidas" in tags or "morosidad" in tags:
            if cxc_vencidas:
                assoc["CxC_vencidas"].append(r); attached = True
        if "vencimientos" in tags or "cxp_vencidas" in tags:
            if cxp_vencidas:
                assoc["CxP_vencidas"].append(r); attached = True

        if not attached:
            assoc["generales"].append(r)

    for key, lst in assoc.items():
        assoc[key] = sorted(lst, key=rule_priority)

    return assoc


def build_kb_rules(
    agent_name: str,
    question: str,
    metrics_for_kb: Dict[str, Any],
    company_context: Dict[str, Any],
    payload_kb_rules: Any,
    state_kb_rules: Any,
) -> List[Dict[str, Any]]:
    kb_rules_global: Dict[str, Any] = (payload_kb_rules or state_kb_rules or {}) if isinstance(payload_kb_rules or state_kb_rules or {}, dict) else {}

    precomputed_rules: List[Dict[str, Any]] = []
    if isinstance(kb_rules_global, dict):
        maybe = kb_rules_global.get(agent_name)
        if isinstance(maybe, list):
            precomputed_rules = maybe

    rules_local = get_applicable_rules(
        agent_name,
        metrics=metrics_for_kb,
        text_query=question,
        context=company_context,
    )

    kb_rules: List[Dict[str, Any]] = []
    seen_ids = set()
    for r in (precomputed_rules or []) + (rules_local or []):
        if not isinstance(r, dict):
            continue
        rid = r.get("id")
        if rid:
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
        kb_rules.append(r)

    kb_rules = sorted(kb_rules, key=rule_priority)
    return kb_rules

def inherit_rules_from_trace(kb_rules_global: dict, trace: list, exclude: set[str] | None = None) -> list[dict]:
    exclude = exclude or set()
    agents_in_trace = [t.get("agent") for t in trace if isinstance(t, dict) and t.get("agent")]
    out = []
    seen = set()
    for a in agents_in_trace:
        if a in exclude:
            continue
        for r in (kb_rules_global.get(a) or []):
            rid = r.get("id")
            if rid and rid in seen:
                continue
            if rid:
                seen.add(rid)
            out.append(r)
    return out


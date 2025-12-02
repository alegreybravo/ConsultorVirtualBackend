# app/agents/av_gerente/kb_support.py
from __future__ import annotations
from typing import Dict, Any, List


def rule_priority(rule: Dict[str, Any]) -> int:
    """
    Asigna prioridad numérica a una regla según su 'scope':
    - riesgo / alerta -> 0 (más importante)
    - operativo       -> 1
    - consultivo/gerencial -> 2
    - otras/indefinidas -> 3
    """
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


def associate_rules_with_kpis(
    rules: List[Dict[str, Any]],
    ctx: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Crea un mapa de reglas KB asociadas a ciertos indicadores/situaciones:
      - DSO, DPO, CCC
      - CxC_vencidas, CxP_vencidas
      - generales (reglas que aplican al contexto pero no a un KPI específico)
    Esto sirve para que el LLM pueda conectar KPIs → reglas concretas.
    """
    assoc: Dict[str, List[Dict[str, Any]]] = {
        "DSO": [],
        "DPO": [],
        "CCC": [],
        "CxC_vencidas": [],
        "CxP_vencidas": [],
        "generales": [],
    }

    aging_cxc = ctx.get("aging_cxc") or {}
    aging_cxp = ctx.get("aging_cxp") or {}

    cxc_vencidas = any(
        isinstance(aging_cxc.get(bucket), (int, float)) and aging_cxc.get(bucket) > 0
        for bucket in ("31_60", "61_90", "90_plus")
    )
    cxp_vencidas = any(
        isinstance(aging_cxp.get(bucket), (int, float)) and aging_cxp.get(bucket) > 0
        for bucket in ("31_60", "61_90", "90_plus")
    )

    for r in rules or []:
        if not isinstance(r, dict):
            continue

        attached = False

        # 1) Reglas con conditions explícitas de métricas
        conds = r.get("conditions") or []
        if isinstance(conds, list):
            for c in conds:
                if not isinstance(c, dict):
                    continue
                metric_name = str(c.get("metric") or "").lower()
                if metric_name in ("dso", "dias_envejecimiento_cxc"):
                    assoc["DSO"].append(r)
                    attached = True
                elif metric_name in ("dpo", "dias_envejecimiento", "dias_atraso_promedio"):
                    assoc["DPO"].append(r)
                    attached = True
                elif metric_name in ("ccc", "ciclo_caja"):
                    assoc["CCC"].append(r)
                    attached = True
                elif metric_name in ("monto_cxc_vencidas", "monto_cxc_vencida"):
                    assoc["CxC_vencidas"].append(r)
                    attached = True
                elif metric_name in ("monto_cxp_vencidas", "monto_cxp_vencida"):
                    assoc["CxP_vencidas"].append(r)
                    attached = True

        # 2) Tags basados en vencimientos
        tags = {str(t).lower() for t in (r.get("tags") or [])}
        if "vencimientos" in tags or "cxc_vencidas" in tags or "morosidad" in tags:
            if cxc_vencidas:
                assoc["CxC_vencidas"].append(r)
                attached = True
        if "vencimientos" in tags or "cxp_vencidas" in tags:
            if cxp_vencidas:
                assoc["CxP_vencidas"].append(r)
                attached = True

        # 3) Si no se pudo asociar a un KPI concreto, va a 'generales'
        if not attached:
            assoc["generales"].append(r)

    # Ordenar cada lista según prioridad (riesgo > operativo > consultivo > otras)
    for key, lst in assoc.items():
        assoc[key] = sorted(lst, key=rule_priority)

    return assoc

# app/graph_lc.py
from __future__ import annotations

from typing import Dict, Any, Optional, List

from app.state import GlobalState
from app.router import Router
from app.utils.knowledge_base import (
    get_applicable_rules,
    get_agent_kb,   # por si luego quieres usar pathways/concepts
)


# --------------------------------------------------------------------
# Clasificador de modo de datos: "db" vs "advisory"
# --------------------------------------------------------------------
def _classify_data_mode(
    metrics: Dict[str, Any],
    trace: List[Dict[str, Any]],
) -> str:
    """
    Decide si estamos en modo:
      - "db": hay datos reales de la BD (KPIs numéricos, agentes CxC/CxP con data)
      - "advisory": no hay datos numéricos → sólo contexto + pregunta + KB.

    Regla simple:
      - Si hay algún KPI numérico (dso/dpo/ccc/cash) -> "db"
      - O si hay outputs de aaav_cxc / aaav_cxp / aav_contable con campo "data" dict -> "db"
      - Si no, "advisory".
    """
    metrics = metrics or {}
    trace = trace or []

    # 1) KPIs numéricos globales
    for key in ("dso", "dpo", "ccc", "cash"):
        val = metrics.get(key)
        if isinstance(val, (int, float)):
            return "db"

    # 2) Resultados de agentes de datos (CxC/CxP/contable) con contenido en "data"
    for item in trace:
        if not isinstance(item, dict):
            continue
        agent_name = item.get("agent")
        if agent_name in ("aaav_cxc", "aaav_cxp", "aav_contable"):
            data = item.get("data")
            if isinstance(data, dict) and data:
                return "db"

    # 3) Si no encontramos datos → modo asesoría
    return "advisory"


def _norm_aging(aging: dict | None) -> dict:
    """
    Normaliza el dict de aging que devuelven los agentes de CxC/CxP
    a un formato estable: keys '0-30','31-60','61-90','90+'.
    """
    aging = aging or {}
    return {
        "0-30": float(
            aging.get("0_30")
            or aging.get("1-30")
            or aging.get("0-30")
            or 0
        ),
        "31-60": float(
            aging.get("31_60")
            or aging.get("31-60")
            or 0
        ),
        "61-90": float(
            aging.get("61_90")
            or aging.get("61-90")
            or 0
        ),
        "90+": float(
            aging.get("90_plus")
            or aging.get("+90")
            or aging.get("90+")
            or 0
        ),
    }


def _get_agent_data(result: Dict[str, Any], agent_name: str) -> Dict[str, Any]:
    """
    Busca en result['trace'] el bloque correspondiente a un agente
    y retorna su 'data' (o {} si no existe).
    """
    for tr in result.get("trace") or []:
        if tr.get("agent") == agent_name:
            return tr.get("data") or {}
    return {}


def _build_metrics_global(result: Dict[str, Any]) -> Dict[str, float]:
    """
    Construye un diccionario de métricas 'globales' combinando:
      - result['metrics']
      - gerente.executive_decision_bsc.kpis (si existen)
    y normalizando algunas claves a minúsculas.
    """
    metrics: Dict[str, float] = {}

    base = result.get("metrics") or {}
    for k, v in base.items():
        metrics[k] = v
        metrics[k.lower()] = v

    # KPIs que vengan en el bloque del gerente (ej: DSO, DPO, CCC)
    exec_pack = (result.get("gerente") or {}).get("executive_decision_bsc") or {}
    kpis_exec = exec_pack.get("kpis") or {}
    for k, v in kpis_exec.items():
        metrics.setdefault(k, v)
        metrics.setdefault(k.lower(), v)

    return metrics


def _build_metrics_cxc(result: Dict[str, Any]) -> Dict[str, float]:
    """
    Construye métricas específicas para AAAV_CxC a partir del trace.
    Ejemplo:
      - monto_cxc_vencidas
      - dias_envejecimiento_cxc (si existiera en data)
      - ratio_cxc_cxp (si viene en metrics globales)
    """
    data = _get_agent_data(result, "aaav_cxc")
    aging_norm = _norm_aging(data.get("aging") or {})

    vencido = aging_norm["31-60"] + aging_norm["61-90"] + aging_norm["90+"]

    metrics_global = _build_metrics_global(result)
    ratio_cxc_cxp = float(
        metrics_global.get("ratio_cxc_cxp")
        or metrics_global.get("ratio_cxc_cxp".lower())
        or 0
    )

    return {
        "monto_cxc_vencidas": float(data.get("monto_cxc_vencidas") or vencido),
        "dias_envejecimiento_cxc": float(data.get("dias_envejecimiento_cxc") or 0.0),
        "ratio_cxc_cxp": ratio_cxc_cxp,
    }


def _build_metrics_cxp(result: Dict[str, Any]) -> Dict[str, float]:
    """
    Construye métricas específicas para AAAV_CxP a partir del trace.
    Ejemplo:
      - monto_cxp_vencidas
      - dias_envejecimiento
    """
    data = _get_agent_data(result, "aaav_cxp")
    aging_norm = _norm_aging(data.get("aging") or {})

    vencido = aging_norm["31-60"] + aging_norm["61-90"] + aging_norm["90+"]

    return {
        "monto_cxp_vencidas": float(data.get("monto_cxp_vencidas") or vencido),
        "dias_envejecimiento": float(data.get("dias_envejecimiento") or 0.0),
    }


def run_query(
    question: str,
    period: Optional[str] = None,
    company_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Punto de entrada único del backend.

    1) Crea el GlobalState y ejecuta el Router (tu grafo de agentes).
    2) Recibe el 'result' normal (igual que antes).
    3) Aplica la Base de Conocimiento (YAML) para activar reglas por agente
       según:
         - métricas (KPIs)
         - texto de la pregunta del usuario
    4) Clasifica el modo de datos ("db" vs "advisory").
    5) Adjunta las reglas activadas en result["kb_rules"],
       el data_mode en result["_meta"] y el company_context en el estado.
    """
    # --- 1) Ejecución normal del grafo ---
    state = GlobalState()
    state.period_raw = period               # para que el Router pueda leer el YYYY-MM
    state.company_context = company_context or {}  # usado por av_gerente (y futuros agentes)

    router = Router()
    result = router.dispatch(
        {"payload": {"question": question, "period": period}},
        state,
    )

    # --- 2) Construcción de métricas ---
    metrics_global = _build_metrics_global(result)
    metrics_cxc = _build_metrics_cxc(result)
    metrics_cxp = _build_metrics_cxp(result)

    # --- 3) Clasificar modo de datos: "db" (con KPIs) vs "advisory" (sin datos duros) ---
    trace = result.get("trace") or []
    data_mode = _classify_data_mode(metrics_global, trace)

    # --- 4) Aplicación de reglas de la KB por agente ---
    kb_rules: Dict[str, Any] = {
        "av_gerente": get_applicable_rules(
            "av_gerente",
            metrics=metrics_global,
            text_query=question,
        ),
        "av_administrativo": get_applicable_rules(
            "av_administrativo",
            metrics=metrics_global,
            text_query=question,
        ),
        "aaav_cxc": get_applicable_rules(
            "aaav_cxc",
            metrics=metrics_cxc,
            text_query=question,
        ),
        "aaav_cxp": get_applicable_rules(
            "aaav_cxp",
            metrics=metrics_cxp,
            text_query=question,
        ),
        "av_finanzas": get_applicable_rules(
            "av_finanzas",
            metrics=metrics_global,
            text_query=question,
        ),
        "av_contador_financiero": get_applicable_rules(
            "av_contador_financiero",
            metrics=metrics_global,
            text_query=question,
        ),
        "aav_contador": get_applicable_rules(
            "aav_contador",
            metrics=metrics_global,
            text_query=question,
        ),
    }

    # --- 5) Adjuntar al resultado y devolver ---
    result["kb_rules"] = kb_rules

    meta = result.get("_meta") or {}
    meta["data_mode"] = data_mode
    result["_meta"] = meta

    return result

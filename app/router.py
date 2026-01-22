# app/router.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
from datetime import datetime
from calendar import monthrange
from zoneinfo import ZoneInfo

from .state import GlobalState
from .agents.registry import get_agent
from .dates.period_resolver import resolve_period
from app.intent.engine import decide_agents  # keywords + LLM + umbrales

TZ = ZoneInfo("America/Costa_Rica")


# -----------------------------
# Helpers de período
# -----------------------------
def _coerce_sidebar_period(period_str: Optional[str]) -> Optional[dict]:
    """
    Convierte 'YYYY-MM' (desde la UI) a override con start/end ISO (TZ CR).
    Si no viene, devuelve None (para que resuelva NLP o default).
    """
    if not period_str:
        return None
    s = period_str.strip()
    if len(s) == 7 and s[4] == "-":
        y, m = map(int, s.split("-"))
        start = datetime(y, m, 1, 0, 0, 0, tzinfo=TZ)
        last = monthrange(y, m)[1]
        end = datetime(y, m, last, 23, 59, 59, tzinfo=TZ)
        return {
            "text": s,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "granularity": "month",
            "source": "sidebar",
            "tz": str(TZ),
        }
    return None


def _dedup_preserving_order(names: List[str]) -> List[str]:
    seen, out = set(), []
    for n in names:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out


def _derive_metrics_from_trace(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Prioridad:
      1) aav_contable.data.kpi.{DSO,DPO,CCC}
      2) mirrors top-level de aaav_cxc/aaav_cxp (dso/dpo/ccc)
    """
    dso = dpo = ccc = cash = None

    # 1) Contable (preferido)
    for r in trace or []:
        if r.get("agent") == "aav_contable":
            kpi = ((r.get("data") or {}).get("kpi") or {})
            if dso is None and isinstance(kpi.get("DSO"), (int, float)):
                dso = float(kpi["DSO"])
            if dpo is None and isinstance(kpi.get("DPO"), (int, float)):
                dpo = float(kpi["DPO"])
            if ccc is None and isinstance(kpi.get("CCC"), (int, float)):
                ccc = float(kpi["CCC"])

    # 2) Mirrors de subagentes si faltó algo
    for r in trace or []:
        if dso is None and isinstance(r.get("dso"), (int, float)):
            dso = float(r["dso"])
        if dpo is None and isinstance(r.get("dpo"), (int, float)):
            dpo = float(r["dpo"])
        if ccc is None and isinstance(r.get("ccc"), (int, float)):
            ccc = float(r["ccc"])

    return {"dso": dso, "dpo": dpo, "ccc": ccc, "cash": cash}


def _truthy(x: Any) -> bool:
    return bool(x is True)


def _select_agents_with_meta(question: str, meta_in: Dict[str, Any]) -> Dict[str, Any]:
    """
    1) Decide con decide_agents(question)
    2) Si el graph ya determinó intent (cxc/cxp), NO lo sobreescribimos:
       - cxp=true y cxc=false  => prohibe aaav_cxc
       - cxc=true y cxp=false  => prohibe aaav_cxp
    3) Además, si hay señales fuertes específicas (flags), forzamos el agente correspondiente.
    """
    intent_pack = decide_agents(question) or {"selected": [], "reasons": {}}
    selected: List[str] = list(intent_pack.get("selected") or [])
    reasons: Dict[str, Any] = dict(intent_pack.get("reasons") or {})

    intent = (meta_in.get("intent") or {}) if isinstance(meta_in, dict) else {}
    if not isinstance(intent, dict):
        intent = {}

    # -------------------------
    # 2) Respeto estricto a cxc/cxp del graph (evita "mezclar" módulos)
    # -------------------------
    graph_cxc = _truthy(intent.get("cxc"))
    graph_cxp = _truthy(intent.get("cxp"))

    # Si el graph fue explícito SOLO CxP -> eliminamos aaav_cxc si venía por keywords/LLM
    if graph_cxp and not graph_cxc:
        selected = [a for a in selected if a != "aaav_cxc"]
        reasons["router_guard"] = (reasons.get("router_guard") or []) + ["graph_says_only_cxp_drop_cxc"]

    # Si el graph fue explícito SOLO CxC -> eliminamos aaav_cxp si venía por keywords/LLM
    if graph_cxc and not graph_cxp:
        selected = [a for a in selected if a != "aaav_cxp"]
        reasons["router_guard"] = (reasons.get("router_guard") or []) + ["graph_says_only_cxc_drop_cxp"]

    # -------------------------
    # 3) Señales fuertes por flags (forzar agente correcto)
    # -------------------------
    cxc_strong = any(
        _truthy(intent.get(k))
        for k in (
            "vencimientos_rango",
            "top_clientes_cxc",
            "vencen_hoy_cxc",
            "cxc_pago_parcial",
            "saldo_cliente_cxc",
        )
    )

    # ✅ FIX: incluir CXP-01 (cxp_abiertas_resumen) como señal fuerte
    cxp_strong = any(
        _truthy(intent.get(k))
        for k in (
            "cxp_abiertas_resumen",   # ✅ CXP-01
            "aging_cxp",              # CXP-02
            "top_proveedores_cxp",    # CXP-03
            "saldo_proveedor_cxp",    # CXP-05
        )
    )

    # También si el intent general dice cxc/cxp, eso cuenta como fuerte
    if graph_cxc:
        cxc_strong = True
    if graph_cxp:
        cxp_strong = True

    # Forzar agentes si corresponde, pero respetando el guard:
    # - si graph dice SOLO CxP, no insertes aaav_cxc aunque haya flags raros
    if cxc_strong and not (graph_cxp and not graph_cxc) and "aaav_cxc" not in selected:
        selected.insert(0, "aaav_cxc")
        reasons["aaav_cxc"] = (reasons.get("aaav_cxc") or []) + ["forced_by_graph_intent"]

    # - si graph dice SOLO CxC, no insertes aaav_cxp
    if cxp_strong and not (graph_cxc and not graph_cxp) and "aaav_cxp" not in selected:
        idx = 1 if (selected and selected[0] == "aaav_cxc") else 0
        selected.insert(idx, "aaav_cxp")
        reasons["aaav_cxp"] = (reasons.get("aaav_cxp") or []) + ["forced_by_graph_intent"]

    intent_pack["selected"] = _dedup_preserving_order(selected)
    intent_pack["reasons"] = reasons
    return intent_pack


# -----------------------------
# Router principal (único orquestador)
# -----------------------------
class Router:
    def __init__(self, default_agent: str = "av_gerente"):
        self.default_agent = default_agent  # no se usa para activar por defecto

    def dispatch(self, task: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {}) or {}
        question = payload.get("question", "") or ""

        # ✅ 0) Traer meta desde graph (CRÍTICO para aaav_cxc / aaav_cxp)
        meta_in: Dict[str, Any] = payload.get("_meta") or {}

        # ✅ 1) Resolver período híbrido:
        #    prioridad: period_override (graph) > sidebar (UI) > NLP/default
        period_override = payload.get("period_override")
        if not isinstance(period_override, dict) or not period_override:
            # fallback: si meta trae date_range, úsalo como override
            dr = (meta_in.get("date_range") or {})
            if isinstance(dr, dict) and dr.get("start") and dr.get("end"):
                period_override = dr

        sidebar_period_str = payload.get("period") or getattr(state, "period_raw", None)
        sidebar_override = _coerce_sidebar_period(sidebar_period_str)

        override = period_override or sidebar_override
        pr = resolve_period(question, override)  # devuelve datetimes (aware) TZ CR

        period = {
            "text": pr["text"],
            "start": pr["start"].isoformat(),
            "end": pr["end"].isoformat(),
            "granularity": pr["granularity"],
            "source": pr["source"],
            "tz": pr["tz"],
        }
        state.period = period  # queda disponible para todos los agentes

        # ✅ 2) Decisión de agentes (keywords + LLM) + guard por meta intent del graph
        intent_pack = _select_agents_with_meta(question, meta_in)
        agent_sequence: List[str] = _dedup_preserving_order(intent_pack.get("selected", []))

        # 🔗 Regla: si hay CxC o CxP, forzar Contable para consolidar KPIs
        if any(a in agent_sequence for a in ("aaav_cxc", "aaav_cxp")) and "aav_contable" not in agent_sequence:
            agent_sequence.append("aav_contable")

        # 3) Trace inicial (por qué se eligieron/no se eligieron)
        if not hasattr(state, "trace"):
            state.trace = []
        state.trace.append(
            {
                "intent_decision": intent_pack,
                "question": question,
                "period": period,
                "_meta_in": meta_in,
            }
        )

        # 4) Si no hay señales suficientes, NO ejecutar y explicar
        if not agent_sequence:
            return {
                "intent": {"informe": False, "cxc": False, "cxp": False, "reason": "no-signals"},
                "gerente": {
                    "executive_decision_bsc": {
                        "resumen_ejecutivo": "No se activaron agentes: la pregunta no aportó señales suficientes.",
                        "hallazgos": [],
                        "riesgos": [],
                        "recomendaciones": [
                            "Especifica si deseas CxC, CxP o consolidado contable.",
                            "Incluye al menos un KPI o proceso (p. ej., DSO, DPO, CCC, aging).",
                            "Añade un período (p. ej., 'agosto 2025' o 'esta semana').",
                        ],
                        "bsc": {
                            "finanzas": [],
                            "clientes": [],
                            "procesos_internos": [],
                            "aprendizaje_crecimiento": [],
                        },
                    }
                },
                "administrativo": {"hallazgos": [], "orders": []},
                "metrics": {"dso": None, "dpo": None, "ccc": None, "cash": None},
                "trace": state.trace,
                "_meta": {"period_resolved": period, "router_sequence": []},
            }

        # ✅ payload base para agentes: aquí viaja _meta, action, params
        base_agent_payload = {
            "question": question,
            "period_range": period,
            "_meta": meta_in,
        }
        # si el graph/UI fuerza action/params, no lo pierdas
        if payload.get("action"):
            base_agent_payload["action"] = payload.get("action")
        if payload.get("params"):
            base_agent_payload["params"] = payload.get("params")

        # 5) Ejecutar subagentes en orden (CxC/CxP primero; Contable después con insumos)
        trace: List[Dict[str, Any]] = []
        cxc_blob: Optional[Dict[str, Any]] = None
        cxp_blob: Optional[Dict[str, Any]] = None

        for agent_name in [a for a in agent_sequence if a != "aav_contable"]:
            agent = get_agent(agent_name)
            try:
                result = agent.handle({"payload": dict(base_agent_payload)}, state)
            except TypeError:
                result = agent.handle({"payload": {"period_range": period, "_meta": meta_in}}, state)

            result = result or {}
            result["agent"] = agent_name
            trace.append(result)

            # conservar blobs exitosos para el contable
            if agent_name == "aaav_cxc" and not result.get("error"):
                cxc_blob = result
            if agent_name == "aaav_cxp" and not result.get("error"):
                cxp_blob = result

        # 6) Ejecutar Contable si corresponde
        run_contable = ("aav_contable" in agent_sequence) or (cxc_blob is not None or cxp_blob is not None)
        if run_contable:
            contable = get_agent("aav_contable")
            cont_payload = {
                "payload": {
                    "period_range": period,
                    "cxc_data": cxc_blob,
                    "cxp_data": cxp_blob,
                    "_meta": meta_in,
                }
            }
            cont_res = contable.handle(cont_payload, state) or {}
            cont_res["agent"] = "aav_contable"
            trace.append(cont_res)

        # 7) Gerente al final (consolidación ejecutiva)
        gerente = get_agent("av_gerente")
        final_report = gerente.handle(
            {"payload": {"trace": trace, "question": question, "period": period, "_meta": meta_in}},
            state,
        ) or {}

        # 8) Normalización de salida para la UI (SIN recortar llaves)
        exec_pack = final_report.get("executive_decision_bsc")
        if not isinstance(exec_pack, dict):
            exec_pack = final_report if isinstance(final_report, dict) else {}

        exec_pack.setdefault("resumen_ejecutivo", "Consolidado generado.")
        exec_pack.setdefault("hallazgos", [])
        exec_pack.setdefault("riesgos", [])
        exec_pack.setdefault("recomendaciones", [])
        exec_pack.setdefault(
            "bsc",
            {"finanzas": [], "clientes": [], "procesos_internos": [], "aprendizaje_crecimiento": []},
        )

        executive = exec_pack
        derived_metrics = _derive_metrics_from_trace(trace)

        orders_src = executive.get("ordenes_prioritarias") or executive.get("orders") or []

        ui_result = {
            "intent": final_report.get("intent")
            or {
                "informe": True,
                "cxc": any(r.get("agent") == "aaav_cxc" and not r.get("error") for r in trace),
                "cxp": any(r.get("agent") == "aaav_cxp" and not r.get("error") for r in trace),
                "reason": "router-exhaustive",
            },
            "gerente": {"executive_decision_bsc": executive},
            "administrativo": {
                "hallazgos": [
                    {"id": f"H{i+1}", "msg": h, "severity": "info"}
                    for i, h in enumerate(executive.get("hallazgos") or [])
                ],
                "orders": orders_src,
            },
            "metrics": derived_metrics,
            "trace": state.trace + trace,
        }

        # 9) Metadatos útiles
        ui_result.setdefault("_meta", {})
        ui_result["_meta"]["router_sequence"] = agent_sequence + ["av_gerente"]
        ui_result["_meta"]["period_resolved"] = period
        return ui_result

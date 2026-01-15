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


# -----------------------------
# Router principal (único orquestador)
# -----------------------------
class Router:
    def __init__(self, default_agent: str = "av_gerente"):
        self.default_agent = default_agent  # no se usa para activar por defecto

    def dispatch(self, task: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {}) or {}
        question = payload.get("question", "") or ""

        # ✅ 0) Traer meta desde graph (CRÍTICO para aaav_cxc)
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

        # 2) Decisión exhaustiva de agentes (keywords + LLM, SIN defaults)
        intent_pack = decide_agents(question)  # {selected: [...], reasons: {...}}
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
                # ✅ para depurar: confirmar que meta viajó hasta aquí
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
        # (aaav_cxc lo necesita para auto-params start/end por date_range)
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

        # 6) Ejecutar Contable si corresponde (estaba en la secuencia o hay al menos un blob)
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

# app/agents/av_gerente/logic.py
from __future__ import annotations

from typing import Dict, Any, List
import json

from ..base import BaseAgent
from ...state import GlobalState
from ...lc_llm import get_chat_model
from ...tools.prompting import build_system_prompt
from ...tools.fuzzy import fuzzify_dso, fuzzify_dpo, fuzzify_ccc, liquidity_risk
from ...tools.causality import causal_hypotheses

from .prompts import SYSTEM_PROMPT_GERENTE_VIRTUAL
from .utils import to_jsonable, period_text_and_due
from .trace_extractors import summarize_trace, extract_context, extract_operational_totals

# ✅ un solo import desde report (sin duplicados)
from .report import (
    sum_aging_overdue,
    has_hard_data,
    build_fallback_report,
    post_process_report,
    build_executive_context,
)

from .orders import deterministic_orders, kb_orders_from_rules

# ✅ KB imports
from .kb import (
    build_kb_rules,
    associate_rules_with_kpis,
    inherit_rules_from_trace,
    rule_priority,
)


class Agent(BaseAgent):
    name = "av_gerente"
    role = "executive"

    MAX_TRACE_ITEMS: int = 30
    MAX_FIELD_CHARS: int = 2_000

    def _build_fuzzy_signals(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        dso, dpo, ccc, cash = (
            metrics.get("dso"),
            metrics.get("dpo"),
            metrics.get("ccc"),
            metrics.get("cash"),
        )
        out: Dict[str, Any] = {}
        if dso is not None:
            out["dso"] = to_jsonable(fuzzify_dso(dso))
        if dpo is not None:
            out["dpo"] = to_jsonable(fuzzify_dpo(dpo))
        if ccc is not None:
            out["ccc"] = to_jsonable(fuzzify_ccc(ccc))
        if cash is not None and ccc is not None:
            out["liquidity_risk"] = to_jsonable(liquidity_risk(cash, ccc))
        return out

    def handle(self, task: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {})
        question: str = payload.get("question", "")
        period_in: Any = payload.get("period", state.period)
        trace: List[Dict[str, Any]] = payload.get("trace", []) or []

        company_context: Dict[str, Any] = getattr(state, "company_context", {}) or {}

        # resumen humano + metrics consolidados del trace
        resumen, metrics = summarize_trace(trace, self.MAX_TRACE_ITEMS, self.MAX_FIELD_CHARS)

        # contexto determinista extraído del trace
        ctx = extract_context(trace)

        overdue_cxc_total = sum_aging_overdue(ctx.get("aging_cxc_overdue") or ctx.get("aging_cxc") or {})
        overdue_cxp_total = sum_aging_overdue(ctx.get("aging_cxp_overdue") or ctx.get("aging_cxp") or {})

        fuzzy_signals = self._build_fuzzy_signals(metrics)
        op_totals = extract_operational_totals(trace, ctx)

        has_data = has_hard_data(ctx, metrics)

        # causalidad determinista
        try:
            causal_traditional = causal_hypotheses(
                trace,
                ctx.get("aging_cxc_overdue") or ctx.get("aging_cxc") or {},
                ctx.get("aging_cxp_overdue") or ctx.get("aging_cxp") or {},
            )
        except TypeError:
            causal_traditional = causal_hypotheses(trace)

        # órdenes deterministas
        det_orders = deterministic_orders(ctx, period_in)

        # métricas para KB rules
        metrics_for_kb = {
            "dso": metrics.get("dso"),
            "dpo": metrics.get("dpo"),
            "ccc": metrics.get("ccc"),
            "monto_cxc_vencidas": overdue_cxc_total,
            "monto_cxp_vencidas": overdue_cxp_total,
        }
        metrics_for_kb = {k: v for k, v in metrics_for_kb.items() if v is not None}

        # KB rules directas
        kb_rules = build_kb_rules(
            agent_name=self.name,
            question=question,
            metrics_for_kb=metrics_for_kb,
            company_context=company_context,
            payload_kb_rules=payload.get("kb_rules"),
            state_kb_rules=getattr(state, "kb_rules", {}),
        )

        # ✅ heredar reglas de otros agentes si gerente no trae propias
        kb_rules_global = payload.get("kb_rules") or getattr(state, "kb_rules", {}) or {}
        if isinstance(kb_rules_global, dict) and not kb_rules:
            kb_rules = inherit_rules_from_trace(kb_rules_global, trace, exclude={self.name})
            kb_rules = sorted(kb_rules, key=rule_priority)

        kb_rules_by_metric = associate_rules_with_kpis(kb_rules, ctx)

        # órdenes desde KB
        kb_orders = kb_orders_from_rules(kb_rules, period_in)

        # LLM
        llm = get_chat_model()
        base_system_prompt = build_system_prompt(self.name)
        system_prompt = base_system_prompt + "\n\n" + SYSTEM_PROMPT_GERENTE_VIRTUAL

        period_text, _ = period_text_and_due(period_in)

        if has_data:
            guardrails = (
                "REGLAS ESTRICTAS (MODO CON DATOS / BD):\n"
                "1) DATOS: Usa SOLO 'context.kpis', 'context.balances' y 'context.aging_*'. Si falta un dato, 'N/D'.\n"
                "2) Fuzzy: cualitativo; NO convertir a números.\n"
                "3) Prohibidas comparaciones intermensuales sin prev_kpis.\n"
                "4) CCC = DSO − DPO. Riesgo CxP viene de vencido, no de DPO.\n"
                "5) Aging: vencido = days_overdue>0.\n"
                "6) No inventes inventario/DIO.\n"
                "7) Usa kb_rules_by_metric primero; cita id de regla cuando aplique.\n"
                "8) Devuelve SOLO JSON válido.\n"
            )
        else:
            guardrails = (
                "REGLAS ESTRICTAS (MODO CONSULTIVO SIN BD):\n"
                "1) NO inventes KPIs numéricos.\n"
                "2) Basa análisis en pregunta + company_context + kb_rules.\n"
                "3) BSC.finanzas con DSO/DPO/CCC como N/D si no hay datos.\n"
                "4) Devuelve SOLO JSON válido.\n"
            )

        user_prompt = (
            f"{guardrails}\n"
            f"Periodo: {period_text}\n"
            f"Pregunta: {question}\n\n"
            f"== CONTEXTO NUMÉRICO ==\n"
            f"KPIs: {ctx.get('kpis')}\n"
            f"Aging CxC OVERDUE: {ctx.get('aging_cxc_overdue')}\n"
            f"Aging CxC CURRENT: {ctx.get('aging_cxc_current')}\n"
            f"Aging CxP OVERDUE: {ctx.get('aging_cxp_overdue')}\n"
            f"Aging CxP CURRENT: {ctx.get('aging_cxp_current')}\n"
            f"CxC overdue_total: {overdue_cxc_total}\n"
            f"CxP overdue_total: {overdue_cxp_total}\n"
            f"Balances: {ctx.get('balances')}\n\n"
            f"== FUZZY (cualitativo) ==\n"
            f"{json.dumps(fuzzy_signals, ensure_ascii=False, indent=2)}\n\n"
            f"== CAUSALIDAD DETERMINISTA ==\n"
            f"{json.dumps(causal_traditional, ensure_ascii=False, indent=2)}\n\n"
            f"== COMPANY CONTEXT ==\n"
            f"{json.dumps(company_context, ensure_ascii=False, indent=2)}\n\n"
            f"== KB RULES ==\n"
            f"{json.dumps(kb_rules, ensure_ascii=False, indent=2)}\n\n"
            f"== KB RULES BY METRIC ==\n"
            f"{json.dumps(kb_rules_by_metric, ensure_ascii=False, indent=2)}\n\n"
            f"Resumen de subagentes:\n{resumen}\n\n"
            "Devuelve EXACTAMENTE este JSON (con comillas dobles):\n"
            "{\n"
            "  \"resumen_ejecutivo\": \"\",\n"
            "  \"hallazgos\": [\"\"],\n"
            "  \"riesgos\": [\"\"],\n"
            "  \"recomendaciones\": [\"\"],\n"
            "  \"bsc\": {\n"
            "    \"finanzas\": [\"\"],\n"
            "    \"clientes\": [\"\"],\n"
            "    \"procesos_internos\": [\"\"],\n"
            "    \"aprendizaje_crecimiento\": [\"\"]\n"
            "  },\n"
            "  \"causalidad\": {\n"
            "    \"hipotesis\": [\"\"],\n"
            "    \"enlaces\": [ {\"causa\": \"\", \"efecto\": \"\", \"evidencia\": \"\", \"confianza\": \"alta\"} ]\n"
            "  },\n"
            "  \"ordenes_prioritarias\": [ {\"title\": \"\", \"owner\": \"\", \"kpi\": \"\", \"due\": \"\", \"impacto\": \"\"} ]\n"
            "}\n"
        )

        from .llm_io import llm_json
        report_json = llm_json(llm, system_prompt, user_prompt)

        # ✅ si el LLM falló: fallback + órdenes + contexto ejecutivo
        if not isinstance(report_json, dict):
            fallback = build_fallback_report(ctx, op_totals, fuzzy_signals, causal_traditional, [])
            fallback["ordenes_prioritarias"] = (kb_orders or []) + (det_orders or [])
            fallback["applied_rules"] = kb_rules

            # alias por compatibilidad
            fallback["orders"] = fallback["ordenes_prioritarias"]

            # ✅ NUEVO: contexto técnico NO-debug
            fallback["executive_context"] = build_executive_context(
                trace=trace,
                ctx=ctx,
                metrics=metrics,
                kb_rules=kb_rules,
                period_in=period_in,
                has_data=has_data,
            )

            return {
                "executive_decision_bsc": fallback,
                "question": question,
                "period": period_in,
                "trace": trace,
                "metrics": metrics,
                "fuzzy_signals": fuzzy_signals,
                "causal_hypotheses": causal_traditional,
                "causal_hypotheses_llm": [],
                "_meta": {"structured": True, "llm_ok": False, "has_data": has_data},
            }

        # ✅ Post-process determinista + merge KB/deterministas
        final_report = post_process_report(
            report_json,
            ctx,
            op_totals,
            det_orders,
            causal_traditional,
            kb_orders,
            causal_llm=[],  # ✅ no reciclar hipotesis del mismo reporte como "llm"
        )
        final_report["applied_rules"] = kb_rules

        # alias por compatibilidad (si algún adapter lee `orders`)
        if "orders" not in final_report:
            final_report["orders"] = final_report.get("ordenes_prioritarias", [])

        # ✅ NUEVO: contexto técnico NO-debug
        final_report["executive_context"] = build_executive_context(
            trace=trace,
            ctx=ctx,
            metrics=metrics,
            kb_rules=kb_rules,
            period_in=period_in,
            has_data=has_data,
        )


        return {
            "executive_decision_bsc": final_report,
            "question": question,
            "period": period_in,
            "trace": trace,
            "metrics": metrics,
            "fuzzy_signals": fuzzy_signals,
            "causal_hypotheses": causal_traditional,
            "causal_hypotheses_llm": final_report.get("causalidad", {}).get("hipotesis", []),
            "_meta": {"structured": True, "llm_ok": True, "has_data": has_data},
        }

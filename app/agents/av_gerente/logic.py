# app/agents/av_gerente/logic.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
import re
import json
from datetime import datetime
import pandas as pd
from dateutil import parser as dateparser

from ..base import BaseAgent
from ...state import GlobalState
from ...lc_llm import get_chat_model
from ...tools.prompting import build_system_prompt
from ...tools.fuzzy import fuzzify_dso, fuzzify_dpo, fuzzify_ccc, liquidity_risk
from ...tools.causality import causal_hypotheses
from ...utils.knowledge_base import get_applicable_rules  # KB
from ...tools.formatters import format_currency, format_days



# =========================
# System Prompt Gerente Virtual
# =========================
SYSTEM_PROMPT_GERENTE_VIRTUAL = """
Eres el Gerente Virtual de una pequeña o mediana empresa (pyme) en Costa Rica.

Tu misión es dar recomendaciones accionables y realistas, combinando:
1) Datos cuantitativos (KPIs como DSO, DPO, CCC, montos de CxC y CxP, balances).
2) Señales cualitativas (fuzzy_signals, causalidad, resumen de subagentes).
3) Contexto de envejecimiento de saldos (aging de CxC y CxP).
4) Situación general descrita en la pregunta del usuario.
5) Contexto de la empresa (tamaño, sector, zona, nivel de formalidad, años operando), cuando esté disponible.
6) Reglas de una base de conocimiento (kb_rules) que ya incluyen buenas prácticas y criterios estándar.

Principios:
- No inventes números: usa SOLO los datos entregados en el contexto.
- Si no hay un dato, trátalo como “N/D” y explica que hace falta información.
- Adapta tus recomendaciones al contexto típico de una pyme/microempresa:
  - Herramientas simples (Excel, calendarios, controles básicos).
  - Nada de ERP complejos, emisiones de bonos, fusiones, etc.

Tu salida debe:
- Ayudar al dueño/gerente a entender en lenguaje sencillo qué está pasando con su liquidez.
- Conectar explícitamente los KPIs con las acciones (ej.: DSO alto → problemas de cobranza; DPO bajo → se paga muy rápido a proveedores, etc.).
- Proponer acciones en horizontes de tiempo razonables (30, 60, 90 días).
- Mantener un enfoque tipo Cuadro de Mando Integral (BSC):
  Finanzas, Clientes, Procesos internos, Aprendizaje y crecimiento.

Uso de contexto de empresa y base de conocimiento:
- Si recibes un bloque JSON llamado company_context, úsalo para ajustar el nivel de sofisticación:
  - micro/pequeña → soluciones sencillas, baja carga administrativa.
  - sectores distintos → ejemplos y énfasis adaptados (comercio, servicios, etc.).
- Si recibes un bloque JSON llamado kb_rules con reglas activadas de una base de conocimiento:
  - Usa esas reglas como guía de buenas prácticas y recomendaciones estándar.
  - No inventes reglas nuevas ni contradigas lo que dice la base de conocimiento.
  - Puedes referenciar las reglas por id (ej. R_CXC_005, R_FIN_001) cuando ayude a explicar el criterio.
  - Cuando apliques una recomendación que coincide claramente con una regla, puedes mencionarla entre paréntesis, por ejemplo: "(según R_CXC_005)".

Importante:
- Si los KPIs son razonables, destaca las fortalezas y sugiere mantener disciplina.
- Si los KPIs son críticos (ej. CCC muy positivo, mucha CxC vencida, caja baja), prioriza liquidez y gestión de riesgo.
- No hagas comparaciones intermensuales (“mejor/peor que el mes pasado”) a menos que el contexto las traiga explícitamente.
"""


class Agent(BaseAgent):
    """
    AV_Gerente — Enfoque BSC (Kaplan & Norton), causalidad y recomendaciones.
    - NUNCA inventa KPIs: usa solo lo que llegue en trace (CxC/CxP/Contable).
    - Genera órdenes con due date consistente con el período resuelto.
    """

    name = "av_gerente"
    role = "executive"

    MAX_TRACE_ITEMS: int = 30
    MAX_FIELD_CHARS: int = 2_000

    # -------------------------
    # Helpers generales
    # -------------------------
    def _to_jsonable(self, obj: Any) -> Any:
        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, dict):
            return {str(k): self._to_jsonable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [self._to_jsonable(v) for v in obj]
        # Objetos tipo fuzzy con atributos low/mid/high → dict
        if all(hasattr(obj, a) for a in ("low", "mid", "high")):
            try:
                return {
                    "low": float(getattr(obj, "low")),
                    "mid": float(getattr(obj, "mid")),
                    "high": float(getattr(obj, "high")),
                }
            except Exception:
                pass
        for attr in ("to_dict", "as_dict", "model_dump", "dict"):
            if hasattr(obj, attr):
                try:
                    v = getattr(obj, attr)()
                    return self._to_jsonable(v)
                except Exception:
                    pass
        try:
            return {str(k): self._to_jsonable(v) for k, v in obj.items()}  # type: ignore
        except Exception:
            pass
        try:
            return [self._to_jsonable(v) for v in obj]  # type: ignore
        except Exception:
            pass
        return str(obj)

    def _coerce_float(self, value: Any) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None

    def _truncate(self, s: str, max_len: int) -> str:
        if s is None:
            return ""
        if len(s) <= max_len:
            return s
        return s[: max_len - 1] + "…"

    def _sanitize_text(self, s: str) -> str:
        if not isinstance(s, str):
            return s
        s = re.sub(r"(?is)<\s*think\s*>.*?</\s*think\s*>", "", s)
        s = re.sub(r"(?is)```(?:json)?(.*?)```", r"\1", s)
        s = re.sub(r"(?is)^(thought|thinking|reasoning|chain\s*of\s*thought).*?(\n\n|$)", "", s)
        return s.strip()

    # -------------------------
    # Período helpers
    # -------------------------
    def _period_text_and_due(self, period_in: Any) -> tuple[str, str]:
        """
        Devuelve (period_text, due_yyyy_mm_30)
        - Si `period_in` es dict del router → usa 'text' si existe;
          si no, deriva YYYY-MM de 'start'.
        - Si es str (YYYY-MM) → úsalo directo.
        """
        period_text = ""
        if isinstance(period_in, dict):
            pt = str(period_in.get("text") or "").strip()
            if pt:
                period_text = pt
            else:
                try:
                    start = dateparser.isoparse(period_in["start"])
                    period_text = f"{start.year:04d}-{start.month:02d}"
                except Exception:
                    period_text = ""
        elif isinstance(period_in, str):
            period_text = period_in.strip()

        due = "XXXX-XX-30"

        def _yyyy_mm_from_any(p: Any) -> Optional[str]:
            if isinstance(p, str) and len(p) >= 7 and p[4] == "-":
                return p[:7]
            if isinstance(p, dict):
                for key in ("start", "end"):
                    try:
                        dt = dateparser.isoparse(p[key])
                        return f"{dt.year:04d}-{dt.month:02d}"
                    except Exception:
                        pass
            return None

        ym = _yyyy_mm_from_any(period_text) or _yyyy_mm_from_any(period_in)
        if ym:
            due = f"{ym}-30"
        return period_text or (ym or ""), due

    # -------------------------
    # Extracción de datos del trace
    # -------------------------
    def _summarize_trace(self, trace: List[Dict[str, Any]]) -> Tuple[str, Dict[str, Any]]:
        if not trace:
            return "(sin resultados de subagentes)", {
                "dso": None,
                "dpo": None,
                "ccc": None,
                "cash": None,
            }
        trimmed = trace[: self.MAX_TRACE_ITEMS]
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
                summary = "; ".join(map(str, summary_candidates)) or str(
                    {k: res[k] for k in list(res)[:6]}
                )
            lines.append(f"{agent_name}: {self._truncate(summary, self.MAX_FIELD_CHARS)}")
            if dso is None and "dso" in res:
                dso = self._coerce_float(res.get("dso"))
            if dpo is None and "dpo" in res:
                dpo = self._coerce_float(res.get("dpo"))
            if ccc is None and "ccc" in res:
                ccc = self._coerce_float(res.get("ccc"))
            if cash is None and "cash" in res:
                cash = self._coerce_float(res.get("cash"))
        return "\n".join(lines), {"dso": dso, "dpo": dpo, "ccc": ccc, "cash": cash}

    def _extract_aging(self, trace: List[Dict[str, Any]], agent_name: str) -> Dict[str, Any]:
        for res in trace or []:
            if res.get("agent") == agent_name:
                data = res.get("data") or {}
                aging = data.get("aging")
                if isinstance(aging, dict):
                    return aging
        return {}

    def _extract_context(self, trace: List[Dict[str, Any]]) -> Dict[str, Any]:
        ctx: Dict[str, Any] = {
            "kpis": {"DSO": None, "DPO": None, "DIO": None, "CCC": None},
            "aging_cxc": {},
            "aging_cxp": {},
            "balances": {},
        }
        ctx["aging_cxc"] = self._extract_aging(trace, "aaav_cxc")
        ctx["aging_cxp"] = self._extract_aging(trace, "aaav_cxp")
        for res in trace or []:
            data = res.get("data") or {}
            kpi = data.get("kpi") or {}
            if isinstance(kpi, dict):
                for k in ("DSO", "DPO", "DIO", "CCC"):
                    if ctx["kpis"].get(k) is None and k in kpi:
                        ctx["kpis"][k] = self._coerce_float(kpi.get(k))
            bal = data.get("balances") or {}
            if isinstance(bal, dict) and not ctx["balances"]:
                ctx["balances"] = {str(k): self._coerce_float(v) for k, v in bal.items()}
        return ctx
    
    def _extract_operational_totals(
        self,
        trace: List[Dict[str, Any]],
        ctx: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Saca totales operativos que nos interesan para el resumen:
          - CxC: total_por_cobrar, open_invoices
          - CxP: total_por_pagar, open_invoices
          - NWC_proxy: desde balances contables
        """
        out = {
            "ar_outstanding": None,       # CxC total
            "ar_open_invoices": None,     # # facturas CxC
            "ap_outstanding": None,       # CxP total
            "ap_open_invoices": None,     # # facturas CxP
            "nwc_proxy": None,            # capital de trabajo proxy
        }

        # NWC desde balances contables (aav_contable)
        balances = ctx.get("balances") or {}
        if isinstance(balances, dict):
            out["nwc_proxy"] = self._coerce_float(balances.get("NWC_proxy"))

        for res in trace or []:
            agent = res.get("agent")
            data = res.get("data") or {}
            if agent == "aaav_cxc":
                if out["ar_outstanding"] is None:
                    out["ar_outstanding"] = self._coerce_float(data.get("total_por_cobrar"))
                if out["ar_open_invoices"] is None:
                    oi = data.get("open_invoices")
                    try:
                        out["ar_open_invoices"] = int(oi) if oi is not None else None
                    except Exception:
                        pass
            elif agent == "aaav_cxp":
                if out["ap_outstanding"] is None:
                    out["ap_outstanding"] = self._coerce_float(data.get("total_por_pagar"))
                if out["ap_open_invoices"] is None:
                    oi = data.get("open_invoices")
                    try:
                        out["ap_open_invoices"] = int(oi) if oi is not None else None
                    except Exception:
                        pass

        return out


    def _build_fuzzy_signals(self, metrics: Dict[str, Optional[float]]) -> Dict[str, Any]:
        dso, dpo, ccc, cash = (
            metrics.get("dso"),
            metrics.get("dpo"),
            metrics.get("ccc"),
            metrics.get("cash"),
        )
        out: Dict[str, Any] = {}
        if dso is not None:
            out["dso"] = self._to_jsonable(fuzzify_dso(dso))
        if dpo is not None:
            out["dpo"] = self._to_jsonable(fuzzify_dpo(dpo))
        if ccc is not None:
            out["ccc"] = self._to_jsonable(fuzzify_ccc(ccc))
        if cash is not None and ccc is not None:
            out["liquidity_risk"] = self._to_jsonable(liquidity_risk(cash, ccc))
        return out

    # -------------------------
    # Detectar si hay datos duros (modo DB) o es consultivo
    # -------------------------
    def _has_hard_data(self, ctx: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
        """
        Devuelve True si hay KPIs numéricos / aging / balances suficientes
        como para decir que estamos en 'modo con datos'.
        De lo contrario, estamos en un caso consultivo (sin BD).
        """
        # 1) KPIs en ctx
        kpis = (ctx.get("kpis") or {})
        for v in kpis.values():
            if isinstance(v, (int, float)):
                return True

        # 2) KPIs top-level (metrics dso/dpo/ccc/cash)
        for k in ("dso", "dpo", "ccc", "cash"):
            v = metrics.get(k)
            if isinstance(v, (int, float)):
                return True

        # 3) Aging con algo distinto de cero
        for bucket in ("aging_cxc", "aging_cxp"):
            aging = ctx.get(bucket) or {}
            if any(
                isinstance(aging.get(b), (int, float)) and aging.get(b) != 0
                for b in ("0_30", "31_60", "61_90", "90_plus")
            ):
                return True

        # 4) Balances con algún valor numérico
        balances = ctx.get("balances") or {}
        if any(isinstance(v, (int, float)) for v in balances.values()):
            return True

        return False

    # -------------------------
    # Causalidad y órdenes deterministas
    # -------------------------
    def _derive_deterministic_causality(self, ctx: Dict[str, Any]) -> List[str]:
        k = ctx.get("kpis", {})
        aging_cxp = ctx.get("aging_cxp") or {}
        hyps = []
        dso = k.get("DSO")
        dpo = k.get("DPO")
        ccc = k.get("CCC")
        if isinstance(dso, (int, float)) and dso > 45:
            hyps.append("DSO alto sugiere fricción en cobranza o crédito laxo (segmentos/condiciones).")
        if isinstance(dpo, (int, float)) and dpo < 40:
            hyps.append("DPO bajo indica negociación débil o pagos anticipados no alineados a caja.")
        if isinstance(ccc, (int, float)) and ccc > 20:
            hyps.append("CCC positivo alto indica presión de caja; probable inventario/AR alto vs AP.")
        share_31_60 = aging_cxp.get("31_60")
        if isinstance(share_31_60, (int, float)) and share_31_60 > 0:
            hyps.append("Proporción relevante de CxP en 31–60 días puede tensar pagos si no se calendariza.")
        return hyps

    def _deterministic_orders(self, ctx: Dict[str, Any], period_in: Any) -> List[Dict[str, Any]]:
        k = ctx.get("kpis", {})
        bal = ctx.get("balances", {})
        dso = k.get("DSO")
        dpo = k.get("DPO")
        ccc = k.get("CCC")
        ar = bal.get("AR_outstanding")
        ap = bal.get("AP_outstanding")
        ratio = (ar / ap) if isinstance(ar, (int, float)) and isinstance(ap, (int, float)) and ap > 0 else None

        _, due = self._period_text_and_due(period_in)

        orders: List[Dict[str, Any]] = []
        if isinstance(dso, (int, float)) and dso > 45:
            orders.append(
                {
                    "title": "Campaña dunning top-10 clientes",
                    "owner": "CxC",
                    "priority": "P1",
                    "kpi": "DSO",
                    "due": due,
                }
            )
        if isinstance(dpo, (int, float)) and dpo < 40:
            orders.append(
                {
                    "title": "Renegociar 3 proveedores clave",
                    "owner": "CxP",
                    "priority": "P2",
                    "kpi": "DPO",
                    "due": due,
                }
            )
        if isinstance(ccc, (int, float)) and ccc > 20:
            orders.append(
                {
                    "title": "Freeze gastos no esenciales (30d)",
                    "owner": "Administración",
                    "priority": "P1",
                    "kpi": "CCC",
                    "due": due,
                }
            )
        if isinstance(ratio, float) and ratio > 1.30:
            orders.append(
                {
                    "title": "Sync semanal CxC/CxP sobre flujos",
                    "owner": "Administración",
                    "priority": "P3",
                    "kpi": "CCC",
                    "due": due,
                }
            )
        return orders

    # -------------------------
    # KB helpers: prioridad y asociación con KPIs
    # -------------------------
    def _rule_priority(self, rule: Dict[str, Any]) -> int:
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

    def _associate_rules_with_kpis(
        self,
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
            assoc[key] = sorted(lst, key=self._rule_priority)

        return assoc

    # -------------------------
    # LLM JSON parser robusto
    # -------------------------
    def _llm_json(self, llm, system_prompt: str, user_prompt: str) -> Optional[Any]:
        def _clean(s: str) -> str:
            return self._sanitize_text(s or "")

        def _try_parse_any_json(s: str) -> Optional[Any]:
            s = s.strip()
            if s.startswith("{") or s.startswith("["):
                try:
                    return json.loads(s)
                except Exception:
                    pass
            starts = [m.start() for m in re.finditer(r"[\{\[]", s)]
            ends = [m.start() for m in re.finditer(r"[\}\]]", s)]
            for i in range(len(starts)):
                for j in range(len(ends) - 1, i - 1, -1):
                    if ends[j] <= starts[i]:
                        continue
                    frag = s[starts[i] : ends[j] + 1]
                    try:
                        return json.loads(frag)
                    except Exception:
                        continue
            return None

        try:
            resp = llm.invoke(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ]
            ).content
        except Exception:
            return None
        return _try_parse_any_json(_clean(resp))

    # -------------------------
    # Fallback y post-proceso
    # -------------------------
    def _fallback_report(
        self,
        ctx: Dict[str, Any],
        op_totals: Dict[str, Any],
        fuzzy_signals: Dict[str, Any],
        causal_traditional: List[str],
        causal_llm: List[str],
    ) -> Dict[str, Any]:
        k = ctx.get("kpis", {})
        dso = k.get("DSO")
        dpo = k.get("DPO")
        ccc = k.get("CCC")

        ar = op_totals.get("ar_outstanding")
        ap = op_totals.get("ap_outstanding")
        ar_open = op_totals.get("ar_open_invoices") or 0
        ap_open = op_totals.get("ap_open_invoices") or 0
        nwc = op_totals.get("nwc_proxy")

        dso_txt = format_days(dso)
        dpo_txt = format_days(dpo)
        ccc_txt = format_days(ccc)
        ar_txt = format_currency(ar)
        ap_txt = format_currency(ap)
        nwc_txt = format_currency(nwc)

        hallazgos, riesgos, reco = [], [], []

        if isinstance(dso, (int, float)) and dso > 45:
            hallazgos.append(f"DSO por encima del umbral (>45 días): {dso_txt}.")
            reco.append("Campaña dunning top-10 clientes (30-60-90).")
        if isinstance(dpo, (int, float)) and dpo < 40:
            hallazgos.append(f"DPO por debajo del umbral (<40 días): {dpo_txt}.")
            reco.append("Renegociar 2–3 proveedores para ampliar plazos.")
        if isinstance(ccc, (int, float)) and ccc > 20:
            hallazgos.append(f"CCC elevado (>20 días): {ccc_txt}.")
            reco.append("Calendario AR/AP semanal y control de gastos no esenciales (30d).")
        if not hallazgos:
            hallazgos.append("KPIs dentro de rangos razonables para el mes.")
            reco.append("Mantener disciplina de caja y seguimiento semanal de aging.")

        if isinstance(ccc, (int, float)) and ccc > 0:
            riesgos.append("Presión de caja por ciclo de conversión positivo.")
        if isinstance(dso, (int, float)) and isinstance(dpo, (int, float)) and (dso - dpo) > 10:
            riesgos.append("Desbalance entre cobros y pagos (DSO mucho mayor que DPO).")
        if not riesgos:
            riesgos.append("Riesgo moderado; continuar monitoreo semanal de AR/AP.")

        resumen = (
            f"KPIs: DSO={dso_txt}, DPO={dpo_txt}, CCC={ccc_txt}. "
            f"Las cuentas por cobrar abiertas suman {ar_txt} en {ar_open} facturas, "
            f"y las cuentas por pagar abiertas ascienden a {ap_txt} en {ap_open} facturas. "
        )
        if nwc is not None:
            resumen += f"El capital de trabajo operativo proxy (NWC) es de {nwc_txt}. "
        resumen += "Informe estructurado con acciones tácticas para liquidez."

        return {
            "resumen_ejecutivo": resumen,
            "hallazgos": hallazgos,
            "riesgos": riesgos,
            "recomendaciones": reco,
            "bsc": {
                "finanzas": [
                    f"DSO: {dso_txt}",
                    f"DPO: {dpo_txt}",
                    f"CCC: {ccc_txt}",
                    f"CxC abiertas: {ar_txt} en {ar_open} facturas",
                    f"CxP abiertas: {ap_txt} en {ap_open} facturas",
                    f"NWC proxy: {nwc_txt}" if nwc is not None else "NWC proxy: N/D",
                ],
                "clientes": ["Sin datos de NPS/Churn en este corte."],
                "procesos_internos": ["Revisión de aging AR/AP semanal."],
                "aprendizaje_crecimiento": ["Playbooks de cobranza y negociación de proveedores."],
            },
            "causalidad": {
                "hipotesis": list(dict.fromkeys((causal_traditional or []) + (causal_llm or [])))[:10],
                "enlaces": [],
            },
            "ordenes_prioritarias": [],
            "_insumos": {"fuzzy_signals": fuzzy_signals},
        }


    def _post_process_report(
        self,
        report: Dict[str, Any],
        ctx: Dict[str, Any],
        op_totals: Dict[str, Any],
        deterministic_orders: List[Dict[str, Any]],
        causal_traditional: List[str],
        causal_llm: List[str],
    ) -> Dict[str, Any]:
        if not isinstance(report, dict):
            return report

        # --- KPIs numéricos ---
        bsc = report.get("bsc") if isinstance(report.get("bsc"), dict) else {}
        k = ctx.get("kpis", {})
        dso = k.get("DSO")
        dpo = k.get("DPO")
        ccc = k.get("CCC")

        ar = op_totals.get("ar_outstanding")
        ap = op_totals.get("ap_outstanding")
        ar_open = op_totals.get("ar_open_invoices") or 0
        ap_open = op_totals.get("ap_open_invoices") or 0
        nwc = op_totals.get("nwc_proxy")

        dso_txt = format_days(dso)
        dpo_txt = format_days(dpo)
        ccc_txt = format_days(ccc)
        ar_txt = format_currency(ar)
        ap_txt = format_currency(ap)
        nwc_txt = format_currency(nwc)

        # FMT-02: BSC.finanzas SIEMPRE con DSO/DPO/CCC + CxC/CxP/NWC
        bsc["finanzas"] = [
            f"DSO: {dso_txt}",
            f"DPO: {dpo_txt}",
            f"CCC: {ccc_txt}",
            f"CxC abiertas: {ar_txt} en {ar_open} facturas",
            f"CxP abiertas: {ap_txt} en {ap_open} facturas",
            f"NWC proxy: {nwc_txt}" if nwc is not None else "NWC proxy: N/D",
        ]
        report["bsc"] = bsc

        # Enriquecer resumen_ejecutivo con estos KPIs clave
        resumen = report.get("resumen_ejecutivo")
        extra_line = (
            f" En este período, las cuentas por cobrar abiertas suman {ar_txt} en {ar_open} facturas, "
            f"las cuentas por pagar abiertas ascienden a {ap_txt} en {ap_open} facturas, "
        )
        if nwc is not None:
            extra_line += f"y el capital de trabajo operativo proxy (NWC) es de {nwc_txt}."
        else:
            extra_line += "y el capital de trabajo operativo proxy (NWC) se reporta como N/D."

        if isinstance(resumen, str) and resumen.strip():
            report["resumen_ejecutivo"] = resumen.strip() + " " + extra_line
        else:
            report["resumen_ejecutivo"] = extra_line.strip()

        # Inserta/une causalidad (igual que antes)
        cz = report.get("causalidad")
        if not isinstance(cz, dict):
            cz = {}
        cz_h = cz.get("hipotesis", [])
        merged_h = list(
            dict.fromkeys(
                (cz_h if isinstance(cz_h, list) else []) + (causal_traditional or []) + (causal_llm or [])
            )
        )
        cz["hipotesis"] = merged_h[:10]
        if not isinstance(cz.get("enlaces"), list):
            cz["enlaces"] = []
        report["causalidad"] = cz

        # Inserta/une órdenes
        curr_orders = report.get("ordenes_prioritarias")
        if not isinstance(curr_orders, list):
            curr_orders = []
        seen = set()
        merged_orders: List[Dict[str, Any]] = []
        for o in list(curr_orders) + deterministic_orders:
            title = (o or {}).get("title")
            if not title or title in seen:
                continue
            seen.add(title)
            merged_orders.append(o)
        report["ordenes_prioritarias"] = merged_orders

        # Sanitiza textos
        if isinstance(report.get("resumen_ejecutivo"), str):
            report["resumen_ejecutivo"] = self._sanitize_text(report["resumen_ejecutivo"])
        for sec in ("hallazgos", "riesgos", "recomendaciones"):
            if isinstance(report.get(sec), list):
                report[sec] = [self._sanitize_text(str(x)) for x in report[sec]]

        return report


    # -------------------------
    # Handler principal
    # -------------------------
    def handle(self, task: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {})
        question: str = payload.get("question", "")
        period_in: Any = payload.get("period", state.period)
        trace: List[Dict[str, Any]] = payload.get("trace", []) or []

        # 🔹 Contexto de empresa (llenado desde frontend → state.company_context)
        company_context: Dict[str, Any] = getattr(state, "company_context", {}) or {}

        # 1) Resumen y métricas top-level
        resumen, metrics = self._summarize_trace(trace)

        # 2) Contexto data-grounded + fuzzy (solo como señal cualitativa)
        ctx = self._extract_context(trace)
        fuzzy_signals = self._build_fuzzy_signals(metrics)
        # Totales operativos para CxC/CxP y NWC (para resumen y BSC)
        op_totals = self._extract_operational_totals(trace, ctx)

        # 🔹 Detectar si estamos en modo con datos (BD) o consultivo
        has_data = self._has_hard_data(ctx, metrics)

        # 3) Causalidad tradicional (reglas + aging)
        try:
            causal_traditional = causal_hypotheses(
                trace,
                ctx.get("aging_cxc") or {},
                ctx.get("aging_cxp") or {},
            )
        except TypeError:
            causal_traditional = causal_hypotheses(trace)

        # 4) Órdenes deterministas (no dependen del LLM)
        det_orders = self._deterministic_orders(ctx, period_in)

        # 5) Reglas de la base de conocimiento específicas para AV_GERENTE
        #    a) Métricas simples como input a la KB
        metrics_for_kb = {
            "dso": metrics.get("dso"),
            "dpo": metrics.get("dpo"),
            "ccc": metrics.get("ccc"),
        }
        metrics_for_kb = {k: v for k, v in metrics_for_kb.items() if v is not None}

        #    b) Reglas precalculadas desde graph_lc (si existen)
        kb_rules_global: Dict[str, Any] = (
            payload.get("kb_rules")
            or getattr(state, "kb_rules", {})
            or {}
        )
        precomputed_rules: List[Dict[str, Any]] = []
        if isinstance(kb_rules_global, dict):
            maybe = kb_rules_global.get(self.name)
            if isinstance(maybe, list):
                precomputed_rules = maybe

        #    c) Reglas calculadas "en caliente" para av_gerente
        rules_local = get_applicable_rules(
            self.name,
            metrics=metrics_for_kb,
            text_query=question,
        )

        #    d) Merge de reglas (precalculadas + locales) deduplicando por id
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

        #    e) Ordenar reglas por prioridad (riesgo > operativo > consultivo/gerencial > otras)
        kb_rules = sorted(kb_rules, key=self._rule_priority)

        #    f) Asociar reglas a KPIs/situaciones
        kb_rules_by_metric = self._associate_rules_with_kpis(kb_rules, ctx)

        # 6) LLM — instrucciones estrictas BSC + causalidad + contexto empresa + KB
        llm = get_chat_model()

        base_system_prompt = build_system_prompt(self.name)
        system_prompt = base_system_prompt + "\n\n" + SYSTEM_PROMPT_GERENTE_VIRTUAL

        # 🔹 Guardrails distintos según si hay datos o no
        if has_data:
            guardrails = (
                "REGLAS ESTRICTAS (MODO CON DATOS / BD):\n"
                "1) DATOS:\n"
                "   • Usa SOLO los datos explícitos de 'context.kpis', 'context.balances' y 'context.aging_cxc/cxp'.\n"
                "   • Si un dato no está presente, escribe 'N/D'.\n"
                "2) Fuzzy:\n"
                "   • 'fuzzy_signals' son cualitativos (low/mid/high). NO los uses como KPI ni los conviertas a valores numéricos.\n"
                "3) Comparaciones intermensuales:\n"
                "   • PROHIBIDO afirmar 'mejor/peor', 'al alza/a la baja', o comparar con 'el mes anterior' si NO existe 'context.prev_kpis'.\n"
                "4) DPO y CCC (semántica correcta):\n"
                "   • En este sistema: CCC = DSO − DPO. Un DPO alto, en aislamiento, TIENDE a mejorar (hacer más negativo) el CCC.\n"
                "   • El riesgo con CxP proviene de tener AP VENCIDO (aging_cxp > 0), NO del nivel de DPO por sí mismo.\n"
                "5) Aging:\n"
                "   • 'vencido' = facturas con days_overdue > 0. NO llames 'por vencer' a lo que ya está vencido.\n"
                "   • Usa explícitamente las sumas por buckets: 0_30, 31_60, 61_90, 90_plus.\n"
                "6) Inventarios/DIO:\n"
                "   • NO menciones inventario ni DIO ni los uses para causalidad si NO existe 'context.kpis.DIO' o datos de inventarios.\n"
                "7) Base de conocimiento:\n"
                "   • Usa primero 'kb_rules_by_metric' cuando comentes un KPI específico (DSO, DPO, CCC, CxC_vencidas, CxP_vencidas).\n"
                "   • Solo usa reglas de 'kb_rules' generales cuando no haya reglas específicas en 'kb_rules_by_metric' para ese tema.\n"
                "   • Prioriza reglas de 'riesgo' o 'alerta' sobre reglas 'operativas', y estas sobre reglas 'consultivas/gerenciales'.\n"
                "   • Cuando una recomendación derive claramente de una regla, menciona el id entre paréntesis (por ejemplo: 'según R_GE_LIQ_002').\n"
                "8) Salida:\n"
                "   • Devuelve ÚNICAMENTE JSON VÁLIDO con la estructura indicada. Sin explicaciones, sin bloques <think>.\n"
            )
        else:
            guardrails = (
                "REGLAS ESTRICTAS (MODO CONSULTIVO SIN BASE DE DATOS):\n"
                "1) DATOS:\n"
                "   • Asume que NO cuentas con KPIs numéricos confiables de la empresa.\n"
                "   • NO inventes DSO, DPO, CCC ni balances. Si el usuario NO te da un valor explícito, usa 'N/D'.\n"
                "   • Puedes referirte a rangos cualitativos (ej. 'ciclo de cobro lento', 'poca liquidez') pero sin números fabricados.\n"
                "2) FUENTE PRINCIPAL:\n"
                "   • Basa tu análisis en: (a) el texto de la pregunta, (b) el 'company_context' y (c) las reglas 'kb_rules' proporcionadas.\n"
                "   • Úsalas como buenas prácticas para pymes/microempresas, con ejemplos sencillos (Excel, calendarios, controles básicos).\n"
                "3) ENFOQUE:\n"
                "   • Tu rol es de CONSULTOR: explica riesgos, prioridades y rutas de acción aunque no existan datos históricos.\n"
                "   • Puedes sugerir qué datos debería empezar a registrar la empresa (CxC, CxP, flujo de caja, etc.).\n"
                "4) BSC:\n"
                "   • En 'bsc.finanzas' usa siempre KPIs como 'DSO: N/D', 'DPO: N/D', 'CCC: N/D' (el backend forzará estos valores si no hay datos).\n"
                "5) Base de conocimiento:\n"
                "   • Prioriza reglas de 'riesgo' o 'alerta' sobre las 'operativas' y 'consultivas'.\n"
                "   • Menciona el id de la regla cuando ayude al gerente a entender la lógica (ej. 'según R_GE_003').\n"
                "6) Salida:\n"
                "   • Devuelve ÚNICAMENTE JSON VÁLIDO con la estructura indicada. Sin explicaciones, sin bloques <think>.\n"
            )

        period_text, _ = self._period_text_and_due(period_in)

        user_prompt = (
            f"{guardrails}\n"
            f"Periodo: {period_text}\n"
            f"Pregunta: {question}\n\n"
            f"== CONTEXTO NUMÉRICO ==\n"
            f"KPIs: {ctx.get('kpis')}\n"
            f"Aging CxC: {ctx.get('aging_cxc')}\n"
            f"Aging CxP: {ctx.get('aging_cxp')}\n"
            f"Balances: {ctx.get('balances')}\n\n"
            f"== SEÑALES DIFUSAS (fuzzy_signals, solo cualitativas) ==\n"
            f"{json.dumps(fuzzy_signals, ensure_ascii=False, indent=2)}\n\n"
            f"== HIPÓTESIS CAUSALES DETERMINISTAS (causal_traditional) ==\n"
            f"{json.dumps(causal_traditional, ensure_ascii=False, indent=2)}\n\n"
            f"== CONTEXTO DE LA EMPRESA (company_context) ==\n"
            f"{json.dumps(company_context, ensure_ascii=False, indent=2)}\n\n"
            f"== REGLAS DE LA BASE DE CONOCIMIENTO ACTIVADAS PARA AV_GERENTE (kb_rules) ==\n"
            f"{json.dumps(kb_rules, ensure_ascii=False, indent=2)}\n\n"
            f"== REGLAS ASOCIADAS A KPIs Y SITUACIONES (kb_rules_by_metric) ==\n"
            f"{json.dumps(kb_rules_by_metric, ensure_ascii=False, indent=2)}\n\n"
            f"Resumen de subagentes:\n{resumen}\n\n"
            "Al redactar 'hallazgos', 'riesgos' y 'recomendaciones':\n"
            "  • Usa primero las reglas de 'kb_rules_by_metric' vinculadas al KPI del que estás hablando.\n"
            "  • Complementa con reglas generales de 'kb_rules' solo si aporta valor adicional.\n"
            "  • Siempre que una frase venga directamente de una regla, referencia su id ('según R_...').\n\n"
            "Devuelve EXACTAMENTE este JSON:\n"
            "{\n"
            "  'resumen_ejecutivo': str,\n"
            "  'hallazgos': [str],\n"
            "  'riesgos': [str],\n"
            "  'recomendaciones': [str],\n"
            "  'bsc': {\n"
            "    'finanzas': [str],\n"
            "    'clientes': [str],\n"
            "    'procesos_internos': [str],\n"
            "    'aprendizaje_crecimiento': [str]\n"
            "  },\n"
            "  'causalidad': {\n"
            "    'hipotesis': [str],\n"
            "    'enlaces': [ {'causa': str, 'efecto': str, 'evidencia': str, 'confianza': 'alta|media|baja'} ]\n"
            "  },\n"
            "  'ordenes_prioritarias': [ {'title': str, 'owner': str, 'kpi': str, 'due': str, 'impacto': str} ]\n"
            "}\n"
        )

        report_json = self._llm_json(llm, system_prompt, user_prompt)

        # Fallback si el LLM no devuelve JSON válido
        if not isinstance(report_json, dict):
            fallback = self._fallback_report(ctx, op_totals, fuzzy_signals, causal_traditional, [])
            fallback["ordenes_prioritarias"] = det_orders
            # También reflejamos qué reglas estaban activas en este caso
            fallback["applied_rules"] = kb_rules
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

        # Post-proceso
        final_report = self._post_process_report(
            report_json,
            ctx,
            op_totals,
            det_orders,
            causal_traditional,
            report_json.get("causalidad", {}).get("hipotesis", []),
        )

        # 🔹 Exponer explícitamente qué reglas se consideraron/aplicaron para este caso
        final_report["applied_rules"] = kb_rules

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

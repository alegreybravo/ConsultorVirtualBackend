# app/agents/av_gerente/report.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .utils import coerce_float, sanitize_text, safe_pct
from ...tools.formatters import format_currency, format_days


# =========================================================
# Aging helpers
# =========================================================
def sum_aging_overdue(aging: Dict[str, Any]) -> float:
    total = 0.0
    legacy_keys = ("0_30", "31_60", "61_90", "90_plus")
    new_keys = ("overdue_1_30", "overdue_31_60", "overdue_61_90", "overdue_90_plus")

    keys = legacy_keys if any(k in (aging or {}) for k in legacy_keys) else new_keys
    for k in keys:
        v = coerce_float((aging or {}).get(k))
        if v is not None:
            total += v
    return float(total)


def has_hard_data(ctx: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
    kpis = (ctx.get("kpis") or {})
    for v in kpis.values():
        if isinstance(v, (int, float)):
            return True

    for k in ("dso", "dpo", "ccc", "cash"):
        v = metrics.get(k)
        if isinstance(v, (int, float)):
            return True

    for bucket in (
        "aging_cxc_overdue",
        "aging_cxp_overdue",
        "aging_cxc_current",
        "aging_cxp_current",
        "aging_cxc",
        "aging_cxp",
    ):
        aging = ctx.get(bucket) or {}
        if any(isinstance(v, (int, float)) and v != 0 for v in (aging or {}).values()):
            return True

    balances = ctx.get("balances") or {}
    if any(isinstance(v, (int, float)) for v in balances.values()):
        return True

    return False


# =========================================================
# Fallback report (cuando falta data dura o falla LLM)
# =========================================================
def build_fallback_report(
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
    ar_open = op_totals.get("ar_open_invoices")
    ap_open = op_totals.get("ap_open_invoices")
    nwc = op_totals.get("nwc_proxy")

    ar_open_txt = str(ar_open) if ar_open is not None else "N/D"
    ap_open_txt = str(ap_open) if ap_open is not None else "N/D"

    dso_txt = format_days(dso)
    dpo_txt = format_days(dpo)
    ccc_txt = format_days(ccc)
    ar_txt = format_currency(ar)
    ap_txt = format_currency(ap)
    nwc_txt = format_currency(nwc)

    hallazgos: List[str] = []
    riesgos: List[str] = []
    reco: List[str] = []

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
        f"Las cuentas por cobrar abiertas suman {ar_txt} en {ar_open_txt} facturas, "
        f"y las cuentas por pagar abiertas ascienden a {ap_txt} en {ap_open_txt} facturas. "
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
                f"CxC abiertas: {ar_txt} en {ar_open_txt} facturas",
                f"CxP abiertas: {ap_txt} en {ap_open_txt} facturas",
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


# =========================================================
# Post-process principal del reporte gerente
# =========================================================
def post_process_report(
    report: Dict[str, Any],
    ctx: Dict[str, Any],
    op_totals: Dict[str, Any],
    deterministic_orders: List[Dict[str, Any]],
    causal_traditional: List[str],
    kb_orders: List[Dict[str, Any]],
    causal_llm: List[str],
) -> Dict[str, Any]:
    if not isinstance(report, dict):
        return report

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

    # BSC finanzas consistente
    bsc["finanzas"] = [
        f"DSO: {dso_txt}",
        f"DPO: {dpo_txt}",
        f"CCC: {ccc_txt}",
        f"CxC abiertas: {ar_txt} en {ar_open} facturas",
        f"CxP abiertas: {ap_txt} en {ap_open} facturas",
        f"NWC proxy: {nwc_txt}" if nwc is not None else "NWC proxy: N/D",
    ]
    report["bsc"] = bsc

    # Resumen ejecutivo + línea extra con totales
    resumen = report.get("resumen_ejecutivo")
    extra_line = (
        f" En este período, las cuentas por cobrar abiertas suman {ar_txt} en {ar_open} facturas, "
        f"las cuentas por pagar abiertas ascienden a {ap_txt} en {ap_open} facturas, "
    )
    extra_line += (
        f"y el capital de trabajo operativo proxy (NWC) es de {nwc_txt}."
        if nwc is not None
        else "y el capital de trabajo operativo proxy (NWC) se reporta como N/D."
    )

    if isinstance(resumen, str) and resumen.strip():
        report["resumen_ejecutivo"] = resumen.strip() + " " + extra_line
    else:
        report["resumen_ejecutivo"] = extra_line.strip()

    # Causalidad merge (existente + tradicional + llm)
    cz = report.get("causalidad")
    if not isinstance(cz, dict):
        cz = {}
    cz_h = cz.get("hipotesis", [])
    merged_h = list(
        dict.fromkeys(
            (cz_h if isinstance(cz_h, list) else [])
            + (causal_traditional or [])
            + (causal_llm or [])
        )
    )
    cz["hipotesis"] = merged_h[:10]
    if not isinstance(cz.get("enlaces"), list):
        cz["enlaces"] = []
    report["causalidad"] = cz

    # Órdenes merge (lo que venía -> kb -> determinísticas)
    curr_orders = report.get("ordenes_prioritarias")
    if not isinstance(curr_orders, list):
        curr_orders = []

    seen = set()
    merged_orders: List[Dict[str, Any]] = []
    for o in list(curr_orders) + (kb_orders or []) + (deterministic_orders or []):
        title = (o or {}).get("title")
        if not title:
            continue
        key = title.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        merged_orders.append(o)
    report["ordenes_prioritarias"] = merged_orders

    # Normalizar secciones
    for sec in ("hallazgos", "riesgos", "recomendaciones"):
        if not isinstance(report.get(sec), list):
            report[sec] = []

    if not isinstance(report.get("causalidad"), dict):
        report["causalidad"] = {"hipotesis": [], "enlaces": []}
    if not isinstance(report["causalidad"].get("enlaces"), list):
        report["causalidad"]["enlaces"] = []

    # Ratios vencido
    ar_total = coerce_float(op_totals.get("ar_outstanding"))
    ap_total = coerce_float(op_totals.get("ap_outstanding"))

    cxc_overdue = sum_aging_overdue(ctx.get("aging_cxc_overdue") or ctx.get("aging_cxc") or {})
    cxp_overdue = sum_aging_overdue(ctx.get("aging_cxp_overdue") or ctx.get("aging_cxp") or {})

    cxc_ratio = (cxc_overdue / ar_total) if isinstance(ar_total, (int, float)) and ar_total > 0 else None
    cxp_ratio = (cxp_overdue / ap_total) if isinstance(ap_total, (int, float)) and ap_total > 0 else None

    # Señales fuertes
    if cxc_ratio is not None and cxc_ratio >= 0.95:
        report["hallazgos"].insert(0, f"Liquidez: la cartera de CxC está prácticamente 100% vencida ({safe_pct(cxc_ratio)}).")
        report["riesgos"].insert(0, "Riesgo crítico de liquidez: los cobros esperados no están entrando en caja.")
        report["causalidad"]["enlaces"].append({
            "causa": "CxC vencida muy alta",
            "efecto": "Presión de liquidez / caja",
            "evidencia": f"CxC vencida={format_currency(cxc_overdue)} de {format_currency(ar_total)} ({safe_pct(cxc_ratio)})",
            "confianza": "alta",
        })

    if isinstance(dso, (int, float)) and dso > 60:
        report["hallazgos"].append(f"Eficiencia de cobro: DSO alto ({dso_txt}) sugiere cobranza lenta o crédito laxo.")
        report["causalidad"]["enlaces"].append({
            "causa": "DSO alto",
            "efecto": "Cobranza lenta / morosidad",
            "evidencia": f"DSO={dso_txt}",
            "confianza": "alta" if dso >= 120 else "media",
        })

    # Garantizar listas BSC
    report.setdefault("bsc", {})
    for dim in ("clientes", "procesos_internos", "aprendizaje_crecimiento"):
        if not isinstance(report["bsc"].get(dim), list):
            report["bsc"][dim] = []

    if cxc_ratio is not None and cxc_ratio >= 0.50:
        report["bsc"]["clientes"].insert(0, "Clientes: reforzar acuerdos de pago y revisar límites de crédito para reducir morosidad.")
        report["bsc"]["procesos_internos"].insert(0, "Procesos: rutina semanal de aging + dunning escalonado con responsables y fechas.")
        report["bsc"]["aprendizaje_crecimiento"].insert(0, "Aprendizaje: playbooks de cobranza y negociación; capacitación corta al equipo.")
        report["recomendaciones"].insert(0, "Aplicar plan de cobranza en 3 niveles (recordatorio → negociación → escalamiento) priorizando las facturas más antiguas.")

    if cxp_ratio is not None and cxp_ratio >= 0.50:
        report["hallazgos"].append(f"Proveedores: una porción relevante de CxP está vencida ({safe_pct(cxp_ratio)}).")
        report["riesgos"].append("Riesgo operativo: fricción con proveedores, penalidades o restricción de crédito.")
        report["causalidad"]["enlaces"].append({
            "causa": "CxP vencida alta",
            "efecto": "Riesgo de continuidad con proveedores",
            "evidencia": f"CxP vencida={format_currency(cxp_overdue)} de {format_currency(ap_total)} ({safe_pct(cxp_ratio)})",
            "confianza": "media",
        })
        report["bsc"]["procesos_internos"].append("Procesos: calendario de pagos por criticidad (esenciales primero) + renegociación de plazos.")

    # Recorte
    for sec in ("hallazgos", "riesgos", "recomendaciones"):
        report[sec] = report[sec][:8]
    report["causalidad"]["enlaces"] = report["causalidad"]["enlaces"][:8]

    # Sanitizar
    if isinstance(report.get("resumen_ejecutivo"), str):
        report["resumen_ejecutivo"] = sanitize_text(report["resumen_ejecutivo"])
    for sec in ("hallazgos", "riesgos", "recomendaciones"):
        if isinstance(report.get(sec), list):
            report[sec] = [sanitize_text(str(x)) for x in report[sec]]

    return report


# =========================================================
# Executive Context (NO-debug, técnico/digerible)
# =========================================================
def _find_calc_basis(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
    for item in trace or []:
        data = (item or {}).get("data") or {}
        cb = data.get("calc_basis")
        if isinstance(cb, dict) and cb:
            return cb
    return {}


def _find_aging(trace: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Soporta:
    - data.aging_overdue / data.aging_current (nuevo)
    - data.aging (legacy)
    """
    for item in trace or []:
        data = (item or {}).get("data") or {}

        ao = data.get("aging_overdue")
        ac = data.get("aging_current")

        legacy = data.get("aging")
        if (not isinstance(ao, dict) and not isinstance(ac, dict)) and isinstance(legacy, dict):
            ao = legacy
            ac = {}

        if isinstance(ao, dict) or isinstance(ac, dict):
            return {
                "aging_overdue": ao if isinstance(ao, dict) else {},
                "aging_current": ac if isinstance(ac, dict) else {},
                "open_invoices": data.get("open_invoices"),
                "total_overdue": data.get("total_overdue"),
                "total_current": data.get("total_current"),
                "total_por_cobrar": data.get("total_por_cobrar"),
            }

    return {}


def _dominant_bucket(aging_overdue: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(aging_overdue, dict) or not aging_overdue:
        return None
    best_k, best_v = None, None
    for k, v in aging_overdue.items():
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if best_v is None or fv > best_v:
            best_k, best_v = k, fv
    if best_k is None:
        return None
    return {"bucket": best_k, "amount": best_v}


def build_executive_context(
    trace: List[Dict[str, Any]],
    ctx: Dict[str, Any],
    metrics: Dict[str, Any],
    kb_rules: List[Dict[str, Any]],
    period_in: Any,
    has_data: bool,
) -> Dict[str, Any]:
    calc_basis = _find_calc_basis(trace)
    aging_pack = _find_aging(trace)

    # soportar DSO / dso
    dso_basis = {}
    if isinstance(calc_basis, dict):
        dso_basis = (calc_basis.get("DSO") or calc_basis.get("dso") or {})
        if not isinstance(dso_basis, dict):
            dso_basis = {}

    dso_method = dso_basis.get("method")
    dso_reason = dso_basis.get("reason")
    dso_window = dso_basis.get("window") or {}
    denom = dso_basis.get("denom")
    required_denom = dso_basis.get("required_denom")

    warnings: List[str] = []
    is_estimated = False

    confidence = "alta" if has_data else "media"

    if isinstance(dso_method, str) and "trailing" in dso_method.lower():
        is_estimated = True
        confidence = "media" if has_data else "baja"

    if isinstance(dso_reason, str) and dso_reason.strip():
        warnings.append(sanitize_text(dso_reason.strip()))

    if has_data and not dso_basis:
        confidence = "media"
        warnings.append("No se encontró calc_basis para explicar el DSO en este corte.")

    aging_overdue = (aging_pack.get("aging_overdue") or {}) if isinstance(aging_pack, dict) else {}
    dom = _dominant_bucket(aging_overdue)

    rule_ids: List[str] = []
    for r in kb_rules or []:
        rid = (r or {}).get("id")
        if isinstance(rid, str) and rid.strip():
            rule_ids.append(rid.strip())

    return {
        "data_quality": {
            "has_data": has_data,
            "confidence": confidence,
            "is_estimated": is_estimated,
            "warnings": warnings[:6],
        },
        "kpi_explain": {
            "dso": {
                "value": metrics.get("dso"),
                "method": dso_method,
                "basis_reason": dso_reason,
                "window": dso_window if isinstance(dso_window, dict) else {},
                "denom": denom,
                "required_denom": required_denom,
            },
            "dpo": {"value": metrics.get("dpo")},
            "ccc": {"value": metrics.get("ccc")},
        },
        "aging_summary": {
            "open_invoices": aging_pack.get("open_invoices") if isinstance(aging_pack, dict) else None,
            "total_overdue": aging_pack.get("total_overdue") if isinstance(aging_pack, dict) else None,
            "total_current": aging_pack.get("total_current") if isinstance(aging_pack, dict) else None,
            "total_por_cobrar": aging_pack.get("total_por_cobrar") if isinstance(aging_pack, dict) else None,
            "buckets_overdue": aging_overdue,
            "dominant_bucket": dom,
        },
        "rules_applied": rule_ids[:20],
    }

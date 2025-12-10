# app/api_adapter.py
from typing import Any, Dict

from .api_models import ChatResponse, PeriodInfo, KPIBlock, Hallazgo, Orden


def build_answer_text(result: Dict[str, Any]) -> str:
    """
    Toma el dict que devuelve run_query y extrae un texto legible.
    Usamos el resumen ejecutivo si existe.
    """
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}

    resumen = exec_pack.get("resumen_ejecutivo")
    if isinstance(resumen, str) and resumen.strip():
        return resumen.strip()

    return "El backend generó un informe, pero no se encontró un 'resumen_ejecutivo' para mostrar."


def build_frontend_payload(result: Dict[str, Any], include_raw: bool) -> ChatResponse:
    # ----- Período resuelto -----
    meta = (result.get("_meta") or {}).get("period_resolved") or {}
    period = None
    if meta:
        period = PeriodInfo(
            text=meta.get("text", ""),
            start=meta.get("start", ""),
            end=meta.get("end", ""),
            granularity=meta.get("granularity", ""),
            tz=meta.get("tz", ""),
        )

    # ----- KPIs (metrics + kpis del gerente) -----
    gerente = result.get("gerente") or {}
    exec_pack = (gerente.get("executive_decision_bsc") or {})
    metrics = (result.get("metrics") or {}) | (exec_pack.get("kpis") or {})

    kpis = KPIBlock(
        dso=metrics.get("dso") or metrics.get("DSO"),
        dpo=metrics.get("dpo") or metrics.get("DPO"),
        ccc=metrics.get("ccc") or metrics.get("CCC"),
    )

    # ----- Hallazgos y órdenes -----
    # Soporta tanto "administrativo" como "av_administrativo"
    admin = result.get("administrativo") or result.get("av_administrativo") or {}

    hallazgos = []
    for h in admin.get("hallazgos") or []:
        if isinstance(h, dict):
            hallazgos.append(Hallazgo(
                id=h.get("id"),
                msg=h.get("msg", ""),
                severity=h.get("severity"),
            ))

    ordenes = []
    for o in admin.get("orders") or []:
        if isinstance(o, dict):
            ordenes.append(Orden(
                title=o.get("title", ""),
                owner=o.get("owner"),
                kpi=o.get("kpi"),
                due=o.get("due"),
                priority=o.get("priority"),
                impacto=o.get("impacto"),
            ))

    # ----- Texto principal -----
    answer_text = build_answer_text(result)

    return ChatResponse(
        answer=answer_text,
        period=period,
        kpis=kpis,
        resumen_ejecutivo=exec_pack.get("resumen_ejecutivo"),
        hallazgos=hallazgos,
        ordenes=ordenes,
        raw=result if include_raw else None,
    )

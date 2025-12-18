# app/api_adapter.py
from typing import Any, Dict, Optional

from .api_models import ChatResponse, PeriodInfo, KPIBlock, Hallazgo, Orden


# =========================================================
# Helpers
# =========================================================
def _metric(metrics: Dict[str, Any], *keys: str) -> Optional[float]:
    """
    Devuelve el primer valor NO None para las claves dadas.
    IMPORTANTE:
    - NO usa `or`, para no perder valores válidos como 0.0
    """
    for k in keys:
        if k in metrics and metrics[k] is not None:
            try:
                return float(metrics[k])
            except (TypeError, ValueError):
                return None
    return None


def build_answer_text(result: Dict[str, Any]) -> str:
    """
    Toma el dict que devuelve run_query y extrae un texto legible.
    Se prioriza el resumen ejecutivo del gerente.
    """
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}

    resumen = exec_pack.get("resumen_ejecutivo")
    if isinstance(resumen, str) and resumen.strip():
        return resumen.strip()

    return (
        "El backend generó un informe estructurado, "
        "pero no se encontró un resumen ejecutivo para mostrar."
    )


# =========================================================
# Adapter principal hacia el frontend
# =========================================================
def build_frontend_payload(result: Dict[str, Any], include_raw: bool) -> ChatResponse:
    # -----------------------------------------------------
    # Período resuelto
    # -----------------------------------------------------
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

    # -----------------------------------------------------
    # KPIs
    # -----------------------------------------------------
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}

    # Mezcla métricas top-level + posibles KPIs del gerente
    metrics: Dict[str, Any] = {}
    metrics.update(result.get("metrics") or {})
    metrics.update(exec_pack.get("kpis") or {})

    kpis = KPIBlock(
        dso=_metric(metrics, "dso", "DSO"),
        dpo=_metric(metrics, "dpo", "DPO"),
        ccc=_metric(metrics, "ccc", "CCC"),
    )

    # -----------------------------------------------------
    # Hallazgos y órdenes (administrativo)
    # -----------------------------------------------------
    # Soporta tanto "administrativo" como "av_administrativo"
    admin = (
        result.get("administrativo")
        or result.get("av_administrativo")
        or {}
    )

    hallazgos = []
    for h in admin.get("hallazgos") or []:
        if isinstance(h, dict):
            hallazgos.append(
                Hallazgo(
                    id=h.get("id"),
                    msg=h.get("msg", ""),
                    severity=h.get("severity"),
                )
            )

    ordenes = []
    for o in admin.get("orders") or []:
        if isinstance(o, dict):
            ordenes.append(
                Orden(
                    title=o.get("title", ""),
                    owner=o.get("owner"),
                    kpi=o.get("kpi"),
                    due=o.get("due"),
                    priority=o.get("priority"),
                    impacto=o.get("impacto"),
                )
            )

    # -----------------------------------------------------
    # Texto principal
    # -----------------------------------------------------
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

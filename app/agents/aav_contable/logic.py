# app/agents/aav_contable/logic.py
from __future__ import annotations

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import pandas as pd
from dateutil import parser as dateparser

from ..base import BaseAgent
from ...state import GlobalState

# (Opcional) Si agregas esquemas más adelante
# from ...tools.schema_validate import validate_with
# SCHEMA = "app/schemas/aav_contable_schema.json"


@dataclass
class PeriodResolved:
    text: str
    start: pd.Timestamp | None
    end: pd.Timestamp | None


def _resolve_period(payload: Dict[str, Any], state: GlobalState) -> PeriodResolved:
    """
    Acepta:
      - payload["period_range"] dict (preferido): {text, start, end, ...}
      - payload["period"] string 'YYYY-MM' (fallback informativo)
      - state.period (dict del router)
    Devuelve texto + timestamps (si disponibles).
    """
    pr = payload.get("period_range") or getattr(state, "period", None)
    if isinstance(pr, dict) and pr.get("start") and pr.get("end"):
        try:
            start = pd.Timestamp(dateparser.isoparse(pr["start"]))
            end   = pd.Timestamp(dateparser.isoparse(pr["end"]))
        except Exception:
            start = end = None
        return PeriodResolved(
            text=str(pr.get("text") or ""),
            start=start,
            end=end,
        )

    # Fallback: sólo texto si viene 'period' (YYYY-MM) o state.period como string
    p = payload.get("period") or getattr(state, "period_raw", None)
    if isinstance(p, str) and p:
        return PeriodResolved(text=p, start=None, end=None)

    # Último recurso: si state.period es dict sin ISO válidos, toma text
    if isinstance(getattr(state, "period", None), dict):
        d = getattr(state, "period")
        return PeriodResolved(text=str(d.get("text") or ""), start=None, end=None)

    return PeriodResolved(text="", start=None, end=None)


class Agent(BaseAgent):
    """Agente Contable (Consolidación)

    Integra salidas de CxC (aaav_cxc), CxP (aaav_cxp) y opcionalmente Inventarios (aaav_inv)
    para producir un *pack contable* con KPIs (DSO/DPO/DIO/CCC) y resúmenes útiles.

    Compatibilidad:
    - Soporta inputs en `task.payload` (cxc_data/cxp_data/inv_data) tanto en forma de
      *objeto `data`* como en *objeto completo de agente* (con llaves top-level `dso`/`dpo` y `data.kpi`).
    - Emite mirrors top-level: `dso`, `dpo`, `dio`, `ccc` para uso directo por `av_gerente`.
    - Devuelve `summary` y `data` estructurada apta para UI.
    """

    name = "aav_contable"
    role = "consolidation"

    # ==========================
    # Utilidades privadas
    # ==========================
    def _safe_float(self, v: Any) -> Optional[float]:
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _extract_period_text(self, blob: Dict[str, Any]) -> Optional[str]:
        # Intenta data.period (string) o period (string)
        data = blob.get("data") or {}
        for k in ("period",):
            if isinstance(data.get(k), str) and data.get(k):
                return data.get(k)
        if isinstance(blob.get("period"), str) and blob.get("period"):
            return blob.get("period")
        return None

    def _extract_kpi(self, blob: Dict[str, Any], key: str) -> Optional[float]:
        """Busca primero espejo top-level (ej. `dso`) y luego en `data.kpi[key]` (ej. `DSO`)."""
        # Mirror top-level
        if key.lower() in blob:
            f = self._safe_float(blob[key.lower()])
            if f is not None:
                return f
        # Dentro de data.kpi
        data = blob.get("data") or blob
        kpi = (data or {}).get("kpi") or {}
        if key in kpi:
            return self._safe_float(kpi[key])
        return None

    def _extract_totals(self, blob: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """
        Lee totales normalizados si existen:
          - CxC: total_por_cobrar, por_vencer/current
          - CxP: total_por_pagar, por_vencer/current
        Si no existen, intenta sumar aging (aunque aging ahora trae sólo vencido).
        """
        data = blob.get("data") or {}
        out: Dict[str, Optional[float]] = {
            "total": None,
            "por_vencer": None,
            "current": None,
            "vencido": None,
        }

        # Preferidos (normalizados)
        for k in ("total_por_cobrar", "total_por_pagar"):
            if k in data:
                out["total"] = self._safe_float(data.get(k))
                break
        for k in ("por_vencer", "current"):
            if k in data:
                val = self._safe_float(data.get(k))
                out["por_vencer"] = val
                out["current"] = val
                break

        # Vencido = suma buckets de aging (si existen)
        aging = data.get("aging") or data.get("aging_overdue") or {}

        if isinstance(aging, dict) and aging:
            try:
                vencido = sum(float(aging.get(k) or 0.0) for k in ("0_30", "31_60", "61_90", "90_plus"))
                out["vencido"] = float(vencido)
            except Exception:
                out["vencido"] = None

            # Si no teníamos total pero sí por_vencer y vencido → total = por_vencer + vencido
            if out["total"] is None and out["por_vencer"] is not None and out["vencido"] is not None:
                out["total"] = float(out["por_vencer"]) + float(out["vencido"])

        return out

    # ==========================
    # Manejo principal
    # ==========================
    def handle(self, task: Dict[str, Any], state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {}) or {}

        # Inputs: datos crudos (data) o objetos completos de agentes
        cxc_in = payload.get("cxc_data") or payload.get("cxc") or {}
        cxp_in = payload.get("cxp_data") or payload.get("cxp") or {}
        inv_in = payload.get("inv_data") or payload.get("inventories") or {}

        # Resolver período (dict o string) y sobreescribir con el más específico si llega desde subagentes
        pr = _resolve_period(payload, state)
        period_text = pr.text or ""  # UI usa texto; el router ya tiene start/end

        # Si los subagentes traen period texto, preferimos el de CxC, luego CxP
        cxc_period_txt = self._extract_period_text(cxc_in) if cxc_in else None
        cxp_period_txt = self._extract_period_text(cxp_in) if cxp_in else None
        period_text = cxc_period_txt or cxp_period_txt or period_text

        # Si no hay ninguno de los dos, devolvemos consolidado parcial en lo posible
        if not cxc_in and not cxp_in:
            return {"agent": self.name, "error": "Faltan datos de CxC y CxP para consolidar"}

        # KPIs CxC / CxP / Inventarios
        dso = self._extract_kpi(cxc_in, "DSO") if cxc_in else None
        dpo = self._extract_kpi(cxp_in, "DPO") if cxp_in else None
        dio = self._extract_kpi(inv_in, "DIO") if inv_in else None

        # CCC: si hay DIO, fórmula completa; si no, simplificada
        ccc = None
        try:
            if dso is not None and dpo is not None and dio is not None:
                ccc = float(dso) + float(dio) - float(dpo)
            elif dso is not None and dpo is not None:
                ccc = float(dso) - float(dpo)
        except Exception:
            ccc = None

        # Saldos totales (si disponibilidad en `data.total_*` o sumando aging + por_vencer)
        ar_totals = self._extract_totals(cxc_in) if cxc_in else {"total": None, "por_vencer": None, "current": None, "vencido": None}
        ap_totals = self._extract_totals(cxp_in) if cxp_in else {"total": None, "por_vencer": None, "current": None, "vencido": None}

        ar_total = ar_totals["total"]
        ap_total = ap_totals["total"]

        # Paquete contable consolidado
        pack = {
            "period": period_text,
            "kpi": {
                "DSO": dso,
                "DPO": dpo,
                "DIO": dio,
                "CCC": ccc,
            },
            "balances": {
                "AR_outstanding": ar_total,
                "AP_outstanding": ap_total,
                # Net Working Capital aproximado (si hay datos)
                "NWC_proxy": (ar_total - ap_total) if (ar_total is not None and ap_total is not None) else None,
            },
            # Estado de Resultados y Balance: placeholders si luego integras más fuentes
            "er": {},
            "esf": {},
            "checks": [
                "Base contable consolidada a partir de CxC/CxP",
                "KPIs consistentes con mirrors top-level cuando existen",
            ],
        }

        # (Opcional) validación de esquema
        # try:
        #     validate_with(SCHEMA, pack)
        # except Exception:
        #     pass

        # Resumen
        parts = []
        if dso is not None: parts.append(f"DSO={dso:.1f}d")
        if dpo is not None: parts.append(f"DPO={dpo:.1f}d")
        if ccc is not None: parts.append(f"CCC={ccc:.1f}d")
        if not parts: parts.append("sin KPIs")
        summary = "Pack contable consolidado (" + ", ".join(parts) + ")"

        # Salida con mirrors top-level + summary
        out: Dict[str, Any] = {
            "agent": self.name,
            "summary": summary,
            "data": pack,
            # Mirrors para consumo directo por av_gerente
            "dso": dso,
            "dpo": dpo,
            "ccc": ccc,
        }
        if dio is not None:
            out["dio"] = dio

        return out

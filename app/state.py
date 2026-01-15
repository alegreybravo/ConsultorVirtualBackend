# app/state.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CR_TZ = ZoneInfo("America/Costa_Rica")


def _now_cr() -> datetime:
    return datetime.now(CR_TZ)


def _default_period() -> Dict[str, Any]:
    """Mes actual en TZ CR como fallback determinista."""
    today = _now_cr()
    start = datetime(today.year, today.month, 1, 0, 0, 0, tzinfo=CR_TZ)

    if today.month == 12:
        next_month = datetime(today.year + 1, 1, 1, 0, 0, 0, tzinfo=CR_TZ)
    else:
        next_month = datetime(today.year, today.month + 1, 1, 0, 0, 0, tzinfo=CR_TZ)

    end = (next_month - timedelta(seconds=1))

    return {
        "text": "auto: mes actual",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": "month",
        "source": "default",
        "tz": "America/Costa_Rica",
    }


@dataclass
class GlobalState:
    """
    Estado global compartido por agentes.

    - period: dict unificado {text, start, end, granularity, source, tz}
    - period_raw: string crudo (e.g., 'YYYY-MM')
    - context: bolsa para valores temporales
    - company_context: información de la pyme (tamaño, sector, empleados, etc.)
    - kb_rules: reglas activadas de la base de conocimiento
    - trace: acumulador de resultados de subagentes
    - errors: errores capturados durante la ejecución
    """

    period: Dict[str, Any] = field(default_factory=_default_period)
    period_raw: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


    company_context: Dict[str, Any] = field(default_factory=dict)

   
    kb_rules: Dict[str, Any] = field(default_factory=dict)

    trace: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    # ---- Utilidades de período ----
    def period_start_dt(self) -> datetime:
        try:
            return datetime.fromisoformat(self.period["start"]).astimezone(CR_TZ)
        except Exception:
            return _now_cr().replace(hour=0, minute=0, second=0, microsecond=0)

    def period_end_dt(self) -> datetime:
        try:
            return datetime.fromisoformat(self.period["end"]).astimezone(CR_TZ)
        except Exception:
            return _now_cr().replace(hour=23, minute=59, second=59, microsecond=0)

    def period_text(self) -> str:
        return (self.period or {}).get("text", "")

    def period_tz(self) -> str:
        return (self.period or {}).get("tz", "America/Costa_Rica")

    # ---- Compatibilidad legacy ----
    def period_yyyymm(self) -> str:
        """
        Devuelve 'YYYY-MM' para agentes legacy que aún reciban un string.
        """
        dt = self.period_start_dt()
        return f"{dt.year:04d}-{dt.month:02d}"

    # ---- Gestión de traza ----
    def add_trace(self, item: Dict[str, Any]) -> None:
        self.trace.append(item)

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    # ---- Setter limpio de período ----
    def set_period(self, period_dict: Dict[str, Any]) -> None:
        """
        Establece un período unificado ya resuelto.
        """
        base = _default_period()
        base.update(period_dict or {})
        self.period = base

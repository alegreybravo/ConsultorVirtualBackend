# app/api_models.py
from typing import Optional, Any, Dict, List
from pydantic import BaseModel


class PeriodInfo(BaseModel):
    text: str = ""
    start: str = ""
    end: str = ""
    granularity: str = ""
    tz: str = ""


class KPIBlock(BaseModel):
    dso: Optional[float] = None
    dpo: Optional[float] = None
    ccc: Optional[float] = None


class Hallazgo(BaseModel):
    id: Optional[str] = None
    msg: str
    severity: Optional[str] = None
    # opcional: puedes agregar más campos si quieres, ej:
    # kpi: Optional[str] = None


class Orden(BaseModel):
    title: str
    owner: Optional[str] = None
    kpi: Optional[str] = None
    due: Optional[str] = None
    priority: Optional[str] = None
    impacto: Optional[str] = None


class ChatRequest(BaseModel):
    question: str
    period: Optional[str] = None

    # Campos de contexto futuro (por ahora no usados por run_query)
    company_name: Optional[str] = None
    company_size: Optional[str] = None
    sector: Optional[str] = None
    years_operating: Optional[int] = None
    employees: Optional[int] = None


class ChatResponse(BaseModel):
    # Texto listo para mostrar en Flutter
    answer: str

    # Info estructurada para dashboards
    period: Optional[PeriodInfo] = None
    kpis: Optional[KPIBlock] = None
    resumen_ejecutivo: Optional[str] = None

    hallazgos: List[Hallazgo] = []
    ordenes: List[Orden] = []

    # Opcional: payload crudo completo
    raw: Optional[Dict[str, Any]] = None

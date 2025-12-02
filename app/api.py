# app/api.py
from dotenv import load_dotenv
load_dotenv()

from typing import Any, Dict, Optional

from fastapi import FastAPI
from pydantic import BaseModel

# Importa tu grafo real
from app.graph_lc import run_query

app = FastAPI(
    title="API Agente Virtual",
    description="API para consultar el agente virtual",
    version="0.1.0",
)

# --------- Modelos de entrada/salida ---------
class ChatRequest(BaseModel):
    question: str
    period: Optional[str] = None   # opcional, por si quieres que Flutter lo mande

    # Campos pensados para futuro multiempresa / contexto,
    # por ahora NO se usan en el backend (run_query no los recibe).
    company_name: Optional[str] = None
    company_size: Optional[str] = None     # ej: "micro", "pequeña", "mediana"
    sector: Optional[str] = None           # ej: "comercio", "servicios"
    years_operating: Optional[int] = None
    employees: Optional[int] = None


class ChatResponse(BaseModel):
    answer: str                         # texto listo para mostrar
    raw: Dict[str, Any] | None = None   # respuesta completa del agente (por si la quieres en el front)


# --------- Helper para armar una respuesta de texto ---------
def build_answer_text(result: Dict[str, Any]) -> str:
    """
    Toma el dict que devuelve run_query y extrae un texto legible.
    Aquí usamos el resumen ejecutivo si existe.
    """
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}

    resumen = exec_pack.get("resumen_ejecutivo")
    if isinstance(resumen, str) and resumen.strip():
        return resumen.strip()

    # Fallback: si no hay resumen, devolvemos algo genérico
    return "El backend generó un informe, pero no se encontró un 'resumen_ejecutivo' para mostrar."


# --------- Endpoints ---------
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(body: ChatRequest):
    """
    Endpoint principal para Flutter.
    Recibe la pregunta (y opcionalmente el periodo) y responde.
    Por ahora se ignoran los campos de contexto de empresa:
      - company_name, company_size, sector, years_operating, employees
    El backend funciona en modo 'unitario'.
    """
    question = body.question.strip()
    period = (body.period or "2025-08").strip()  # valor por defecto, cámbialo si quieres

    # 🔹 IMPORTANTE: run_query SOLO recibe (question, period)
    result = run_query(question, period)

    # Construimos texto de respuesta
    answer_text = build_answer_text(result)

    return ChatResponse(answer=answer_text, raw=result)

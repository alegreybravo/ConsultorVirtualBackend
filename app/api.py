# app/api.py
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from typing import Any, Dict

from app.graph_lc import run_query
from app.api_models import ChatRequest, ChatResponse
from app.api_adapter import build_frontend_payload


app = FastAPI(
    title="API Agente Virtual",
    description="API para consultar el agente virtual",
    version="0.1.0",
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(body: ChatRequest, debug: bool = False):
    """
    Endpoint principal para el frontend (Flutter, web, etc.).
    - question: pregunta en lenguaje natural
    - period: opcional; si viene vacío, el backend hace NLP/auto
    - debug: si true, incluye el 'raw' completo en la respuesta
    """
    question = body.question.strip()
    period_str = (body.period or "").strip() or None  # None = que el grafo resuelva

    # 👇 Aquí se arma todo el informe (intents, CxC, CxP, etc.)
    result: Dict[str, Any] = run_query(question, period_str)

    # 👇 Aquí se lo "resumimos" para el frontend
    return build_frontend_payload(result, include_raw=debug)

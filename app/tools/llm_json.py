# app/tools/llm_json.py
from __future__ import annotations
from typing import Dict, Any
import json

from langchain.schema import SystemMessage, HumanMessage
from app.lc_llm import get_chat_model


def call_llm_json(
    *,
    system: str,
    user: str,
    model: str | None = None,
    temperature: float = 0.0,
) -> Dict[str, Any]:
    """
    Llama al LLM y devuelve un dict JSON parseado.
    Si falla el parseo, devuelve un fallback seguro.
    """
    llm = get_chat_model(model=model, temperature=temperature)

    resp = llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=user),
    ])

    text = resp.content if isinstance(resp.content, str) else str(resp.content)

    try:
        return json.loads(text)
    except Exception as e:
        return {
            "actions": [
                {"name": "metrics", "params": {}}
            ],
            "reasons": [f"Fallback por error parseando JSON del LLM: {e}"],
        }

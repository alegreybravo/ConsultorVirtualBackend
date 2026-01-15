# app/lc_llm.py
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()


def get_chat_model(model: str | None = None, temperature: float | None = None) -> ChatOpenAI:
    """
    Devuelve un ChatOpenAI configurado.

    - Si 'model' viene en el código -> lo usa.
    - Si no, usa OPENAI_MODEL o 'gpt-4o' por defecto.
    - Igual con temperature: parámetro > ENV > 0.0
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Falta la variable OPENAI_API_KEY")

    model_name = model or os.getenv("OPENAI_MODEL", "gpt-4o")
    temp = temperature if temperature is not None else float(os.getenv("OPENAI_TEMPERATURE", "0"))

    # 👈 OJO: en algunas versiones es model_name, NO model
    return ChatOpenAI(
        model_name=model_name,
        temperature=temp,
        api_key=api_key,
    )

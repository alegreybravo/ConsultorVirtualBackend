# app/lc_llm.py
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

# Cargar variables de entorno desde .env
load_dotenv()

def get_chat_model():
    """
    Devuelve el modelo GPT-5.1-mini para tareas de análisis financiero.
    Configurado con variables de entorno:
      - OPENAI_API_KEY (obligatorio)
      - OPENAI_MODEL (opcional, por defecto 'gpt-5.1')
      - OPENAI_TEMPERATURE (opcional, por defecto 0)
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Falta la variable OPENAI_API_KEY")

    # Por defecto usamos GPT-5.1 (modelo robusto)
    model = os.getenv("OPENAI_MODEL", "gpt-5.1-mini")
    temperature = float(os.getenv("OPENAI_TEMPERATURE", "0"))


    return ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=api_key
    )

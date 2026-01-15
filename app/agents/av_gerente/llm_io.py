# app/agents/av_gerente/llm_io.py
from __future__ import annotations

from typing import Any, Optional
import json

from .utils import sanitize_text, try_parse_any_json


def llm_json(llm, system_prompt: str, user_prompt: str) -> Optional[Any]:
    try:
        resp = llm.invoke(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
        ).content
    except Exception:
        return None

    cleaned = sanitize_text(resp or "")
    return try_parse_any_json(cleaned)

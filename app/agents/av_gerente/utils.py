# app/agents/av_gerente/utils.py
from __future__ import annotations

from typing import Any, Dict, Optional, List
import re
import json
from dateutil import parser as dateparser


def to_jsonable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(v) for v in obj]

    # Objetos tipo fuzzy con atributos low/mid/high → dict
    if all(hasattr(obj, a) for a in ("low", "mid", "high")):
        try:
            return {
                "low": float(getattr(obj, "low")),
                "mid": float(getattr(obj, "mid")),
                "high": float(getattr(obj, "high")),
            }
        except Exception:
            pass

    for attr in ("to_dict", "as_dict", "model_dump", "dict"):
        if hasattr(obj, attr):
            try:
                v = getattr(obj, attr)()
                return to_jsonable(v)
            except Exception:
                pass

    try:
        return {str(k): to_jsonable(v) for k, v in obj.items()}  # type: ignore
    except Exception:
        pass
    try:
        return [to_jsonable(v) for v in obj]  # type: ignore
    except Exception:
        pass

    return str(obj)


def coerce_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def truncate(s: str, max_len: int) -> str:
    if s is None:
        return ""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def safe_pct(ratio: Optional[float]) -> str:
    return "N/D" if ratio is None else f"{round(ratio * 100, 1)}%"


def sanitize_text(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    s = re.sub(r"(?is)<\s*think\s*>.*?</\s*think\s*>", "", s)
    s = re.sub(r"(?is)```(?:json)?(.*?)```", r"\1", s)
    s = re.sub(r"(?is)^(thought|thinking|reasoning|chain\s*of\s*thought).*?(\n\n|$)", "", s)
    return s.strip()


def period_text_and_due(period_in: Any) -> tuple[str, str]:
    """
    Devuelve (period_text, due_yyyy_mm_30)
    - Si `period_in` es dict del router → usa 'text' si existe;
      si no, deriva YYYY-MM de 'start'.
    - Si es str (YYYY-MM) → úsalo directo.
    """
    period_text = ""
    if isinstance(period_in, dict):
        pt = str(period_in.get("text") or "").strip()
        if pt:
            period_text = pt
        else:
            try:
                start = dateparser.isoparse(period_in["start"])
                period_text = f"{start.year:04d}-{start.month:02d}"
            except Exception:
                period_text = ""
    elif isinstance(period_in, str):
        period_text = period_in.strip()

    due = "XXXX-XX-30"

    def _yyyy_mm_from_any(p: Any) -> Optional[str]:
        if isinstance(p, str) and len(p) >= 7 and p[4] == "-":
            return p[:7]
        if isinstance(p, dict):
            for key in ("start", "end"):
                try:
                    dt = dateparser.isoparse(p[key])
                    return f"{dt.year:04d}-{dt.month:02d}"
                except Exception:
                    pass
        return None

    ym = _yyyy_mm_from_any(period_text) or _yyyy_mm_from_any(period_in)
    if ym:
        due = f"{ym}-30"
    return period_text or (ym or ""), due


def try_parse_any_json(text: str) -> Optional[Any]:
    """
    Intenta parsear JSON incluso si el modelo devolvió texto alrededor.
    """
    s = (text or "").strip()
    if s.startswith("{") or s.startswith("["):
        try:
            return json.loads(s)
        except Exception:
            pass

    starts = [m.start() for m in re.finditer(r"[\{\[]", s)]
    ends = [m.start() for m in re.finditer(r"[\}\]]", s)]
    for i in range(len(starts)):
        for j in range(len(ends) - 1, i - 1, -1):
            if ends[j] <= starts[i]:
                continue
            frag = s[starts[i] : ends[j] + 1]
            try:
                return json.loads(frag)
            except Exception:
                continue
    return None

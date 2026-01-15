from pathlib import Path
from functools import lru_cache
import re
import yaml
from typing import Dict, Any, List, Optional

KB_PATH = Path(__file__).resolve().parent.parent / "configs" / "kb_gerente_virtual.yml"


@lru_cache(maxsize=1)
def load_kb() -> dict:
    with KB_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_agent_kb(agent_name: str) -> dict:
    kb = load_kb()
    return (kb.get("agents") or {}).get(agent_name, {}) or {}


def _coerce_number(value: Any) -> Optional[float]:
    """
    Convierte valores a float cuando sea posible.
    Soporta strings con %, moneda, comas, espacios.
    Retorna None si no se puede convertir.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # 50% -> 50
        s = s.replace("%", "")
        # eliminar símbolos de moneda y separadores típicos
        s = re.sub(r"[₡$,\s]", "", s)
        # si queda algo como 80.000,50 (formato europeo) lo normalizamos básico:
        # (si tiene más de un punto, quitamos todos menos el último)
        if s.count(".") > 1:
            parts = s.split(".")
            s = "".join(parts[:-1]) + "." + parts[-1]
        try:
            return float(s)
        except ValueError:
            return None

    # otros tipos
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _text_has_any_keyword(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    t = (text or "").lower()
    return any(str(kw).lower() in t for kw in keywords)


def _rule_applies(
    rule: dict,
    metrics: dict,
    text_query: str = "",
    context: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Evalúa si una regla aplica, soportando:
      - triggers.keywords
      - conditions numéricas: metric/op/value
      - conditions cualitativas: dimension/level (ej: conducta_tiempo=bajo)
      - value como referencia a otra métrica (egresos > ingresos)

    Nota:
      - triggers.schedule se ignora para “consultas on-demand” (no cron).
    """
    metrics = metrics or {}
    context = context or {}
    text_query = (text_query or "")

    # 1) TRIGGERS
    triggers = rule.get("triggers") or {}
    keywords = triggers.get("keywords") or []

    # Si hay keywords, deben aparecer
    if keywords and not _text_has_any_keyword(text_query, keywords):
        return False

    # 2) CONDITIONS (todas deben cumplirse)
    conditions = rule.get("conditions") or []
    if not conditions:
        # Si no hay condiciones, con que pasen triggers (o no haya triggers) aplica
        return True

    for cond in conditions:
        if not isinstance(cond, dict):
            return False

        # 2A) Condición cualitativa: dimension/level
        if "dimension" in cond:
            dim = cond.get("dimension")
            expected = cond.get("level")

            if dim is None or expected is None:
                return False

            # buscamos la dimensión en context (por ejemplo: {"conducta_tiempo": "bajo"})
            actual = context.get(dim)

            # normalizamos strings
            if isinstance(actual, str):
                actual_norm = actual.strip().lower()
            else:
                actual_norm = str(actual).strip().lower() if actual is not None else None

            expected_norm = str(expected).strip().lower()

            if actual_norm is None:
                return False
            if actual_norm != expected_norm:
                return False

            continue  # condición cumplida

        # 2B) Condición numérica: metric/op/value
        metric_name = cond.get("metric")
        if metric_name is None:
            # condición desconocida
            return False

        # debe existir la métrica
        if metric_name not in metrics:
            return False

        m_val_raw = metrics.get(metric_name)
        m_val = _coerce_number(m_val_raw)
        if m_val is None:
            return False

        op = cond.get("op")
        value = cond.get("value")

        # value como referencia a otra métrica (p.ej. "ingresos")
        if isinstance(value, str) and value in metrics:
            value = metrics.get(value)

        v_val = _coerce_number(value)
        if v_val is None:
            return False

        if op == ">":
            if not (m_val > v_val):
                return False
        elif op == "<":
            if not (m_val < v_val):
                return False
        elif op == ">=":
            if not (m_val >= v_val):
                return False
        elif op == "<=":
            if not (m_val <= v_val):
                return False
        elif op == "==":
            if not (m_val == v_val):
                return False
        else:
            # op inválido -> por seguridad, no aplica
            return False

    return True


def get_applicable_rules(
    agent_name: str,
    metrics: dict,
    text_query: str = "",
    context: Optional[Dict[str, Any]] = None,
) -> List[dict]:
    agent_kb = get_agent_kb(agent_name)
    rules = agent_kb.get("rules") or []
    return [r for r in rules if _rule_applies(r, metrics, text_query, context=context)]

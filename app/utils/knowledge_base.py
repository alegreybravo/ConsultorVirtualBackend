from pathlib import Path
from functools import lru_cache
import yaml
from typing import Dict, Any, List

KB_PATH = Path(__file__).resolve().parent.parent / "configs" / "kb_gerente_virtual.yml"


@lru_cache(maxsize=1)
def load_kb() -> dict:
    with KB_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_agent_kb(agent_name: str) -> dict:
    kb = load_kb()
    return (kb.get("agents") or {}).get(agent_name, {}) or {}


def _coerce_number(value: Any) -> Any:
    """
    Intenta convertir a número simple (int/float) cuando sea posible.
    Si no se puede, devuelve el valor original.
    """
    if isinstance(value, (int, float)):
        return value
    try:
        # Evita cascar si es None o algo obviamente no numérico
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return value


def _rule_applies(rule: dict, metrics: dict, text_query: str = "") -> bool:
    """
    Evalúa si una regla aplica dado:
      - metrics: dict de KPIs numéricos (pueden venir con None)
      - text_query: texto de la pregunta del usuario

    Soporta:
      - triggers.keywords
      - conditions con 'metric' numérico
      - condiciones donde value es el nombre de otra métrica (p.ej. egresos > ingresos)

    Ignora (por ahora) condiciones NO numéricas (ej. 'dimension', 'level')
    para evitar errores tipo KeyError / TypeError.
    """
    text_query = (text_query or "").lower()
    metrics = metrics or {}

    # 1) Triggers por keywords (si existen)
    triggers = rule.get("triggers") or {}
    keywords = triggers.get("keywords") or []
    if keywords:
        if not any(kw.lower() in text_query for kw in keywords):
            return False

    # 2) Condiciones numéricas (metric/op/value)
    conditions = rule.get("conditions") or []
    has_numeric_condition = False

    for cond in conditions:
        metric_name = cond.get("metric")
        if metric_name is None:
            # Es una condición de otro tipo (ej. 'dimension'), la ignoramos por ahora
            continue

        has_numeric_condition = True

        # Si la métrica no existe o es None → la regla NO aplica
        if metric_name not in metrics:
            return False

        m_val = metrics.get(metric_name)
        if m_val is None:
            return False

        op = cond.get("op")
        value = cond.get("value")

        # Soporte para value como nombre de otra métrica (p.ej. "ingresos")
        if isinstance(value, str) and value in metrics:
            value = metrics.get(value)

        # Intentar convertir ambos a números simples
        m_val = _coerce_number(m_val)
        value = _coerce_number(value)

        # Si después de eso no son numéricos → por seguridad no aplicamos la regla
        if not isinstance(m_val, (int, float)) or not isinstance(value, (int, float)):
            return False

        # Comparaciones
        if op == ">" and not (m_val > value):
            return False
        elif op == "<" and not (m_val < value):
            return False
        elif op == ">=" and not (m_val >= value):
            return False
        elif op == "<=" and not (m_val <= value):
            return False
        elif op == "==" and not (m_val == value):
            return False
        # Si op es None o algo raro, simplemente no lo usamos como filtro extra

    # Si hay condiciones pero ninguna era numérica → por ahora NO aplicamos la regla
    if conditions and not has_numeric_condition:
        return False

    # Si pasó todos los filtros, la regla aplica
    return True


def get_applicable_rules(agent_name: str, metrics: dict, text_query: str = "") -> List[dict]:
    agent_kb = get_agent_kb(agent_name)
    rules = agent_kb.get("rules") or []
    return [r for r in rules if _rule_applies(r, metrics, text_query)]

# app/agents/aaav_cxc/logic.py
from __future__ import annotations
from typing import Dict, Any

from ..base import BaseAgent
from ...state import GlobalState
from .functions import run_action


# ---------------------------------------------------------------------
# Agente CxC normalizado (orquestador)
# ---------------------------------------------------------------------
class Agent(BaseAgent):
    name = "aaav_cxc"
    role = "operational"

    def handle(self, task, state: GlobalState) -> Dict[str, Any]:
        """
        Espera en payload:
          - period_range: {text, start, end, granularity, tz}  (preferido)
          - period: "YYYY-MM"                                   (fallback)
          - action: {"metrics","top_overdue","customer_balance","list_open","list_overdue"}
          - params: {n, customer, min_days?, max_days?}
        """
        payload = task.get("payload", {}) or {}
        action: str = (payload.get("action") or "metrics").strip()
        params: Dict[str, Any] = payload.get("params", {}) or {}
        question = (payload.get("question") or "").lower().strip()

        # --- Mini-mapeo NL -> acción (sin tocar router) ---
        has_overdue_words = any(
            w in question
            for w in ["vencidas", "vencido", "atrasadas", "atraso"]
        )
        wants_list = any(
            w in question
            for w in [
                "lista",
                "listar",
                "muéstrame",
                "muestrame",
                "mostrar",
                "detalle",
                "detall",
                "todas",
                "cada",
            ]
        )
        wants_aging = (
            "aging" in question
            or "antigüedad" in question
            or "antiguedad" in question
        )

        if action == "metrics":
            if has_overdue_words and wants_list:
                action = "list_overdue"
            elif wants_aging:
                action = "metrics"  # aging vendrá en data_norm["aging"]

        # Delega toda la lógica a functions.run_action
        result = run_action(action=action, payload=payload, params=params, state=state)

        # Aseguramos que siempre devuelva el nombre del agente
        if "agent" not in result:
            result["agent"] = self.name

        return result

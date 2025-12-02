# app/agents/aaav_cxp/logic.py
from __future__ import annotations
from typing import Dict, Any

from ..base import BaseAgent
from ...state import GlobalState
from .functions import run_agent


class Agent(BaseAgent):
    name = "aaav_cxp"
    role = "operational"

    def handle(self, task, state: GlobalState) -> Dict[str, Any]:
        """
        payload:
          - question: str (si no viene 'action', se infiere plan)
          - period_range: dict {text,start,end,...} (preferido)
          - period: 'YYYY-MM' (fallback)
          - action: {"metrics","aging","top_overdue","due_soon","supplier_balance","list_open"} (opcional)
          - params: {n, days, supplier}
        """
        payload = task.get("payload", {}) or {}

        # Delegamos TODO a functions.run_agent
        result = run_agent(payload=payload, state=state)

        # Aseguramos que siempre aparezca el nombre del agente
        if "agent" not in result:
            result["agent"] = self.name

        return result

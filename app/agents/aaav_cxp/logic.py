# app/agents/aaav_cxp/logic.py
from __future__ import annotations

from typing import Dict, Any, Optional
import re

from ..base import BaseAgent
from ...state import GlobalState
from .functions import run_action


# -----------------------------
# Helpers
# -----------------------------
def _extract_supplier_from_question(question: str) -> Optional[str]:
    """
    Extrae proveedor desde preguntas tipo:
      - "¿Cuál es el saldo abierto con Proveedora Gamma S.A. al 29/10/2025?"
      - "saldo con Proveedora Gamma S.A. al 2025-10-29"
      - "saldo con Proveedora Gamma S.A.?"
    """
    q = (question or "").strip()
    if not q:
        return None

    # 1) "con <proveedor> al <fecha>" (dd/mm/yyyy o yyyy-mm-dd)
    m = re.search(
        r"\bcon\s+(.+?)(?:\s+al\s+\d{1,2}/\d{1,2}/\d{2,4}|\s+al\s+\d{4}-\d{1,2}-\d{1,2}|\?|$)",
        q,
        flags=re.IGNORECASE,
    )
    if m:
        name = m.group(1).strip().strip('"').strip("'").rstrip("?").strip()
        return name if len(name) >= 3 else None

    # 2) "con <proveedor> a <fecha>"
    m = re.search(
        r"\bcon\s+(.+?)(?:\s+a\s+\d{1,2}/\d{1,2}/\d{2,4}|\s+a\s+\d{4}-\d{1,2}-\d{1,2}|\?|$)",
        q,
        flags=re.IGNORECASE,
    )
    if m:
        name = m.group(1).strip().strip('"').strip("'").rstrip("?").strip()
        return name if len(name) >= 3 else None

    # 3) fallback: "con <proveedor>" hasta el final
    m = re.search(r"\bcon\s+(.+)$", q, flags=re.IGNORECASE)
    if m:
        name = m.group(1).strip().strip('"').strip("'").rstrip("?").strip()
        return name if len(name) >= 3 else None

    return None


def _autofill_as_of(params: Dict[str, Any], meta: Dict[str, Any]) -> None:
    """
    Rellena params['as_of'] con prioridad:
      meta.as_of.as_of -> meta.due_on.date -> meta.date_range.end
    """
    if params.get("as_of"):
        return

    date_range = meta.get("date_range") or {}
    due_on = meta.get("due_on") or {}
    as_of_meta = meta.get("as_of") or {}

    if isinstance(as_of_meta, dict) and as_of_meta.get("as_of"):
        params["as_of"] = as_of_meta["as_of"]
        return
    if isinstance(due_on, dict) and due_on.get("date"):
        params["as_of"] = due_on["date"]
        return
    if isinstance(date_range, dict) and date_range.get("end"):
        params["as_of"] = date_range["end"]
        return


def _autofill_supplier(params: Dict[str, Any], meta: Dict[str, Any], question: str) -> None:
    """
    Rellena params['supplier'] con prioridad:
      meta.supplier.name -> extraído de la pregunta
    """
    if params.get("supplier"):
        return

    supplier_meta = meta.get("supplier") or {}
    if isinstance(supplier_meta, dict) and supplier_meta.get("name"):
        params["supplier"] = supplier_meta["name"]
        return

    sup = _extract_supplier_from_question(question)
    if sup:
        params["supplier"] = sup


class Agent(BaseAgent):
    name = "aaav_cxp"
    role = "operational"

    def handle(self, task, state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {}) or {}
        question = payload.get("question", "") or ""

        # meta/intent vienen pegados por graph/router
        meta: Dict[str, Any] = payload.get("_meta") or {}
        intent: Dict[str, Any] = meta.get("intent") or {}

        # si router fuerza algo, viene aquí
        action_raw = (payload.get("action") or "").strip()
        params: Dict[str, Any] = dict(payload.get("params") or {})

        # -----------------------------
        # 1) Decidir action por intent
        # -----------------------------
        action = action_raw
        if (not action) or (action == "metrics"):
            # ✅ CXP-01 (resumen abiertas)
            if intent.get("cxp_abiertas_resumen") is True or intent.get("cxp_open_summary") is True:
                action = "cxp_open_summary_as_of"
            # ✅ CXP-02 (aging)
            elif intent.get("aging_cxp") is True or intent.get("aging") is True:
                action = "cxp_aging_as_of"
            # ✅ CXP-03 (top proveedores)
            elif intent.get("top_proveedores_cxp") is True or intent.get("top_suppliers_cxp") is True:
                action = "cxp_top_suppliers_open"
            # ✅ CXP-05 (saldo proveedor)
            elif intent.get("saldo_proveedor_cxp") is True or intent.get("supplier_balance") is True:
                action = "cxp_supplier_open_balance_on"
            else:
                action = "metrics"

        # -----------------------------
        # 2) Auto-params desde meta (as_of)
        # -----------------------------
        if action in ("cxp_aging_as_of", "cxp_top_suppliers_open", "cxp_supplier_open_balance_on", "cxp_open_summary_as_of"):
            _autofill_as_of(params, meta)

        # limit default para top
        if action == "cxp_top_suppliers_open":
            params.setdefault("limit", 5)

        # supplier para saldo proveedor (CXP-05)
        if action == "cxp_supplier_open_balance_on":
            _autofill_supplier(params, meta, question)

        # -----------------------------
        # 3) Ejecutar
        # -----------------------------
        result = run_action(action=action, payload=payload, params=params, state=state) or {}

        if "agent" not in result:
            result["agent"] = self.name

        return result

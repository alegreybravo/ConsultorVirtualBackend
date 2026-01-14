# app/agents/aaav_cxc/logic.py
from __future__ import annotations
from typing import Dict, Any

from ..base import BaseAgent
from ...state import GlobalState
from .functions import run_action


class Agent(BaseAgent):
    name = "aaav_cxc"
    role = "operational"

    def handle(self, task, state: GlobalState) -> Dict[str, Any]:
        payload = task.get("payload", {}) or {}

        # meta/intent vienen pegados por graph/router
        meta: Dict[str, Any] = payload.get("_meta") or {}
        intent: Dict[str, Any] = meta.get("intent") or {}

        # si router fuerza algo, viene aquí
        action_raw = (payload.get("action") or "").strip()
        params: Dict[str, Any] = dict(payload.get("params") or {})

        # -----------------------------
        # 1) Decidir action por intent
        #    IMPORTANTE: si viene "metrics" pero intent pide algo específico,
        #    lo tratamos como NO-FORZADO y remapeamos.
        # -----------------------------
        action = action_raw
        if (not action) or (action == "metrics"):
            if intent.get("vencimientos_rango") is True:
                action = "cxc_due_between"  # CXC-03
            elif intent.get("top_clientes_cxc") is True:
                action = "cxc_top_clients_open"  # CXC-04
            elif intent.get("vencen_hoy_cxc") is True:
                action = "cxc_invoices_due_on"  # CXC-06
            elif intent.get("cxc_pago_parcial") is True or intent.get("pago_parcial_cxc") is True:
                action = "cxc_partial_payments"  # CXC-07
            elif intent.get("saldo_cliente_cxc") is True:
                action = "cxc_customer_open_balance_on"  # ✅ CXC-08
            else:
                action = "metrics"

        # -----------------------------
        # 2) Auto-params desde meta
        # -----------------------------
        date_range = meta.get("date_range") or {}
        due_on = meta.get("due_on") or {}
        as_of_meta = meta.get("as_of") or {}

        # CXC-03 / CXC-07: necesitan start/end
        if action in ("cxc_due_between", "cxc_partial_payments"):
            if not params.get("start") and date_range.get("start"):
                params["start"] = date_range["start"]
            if not params.get("end") and date_range.get("end"):
                params["end"] = date_range["end"]

        # CXC-04: necesita as_of (+limit opcional)
        if action == "cxc_top_clients_open":
            if not params.get("as_of"):
                # ✅ prioriza as_of del graph si existe
                if as_of_meta.get("as_of"):
                    params["as_of"] = as_of_meta["as_of"]
                elif due_on.get("date"):
                    params["as_of"] = due_on["date"]
                elif date_range.get("end"):
                    params["as_of"] = date_range["end"]
            params.setdefault("limit", 5)

        # CXC-06: necesita date (o as_of)
        if action == "cxc_invoices_due_on":
            if not (params.get("date") or params.get("as_of")):
                if due_on.get("date"):
                    params["date"] = due_on["date"]
                elif as_of_meta.get("as_of"):
                    # fallback razonable si alguien usó meta.as_of en vez de due_on
                    params["date"] = as_of_meta["as_of"]

        # ✅ CXC-08: necesita customer + as_of
        if action == "cxc_customer_open_balance_on":
            if not params.get("as_of"):
                if as_of_meta.get("as_of"):
                    params["as_of"] = as_of_meta["as_of"]
                elif due_on.get("date"):
                    params["as_of"] = due_on["date"]
                elif date_range.get("end"):
                    params["as_of"] = date_range["end"]
            # customer puede venir vacío; el bridge en functions.py lo intenta extraer de payload["question"]

        # -----------------------------
        # 3) Ejecutar
        # -----------------------------
        result = run_action(action=action, payload=payload, params=params, state=state) or {}

        if "agent" not in result:
            result["agent"] = self.name

        return result

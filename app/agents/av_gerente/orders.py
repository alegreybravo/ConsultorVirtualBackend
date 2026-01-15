# app/agents/av_gerente/orders.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from .utils import period_text_and_due, coerce_float


def deterministic_orders(ctx: Dict[str, Any], period_in: Any) -> List[Dict[str, Any]]:
    k = ctx.get("kpis", {})
    bal = ctx.get("balances", {})
    dso = k.get("DSO")
    dpo = k.get("DPO")
    ccc = k.get("CCC")
    ar = bal.get("AR_outstanding")
    ap = bal.get("AP_outstanding")
    ratio = (ar / ap) if isinstance(ar, (int, float)) and isinstance(ap, (int, float)) and ap > 0 else None

    _, due = period_text_and_due(period_in)

    orders: List[Dict[str, Any]] = []
    if isinstance(dso, (int, float)) and dso > 45:
        orders.append({"title": "Campaña dunning top-10 clientes", "owner": "CxC", "priority": "P1", "kpi": "DSO", "due": due})
    if isinstance(dpo, (int, float)) and dpo < 40:
        orders.append({"title": "Renegociar 3 proveedores clave", "owner": "CxP", "priority": "P2", "kpi": "DPO", "due": due})
    if isinstance(ccc, (int, float)) and ccc > 20:
        orders.append({"title": "Freeze gastos no esenciales (30d)", "owner": "Administración", "priority": "P1", "kpi": "CCC", "due": due})
    if isinstance(ratio, float) and ratio > 1.30:
        orders.append({"title": "Sync semanal CxC/CxP sobre flujos", "owner": "Administración", "priority": "P3", "kpi": "CCC", "due": due})

    return orders


def kb_orders_from_rules(rules: List[Dict[str, Any]], period_in: Any) -> List[Dict[str, Any]]:
    _, due = period_text_and_due(period_in)
    orders: List[Dict[str, Any]] = []

    for r in rules or []:
        if not isinstance(r, dict):
            continue

        rec = r.get("recommendation") or {}
        impact = (r.get("impact") or {}).get("label") or r.get("impacto") or "medio"

        for o in (r.get("orders") or []):
            if not isinstance(o, dict):
                continue
            orders.append(
                {
                    "title": o.get("title") or rec.get("short") or r.get("name") or "Acción",
                    "owner": o.get("owner") or "Administración",
                    "kpi": o.get("kpi") or "N/D",
                    "due": o.get("due") or due,
                    "impacto": o.get("impacto") or impact,
                    "source_rule": r.get("id"),
                }
            )

        if not (r.get("orders") or []):
            short = rec.get("short")
            if short:
                orders.append(
                    {
                        "title": short,
                        "owner": "Administración",
                        "kpi": "N/D",
                        "due": due,
                        "impacto": impact,
                        "source_rule": r.get("id"),
                    }
                )

    return orders

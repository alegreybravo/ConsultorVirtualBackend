# app/utils/executive_summary.py
from __future__ import annotations

from typing import Any, Dict, Optional
import json

from app.lc_llm import get_chat_model


def _safe_json_or_str(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, default=str)
    except Exception:
        try:
            return str(obj)
        except Exception:
            return "{}"


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in {"true", "1", "si", "sí", "yes", "y"}


def _money(v: Any) -> float:
    try:
        return float(v if v is not None else 0.0)
    except Exception:
        return 0.0


def _date10(v: Any) -> str:
    s = str(v or "").strip()
    return s[:10] if s else ""


def generate_executive_summary(
    question: str,
    intent: Dict[str, Any],
    period_resolved: Dict[str, Any],
    kpis: Dict[str, Any],
    executive_context: Dict[str, Any],
) -> Optional[str]:
    """
    Genera resumen_ejecutivo DESPUÉS de enriquecer executive_context (CXC-06/CXC-03/CXC-04/CXC-07/CXC-08),
    para que NO contradiga la respuesta operativa.

    Retorna string o None.
    """

    llm = get_chat_model()

    # Señales operativas clave (solo lo que importa para no contradecir)
    due_on = (
        (executive_context or {}).get("cxc_due_on")
        or (executive_context or {}).get("cxc_invoices_due_on")
        or {}
    )
    due_range = (executive_context or {}).get("due_range_summary") or {}
    top_clients = (executive_context or {}).get("top_clientes_cxc") or {}
    aging = (executive_context or {}).get("aging_summary") or {}

    # ✅ CXC-07: pagos parciales
    partial = (
        (executive_context or {}).get("cxc_pago_parcial")
        or (executive_context or {}).get("cxc_partial_payments")
        or {}
    )

    # ✅ CXC-08: saldo cliente al corte
    saldo_cliente = (
        (executive_context or {}).get("cxc_saldo_cliente")
        or (executive_context or {}).get("cxc_customer_open_balance_on")
        or {}
    )

    # intent flags
    vencen_hoy_cxc = _coerce_bool((intent or {}).get("vencen_hoy_cxc"))
    vencimientos_rango = _coerce_bool((intent or {}).get("vencimientos_rango"))
    top_clientes_cxc = _coerce_bool((intent or {}).get("top_clientes_cxc"))
    aging_flag = _coerce_bool((intent or {}).get("aging"))

    # ✅ CXC-07
    cxc_pago_parcial = _coerce_bool((intent or {}).get("cxc_pago_parcial"))

    # ✅ CXC-08
    saldo_cliente_cxc = _coerce_bool((intent or {}).get("saldo_cliente_cxc"))

    # ------------------------------------------------------------------
    # ✅ Resumen determinístico para CXC-08 (evita que el LLM “se vaya” al aging global).
    # ------------------------------------------------------------------
    try:
        if saldo_cliente_cxc:
            c = saldo_cliente or {}
            # si por alguna razón no viene saldo_cliente, no devolvemos y dejamos que LLM intente
            if isinstance(c, dict) and ("saldo" in c or "count_facturas" in c):
                as_of = _date10(c.get("as_of") or (period_resolved or {}).get("text") or "")
                customer = str(c.get("customer") or "").strip() or "el cliente consultado"
                saldo = _money(c.get("saldo"))
                cnt = int(c.get("count_facturas") or 0)

                # Nota: el símbolo ₡ lo podés ajustar si tu sistema es multi-moneda.
                return (
                    f"Al {as_of or 'corte consultado'}, {customer} tiene un saldo abierto por cobrar de ₡{saldo:,.2f}, "
                    f"distribuido en {cnt} factura(s) pendiente(s)."
                    if cnt > 0
                    else f"Al {as_of or 'corte consultado'}, {customer} no presenta saldo abierto por cobrar."
                )
    except Exception:
        pass

    # ------------------------------------------------------------------
    # Prompt LLM (para el resto de casos)
    # ------------------------------------------------------------------
    system = (
        "Eres un analista ejecutivo financiero.\n"
        "Tu tarea: redactar un resumen_ejecutivo corto y CLARO (1-3 oraciones).\n"
        "REGLA CRÍTICA: NO puedes contradecir el contexto operativo entregado.\n"
        "Si el contexto dice count>0, DEBES decir que sí hay.\n"
        "Si count==0, DEBES decir que no hay.\n"
        "No inventes datos fuera del JSON.\n"
        "Responde SOLO texto plano (sin JSON)."
    )

    # Construimos un “paquete” mínimo para el LLM (evita que se pierda)
    payload = {
        "question": question,
        "period_resolved": period_resolved,
        "intent": {
            "vencen_hoy_cxc": vencen_hoy_cxc,
            "vencimientos_rango": vencimientos_rango,
            "top_clientes_cxc": top_clientes_cxc,
            "aging": aging_flag,
            # ✅ CXC-07
            "cxc_pago_parcial": cxc_pago_parcial,
            # ✅ CXC-08
            "saldo_cliente_cxc": saldo_cliente_cxc,
        },
        "kpis": kpis,
        "operational_context": {
            "cxc_due_on": due_on,              # CXC-06
            "due_range_summary": due_range,    # CXC-03
            "top_clientes_cxc": top_clients,   # CXC-04
            "aging_summary": aging,            # aging general
            "cxc_pago_parcial": partial,       # CXC-07
            "cxc_saldo_cliente": saldo_cliente # ✅ CXC-08
        },
    }

    # Instrucción específica según intent (para que sea ultra consistente)
    focus = "General."

    # ✅ PRIORIDAD: CXC-08 por encima de todo lo demás
    if saldo_cliente_cxc:
        focus = (
            "Enfócate EXCLUSIVAMENTE en el saldo abierto del cliente al corte (CXC-08). "
            "Usa operational_context.cxc_saldo_cliente.customer, saldo y count_facturas. "
            "NO uses aging_summary.total_por_cobrar porque es global y puede contradecir."
        )
    # ✅ PRIORIDAD: CXC-07 antes que aging / vencidas
    elif cxc_pago_parcial:
        focus = (
            "Enfócate EXCLUSIVAMENTE en facturas CxC con pago parcial. "
            "Usa operational_context.cxc_pago_parcial.count y total_saldo_pendiente. "
            "Si count>0, menciona cantidad y saldo pendiente total. "
            "Si count==0, di explícitamente que no hay facturas con pago parcial."
        )
    elif vencen_hoy_cxc:
        focus = (
            "Enfócate en facturas CxC que vencen en la fecha. "
            "Si operational_context.cxc_due_on.count > 0, menciona cantidad y total. "
            "Si count == 0, di explícitamente que no hay facturas que venzan en esa fecha."
        )
    elif vencimientos_rango:
        focus = (
            "Enfócate en vencimientos en rango. Menciona count y total del due_range_summary."
        )
    elif top_clientes_cxc:
        focus = (
            "Enfócate en top clientes por saldo CxC abierto. Menciona top 1-2 y el total si viene."
        )
    elif aging_flag:
        focus = (
            "Enfócate en aging: total_current, total_overdue y bucket dominante si existe."
        )

    human = (
        f"{focus}\n\n"
        "Contexto (JSON):\n"
        f"{_safe_json_or_str(payload)}\n\n"
        "Redacta el resumen ejecutivo ahora:"
    )

    try:
        msg = llm.invoke(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": human},
            ]
        )
        text = getattr(msg, "content", str(msg))
        text = (text or "").strip()
        return text if text else None
    except Exception:
        return None

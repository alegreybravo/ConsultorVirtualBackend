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
    Genera resumen_ejecutivo DESPUÉS de enriquecer executive_context (patches operativos),
    para que NO contradiga la respuesta operativa.

    Retorna string o None.
    """
    llm = get_chat_model()
    exec_ctx = executive_context or {}

    # ---------------------------------------------------------
    # Contexto operativo CxC (existente)
    # ---------------------------------------------------------
    due_on_cxc = exec_ctx.get("cxc_due_on") or exec_ctx.get("cxc_invoices_due_on") or {}
    due_range = exec_ctx.get("due_range_summary") or {}
    top_clients = exec_ctx.get("top_clientes_cxc") or {}
    aging_cxc = exec_ctx.get("aging_summary") or {}

    partial = exec_ctx.get("cxc_pago_parcial") or exec_ctx.get("cxc_partial_payments") or {}
    saldo_cliente = exec_ctx.get("cxc_saldo_cliente") or exec_ctx.get("cxc_customer_open_balance_on") or {}

    # ---------------------------------------------------------
    # ✅ Contexto operativo CxP (nuevo) - CORREGIDO
    # Tu output real usa executive_context["cxp_aging"]
    # ---------------------------------------------------------
    aging_cxp_ctx = (
        exec_ctx.get("cxp_aging")  # ✅ ESTA ES LA CLAVE QUE ESTÁ SALIENDO EN TU CONTEXTO
        or exec_ctx.get("cxp_aging_summary")
        or exec_ctx.get("aging_cxp_summary")
        or exec_ctx.get("cxp_aging_as_of")
        or {}
    )

    top_suppliers_ctx = (
        exec_ctx.get("top_proveedores_cxp")
        or exec_ctx.get("cxp_top_suppliers_open")
        or exec_ctx.get("cxp_top_suppliers")
        or {}
    )

    saldo_proveedor_ctx = (
        exec_ctx.get("cxp_saldo_proveedor")
        or exec_ctx.get("cxp_supplier_open_balance_on")
        or exec_ctx.get("saldo_proveedor_cxp")
        or {}
    )

    due_on_cxp = (
        exec_ctx.get("cxp_due_on")
        or exec_ctx.get("cxp_invoices_due_on")
        or {}
    )

    # ✅ CXP-01: resumen de abiertas + saldo total (nuevo)
    open_summary_cxp_ctx = (
        exec_ctx.get("cxp_open_summary")
        or exec_ctx.get("cxp_open_summary_as_of")
        or exec_ctx.get("open_summary_cxp")
        or {}
    )

    # ---------------------------------------------------------
    # Intent flags CxC (existente)
    # ---------------------------------------------------------
    vencen_hoy_cxc = _coerce_bool((intent or {}).get("vencen_hoy_cxc"))
    vencimientos_rango = _coerce_bool((intent or {}).get("vencimientos_rango"))
    top_clientes_cxc = _coerce_bool((intent or {}).get("top_clientes_cxc"))
    aging_flag = _coerce_bool((intent or {}).get("aging"))
    cxc_pago_parcial = _coerce_bool((intent or {}).get("cxc_pago_parcial"))
    saldo_cliente_cxc = _coerce_bool((intent or {}).get("saldo_cliente_cxc"))

    # ---------------------------------------------------------
    # ✅ Intent flags CxP (nuevo)
    # ---------------------------------------------------------
    cxp_abiertas_resumen_flag = _coerce_bool((intent or {}).get("cxp_abiertas_resumen"))  # CXP-01
    aging_cxp_flag = _coerce_bool((intent or {}).get("aging_cxp"))
    top_proveedores_cxp_flag = _coerce_bool((intent or {}).get("top_proveedores_cxp"))
    saldo_proveedor_cxp_flag = _coerce_bool((intent or {}).get("saldo_proveedor_cxp"))

    # ---------------------------------------------------------
    # ✅ Resumen determinístico para CXC-08 (existente)
    # ---------------------------------------------------------
    try:
        if saldo_cliente_cxc:
            c = saldo_cliente or {}
            if isinstance(c, dict) and ("saldo" in c or "count_facturas" in c):
                as_of = _date10(c.get("as_of") or (period_resolved or {}).get("text") or "")
                customer = str(c.get("customer") or "").strip() or "el cliente consultado"
                saldo = _money(c.get("saldo"))
                cnt = int(c.get("count_facturas") or 0)

                return (
                    f"Al {as_of or 'corte consultado'}, {customer} tiene un saldo abierto por cobrar de ₡{saldo:,.2f}, "
                    f"distribuido en {cnt} factura(s) pendiente(s)."
                    if cnt > 0
                    else f"Al {as_of or 'corte consultado'}, {customer} no presenta saldo abierto por cobrar."
                )
    except Exception:
        pass

    # ---------------------------------------------------------
    # ✅ Resumen determinístico para CXP-05 (saldo proveedor al corte) (nuevo)
    # ---------------------------------------------------------
    try:
        if saldo_proveedor_cxp_flag:
            p = saldo_proveedor_ctx or {}
            # esperamos algo como: {"as_of": "...", "supplier": "...", "saldo": 123, "count_facturas": 2}
            if isinstance(p, dict) and ("saldo" in p or "count_facturas" in p or "count" in p):
                as_of = _date10(p.get("as_of") or (period_resolved or {}).get("text") or "")
                supplier = str(p.get("supplier") or p.get("proveedor") or "").strip() or "el proveedor consultado"
                saldo = _money(p.get("saldo") or p.get("balance") or p.get("saldo_abierto"))

                # count puede no venir (y eso NO significa saldo 0)
                raw_cnt = p.get("count_facturas", p.get("count", None))
                cnt = None
                try:
                    cnt = int(raw_cnt) if raw_cnt is not None else None
                except Exception:
                    cnt = None

                # ✅ Regla: si saldo > 0 => sí hay saldo aunque cnt sea None/0
                if saldo > 0:
                    if cnt is None or cnt <= 0:
                        return (
                            f"Al {as_of or 'corte consultado'}, el saldo abierto por pagar con {supplier} es de ₡{saldo:,.2f}."
                        )
                    return (
                        f"Al {as_of or 'corte consultado'}, el saldo abierto por pagar con {supplier} es de ₡{saldo:,.2f}, "
                        f"distribuido en {cnt} documento(s) pendiente(s)."
                    )

                # saldo == 0
                return f"Al {as_of or 'corte consultado'}, no hay saldo abierto por pagar con {supplier}."
    except Exception:
        pass

    # ---------------------------------------------------------
    # ✅ Resumen determinístico para CXP-01 (abiertas + saldo total) (nuevo)
    # ---------------------------------------------------------
    try:
        if cxp_abiertas_resumen_flag:
            s = open_summary_cxp_ctx or {}
            if isinstance(s, dict) and ("abiertas" in s or "saldo" in s):
                as_of = _date10(s.get("as_of") or (period_resolved or {}).get("text") or "")
                abiertas = int(s.get("abiertas") or 0)
                saldo = _money(s.get("saldo"))

                if abiertas <= 0 or saldo <= 0:
                    return (
                        f"Al {as_of or 'corte consultado'}, no hay facturas CxP abiertas "
                        f"ni saldo pendiente por pagar."
                    )

                return (
                    f"Al {as_of or 'corte consultado'}, hay {abiertas} factura(s) CxP abierta(s) "
                    f"con un saldo total pendiente por pagar de ₡{saldo:,.2f}."
                )
    except Exception:
        pass

    # ---------------------------------------------------------
    # ✅ Resumen determinístico para CXP-02 (Aging CxP) (RECOMENDADO)
    # Evita que el LLM diga "no hay info" cuando sí existe.
    # ---------------------------------------------------------
    try:
        if aging_cxp_flag:
            a = aging_cxp_ctx or {}
            if isinstance(a, dict) and ("buckets" in a or "total_open" in a or "total_overdue" in a):
                as_of = _date10(a.get("as_of") or (period_resolved or {}).get("text") or "")
                total_open = _money(a.get("total_open"))
                total_overdue = _money(a.get("total_overdue"))
                buckets = a.get("buckets") or {}

                # bucket dominante (si existe)
                dominant_label = ""
                dominant_amount = 0.0
                if isinstance(buckets, dict) and buckets:
                    for k, v in buckets.items():
                        vv = _money(v)
                        if vv > dominant_amount:
                            dominant_amount = vv
                            dominant_label = str(k)

                if total_open <= 0:
                    return f"Al {as_of or 'corte consultado'}, no hay saldo abierto por pagar (CxP)."

                if dominant_label:
                    return (
                        f"Al {as_of or 'corte consultado'}, el saldo abierto por pagar (CxP) es ₡{total_open:,.2f}; "
                        f"de ese total, ₡{total_overdue:,.2f} está vencido. El bucket dominante es '{dominant_label}'."
                    )

                return (
                    f"Al {as_of or 'corte consultado'}, el saldo abierto por pagar (CxP) es ₡{total_open:,.2f}; "
                    f"₡{total_overdue:,.2f} está vencido."
                )
    except Exception:
        pass

    # ---------------------------------------------------------
    # Prompt LLM (para el resto de casos)
    # ---------------------------------------------------------
    system = (
        "Eres un analista ejecutivo financiero.\n"
        "Tu tarea: redactar un resumen_ejecutivo corto y CLARO (1-3 oraciones).\n"
        "REGLA CRÍTICA: NO puedes contradecir el contexto operativo entregado.\n"
        "Si el contexto dice count>0, DEBES decir que sí hay.\n"
        "Si count==0, DEBES decir que no hay.\n"
        "No inventes datos fuera del JSON.\n"
        "Responde SOLO texto plano (sin JSON)."
    )

    payload = {
        "question": question,
        "period_resolved": period_resolved,
        "intent": {
            # CxC
            "vencen_hoy_cxc": vencen_hoy_cxc,
            "vencimientos_rango": vencimientos_rango,
            "top_clientes_cxc": top_clientes_cxc,
            "aging": aging_flag,
            "cxc_pago_parcial": cxc_pago_parcial,
            "saldo_cliente_cxc": saldo_cliente_cxc,
            # CxP
            "cxp_abiertas_resumen": cxp_abiertas_resumen_flag,  # ✅ CXP-01
            "aging_cxp": aging_cxp_flag,
            "top_proveedores_cxp": top_proveedores_cxp_flag,
            "saldo_proveedor_cxp": saldo_proveedor_cxp_flag,
        },
        "kpis": kpis,
        "operational_context": {
            # CxC
            "cxc_due_on": due_on_cxc,
            "due_range_summary": due_range,
            "top_clientes_cxc": top_clients,
            "aging_summary": aging_cxc,
            "cxc_pago_parcial": partial,
            "cxc_saldo_cliente": saldo_cliente,
            # CxP
            "cxp_due_on": due_on_cxp,
            "cxp_aging": aging_cxp_ctx,  # ✅ usa el nombre real
            "top_proveedores_cxp": top_suppliers_ctx,
            "cxp_saldo_proveedor": saldo_proveedor_ctx,
            "cxp_open_summary": open_summary_cxp_ctx,  # ✅ CXP-01
        },
    }

    # ---------------------------------------------------------
    # Foco: prioridades para consistencia (CxP y CxC)
    # ---------------------------------------------------------
    focus = "General."

    if saldo_cliente_cxc:
        focus = (
            "Enfócate EXCLUSIVAMENTE en el saldo abierto del cliente al corte (CXC-08). "
            "Usa operational_context.cxc_saldo_cliente.customer, saldo y count_facturas. "
            "NO uses aging_summary.total_por_cobrar porque es global y puede contradecir."
        )
    elif saldo_proveedor_cxp_flag:
        focus = (
            "Enfócate EXCLUSIVAMENTE en el saldo abierto por pagar del proveedor al corte (CXP-05). "
            "Usa operational_context.cxp_saldo_proveedor.supplier/proveedor, saldo/balance y count_facturas/count. "
            "NO uses cxp_aging si puede contradecir."
        )
    elif cxp_abiertas_resumen_flag:
        focus = (
            "Enfócate EXCLUSIVAMENTE en el resumen de CxP abiertas al corte (CXP-01). "
            "Usa operational_context.cxp_open_summary.abiertas y saldo."
        )
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
        focus = "Enfócate en vencimientos en rango. Menciona count y total del due_range_summary."
    elif top_clientes_cxc:
        focus = "Enfócate en top clientes por saldo CxC abierto. Menciona top 1-2 y el total si viene."
    elif top_proveedores_cxp_flag:
        focus = (
            "Enfócate en top proveedores por saldo CxP abierto. "
            "Si operational_context.top_proveedores_cxp.rows tiene elementos, "
            "LISTA los 5 (o hasta el 'limit' indicado). "
            "Incluye proveedor y saldo en formato de lista. "
            "No inventes proveedores fuera del JSON."
            "Formatea los saldos con ₡ y separadores de miles."
        )
    elif aging_cxp_flag:
        focus = (
            "Enfócate EXCLUSIVAMENTE en aging de CxP usando operational_context.cxp_aging: "
            "total_open, total_overdue y el bucket dominante según buckets."
        )
    elif aging_flag:
        focus = "Enfócate en aging: total_current, total_overdue y bucket dominante si existe."

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

# app/utils/executive_summary.py
from __future__ import annotations

from typing import Any, Dict, Optional
import json

from app.lc_llm import get_chat_model


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2, default=str)
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


def generate_executive_summary(
    question: str,
    intent: Dict[str, Any],
    period_resolved: Dict[str, Any],
    kpis: Dict[str, Any],
    executive_context: Dict[str, Any],
) -> Optional[str]:
    """
    Genera resumen_ejecutivo DESPUÉS de enriquecer executive_context (CXC-06/CXC-03/CXC-04/CXC-07),
    para que NO contradiga la respuesta operativa.

    Retorna string o None.
    """

    llm = get_chat_model()

    # Señales operativas clave (solo lo que importa para no contradecir)
    due_on = (
        executive_context.get("cxc_due_on")
        or executive_context.get("cxc_invoices_due_on")
        or {}
    )
    due_range = executive_context.get("due_range_summary") or {}
    top_clients = executive_context.get("top_clientes_cxc") or {}
    aging = executive_context.get("aging_summary") or {}

    # ✅ CXC-07: pagos parciales
    partial = (
        executive_context.get("cxc_pago_parcial")
        or executive_context.get("cxc_partial_payments")
        or {}
    )

    # intent flags
    vencen_hoy_cxc = _coerce_bool(intent.get("vencen_hoy_cxc"))
    vencimientos_rango = _coerce_bool(intent.get("vencimientos_rango"))
    top_clientes_cxc = _coerce_bool(intent.get("top_clientes_cxc"))
    aging_flag = _coerce_bool(intent.get("aging"))

    # ✅ CXC-07
    cxc_pago_parcial = _coerce_bool(intent.get("cxc_pago_parcial"))

    # ------------------------------------------------------------------
    # ✅ (Opcional recomendado) Resumen determinístico para evitar cualquier
    # contradicción por LLM en casos operativos.
    #
    # Si querés activarlo, descomentá este bloque.
    # ------------------------------------------------------------------
    # try:
    #     if cxc_pago_parcial:
    #         cnt = int((partial or {}).get("count") or 0)
    #         total = _money((partial or {}).get("total_saldo_pendiente"))
    #         return (
    #             f"En el rango consultado, hay {cnt} facturas CxC con pago parcial "
    #             f"y un saldo pendiente total de ₡{total:,.2f}."
    #             if cnt > 0
    #             else "No se encontraron facturas CxC con pago parcial en el rango consultado."
    #         )
    #     if vencen_hoy_cxc:
    #         cnt = int((due_on or {}).get("count") or 0)
    #         total = _money((due_on or {}).get("total") or (due_on or {}).get("saldo_total"))
    #         date = (due_on or {}).get("date") or (due_on or {}).get("as_of") or ""
    #         date = str(date)[:10] if date else ""
    #         if cnt > 0:
    #             return f"Hay {cnt} facturas CxC que vencen en {date}, con un total de ₡{total:,.2f}."
    #         return f"No hay facturas CxC que venzan en {date or 'esa fecha'}."
    #     if vencimientos_rango:
    #         cnt = int((due_range or {}).get("count") or 0)
    #         total = _money((due_range or {}).get("total") or (due_range or {}).get("saldo_total"))
    #         start = str((due_range or {}).get("start") or "")[:10]
    #         end = str((due_range or {}).get("end") or "")[:10]
    #         return f"Entre {start} y {end} vencen {cnt} facturas CxC por un total de ₡{total:,.2f}."
    #     if top_clientes_cxc:
    #         rows = (top_clients or {}).get("rows") or []
    #         if isinstance(rows, list) and rows:
    #             top1 = rows[0]
    #             name = top1.get("cliente_nombre") or f"Cliente #{top1.get('id_entidad_cliente')}"
    #             saldo = _money(top1.get("saldo_total"))
    #             return f"El mayor saldo CxC abierto es de {name}: ₡{saldo:,.2f}."
    # except Exception:
    #     pass

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
        },
        "kpis": kpis,
        "operational_context": {
            "cxc_due_on": due_on,              # CXC-06
            "due_range_summary": due_range,    # CXC-03
            "top_clientes_cxc": top_clients,   # CXC-04
            "aging_summary": aging,            # aging general
            # ✅ CXC-07
            "cxc_pago_parcial": partial,
        },
    }

    # Instrucción específica según intent (para que sea ultra consistente)
    focus = "General."

    # ✅ PRIORIDAD: CXC-07 antes que aging / vencidas
    if cxc_pago_parcial:
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
        f"{_safe_json(payload)}\n\n"
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

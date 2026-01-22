# app/agents/intent.py
from typing import Any, Dict
import json
import re
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate
from app.lc_llm import get_chat_model

try:
    from app.utils.text import strip_think
except Exception:
    def strip_think(text: str) -> str:
        return (text or "").replace("<think>", "").replace("</think>", "").strip()


# ---------------------------------------------------------
# Helpers de normalización / parsing
# ---------------------------------------------------------
def _norm_text(s: str) -> str:
    """Normaliza NBSP y colapsa whitespace."""
    s = (s or "").replace("\u00a0", " ")
    s = " ".join(s.strip().lower().split())
    return s


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, (list, dict)):
        return len(value) > 0
    s = str(value).strip().lower()
    return s in {"true", "sí", "si", "yes", "y", "1"}


def _extract_json(text: str) -> Dict[str, Any]:
    t = strip_think(text or "")
    try:
        return json.loads(t)
    except Exception:
        pass

    start = t.find("{")
    end = t.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(t[start: end + 1])
        except Exception:
            pass
    return {}


# ---------------------------------------------------------
# Regex de fechas
# ---------------------------------------------------------
_RX_DATE_DMY = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")  # 29/10/2025 o 29/10/25
_RX_DATE_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")    # 2025-10-29


def _has_two_dates(text: str) -> bool:
    n = len(_RX_DATE_DMY.findall(text)) + len(_RX_DATE_ISO.findall(text))
    return n >= 2


def _has_any_date(text: str) -> bool:
    return bool(_RX_DATE_DMY.search(text) or _RX_DATE_ISO.search(text))


# Detecta consultas tipo vencen hoy / vencen en fecha (una sola fecha)
_RX_DUE_ON = re.compile(r"\bvenc(?:e|en)\b.*\b(hoy|el|en)\b")


# ---------------------------------------------------------
# Modelo Intent
# ---------------------------------------------------------
class Intent(BaseModel):
    cxc: bool = Field(False)
    cxp: bool = Field(False)
    informe: bool = Field(False)
    aging: bool = Field(False)

    # CXC-03
    vencimientos_rango: bool = Field(False)

    # CXC-04
    top_clientes_cxc: bool = Field(False)

    # CXC-06
    vencen_hoy_cxc: bool = Field(False)

    # CXC-07
    cxc_pago_parcial: bool = Field(False)

    # CXC-08
    saldo_cliente_cxc: bool = Field(False)

    # -------------------------
    # CXP flags
    # -------------------------
    cxp_abiertas_resumen: bool = Field(False)   # CXP-01
    aging_cxp: bool = Field(False)              # CXP-02
    top_proveedores_cxp: bool = Field(False)    # CXP-03
    saldo_proveedor_cxp: bool = Field(False)    # CXP-05

    reason: str = Field("")


# ---------------------------------------------------------
# Router principal
# ---------------------------------------------------------
def route_intent(question: str) -> Intent:
    q_low = (question or "").lower().strip()
    q_norm = _norm_text(question or "")

    # 1) Heurística rápida (no bloquea)
    cxc = any(k in q_low for k in [
        "cxc", "cobrar", "cliente", "clientes", "factura", "facturas", "dso",
        "por cobrar", "cuentas por cobrar"
    ])

    cxp = any(k in q_low for k in [
        "cxp", "proveedor", "proveedores", "pago", "pagos", "dpo",
        "por pagar", "cuentas por pagar"
    ])

    informe = any(k in q_low for k in [
        "informe ejecutivo", "bsc", "balanced scorecard", "resumen gerencial", "informe"
    ])

    # -------------------------
    # CXC-06: vencen hoy / fecha específica (una sola fecha)
    # -------------------------
    vencen_hoy_cxc = False

    if "hoy" in q_norm and re.search(r"\bvenc(?:e|en)\b", q_norm):
        vencen_hoy_cxc = True

    if (
        not vencen_hoy_cxc
        and re.search(r"\bvenc(?:e|en)\b", q_norm)
        and _has_any_date(q_norm)
        and not _has_two_dates(q_norm)
    ):
        vencen_hoy_cxc = True

    if (
        not vencen_hoy_cxc
        and _RX_DUE_ON.search(q_norm)
        and _has_any_date(q_norm)
        and not _has_two_dates(q_norm)
    ):
        vencen_hoy_cxc = True

    # -------------------------
    # Aging (buckets / antigüedad / vencidas)
    # -------------------------
    aging = any(k in q_low for k in [
        "aging", "buckets", "antiguedad", "antigüedad", "no vencido",
        "1-30", "31-60", "61-90", "90+", "vencido", "vencidas", "por vencer"
    ])

    # -------------------------
    # CXC-03: rango de vencimientos (requiere 2 fechas)
    # -------------------------
    vencimientos_kw = any(k in q_low for k in [
        "vence", "vencen", "vencida", "vencidas", "vencimiento", "vencimientos",
        "fecha limite", "fecha límite",
        "entre", "desde", "hasta", "del", "al"
    ])
    vencimientos_rango = bool(vencimientos_kw and _has_two_dates(q_low))

    # -------------------------
    # Keywords base reutilizables
    # -------------------------
    top_kw = any(k in q_low for k in ["top", "ranking", "mayores", "mayor", "principales"])
    saldo_kw = any(k in q_low for k in ["saldo", "saldos", "monto", "montos"])
    abierto_kw = any(k in q_low for k in ["abierto", "abierta", "pendiente", "pendientes", "por cobrar", "por pagar"])

    # -------------------------
    # CXC-04: Top clientes CxC por saldo
    # -------------------------
    clientes_kw = ("cliente" in q_low) or ("clientes" in q_low)

    top_clientes_cxc = bool(
        top_kw and clientes_kw and saldo_kw
        and (abierto_kw or "cxc" in q_low or "cuentas por cobrar" in q_low)
        and not ("cxp" in q_low or "proveedor" in q_low or "proveedores" in q_low)
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
    )

    # -------------------------
    # CXC-07: pago parcial
    # -------------------------
    pago_parcial_kw = any(k in q_norm for k in [
        "pago parcial", "pagos parciales",
        "abono", "abonos",
        "parcialmente pagada", "parcialmente pagadas",
        "pagada parcialmente", "pagadas parcialmente",
        "pago incompleto", "pagos incompletos",
        "saldo pendiente con abono", "abonada", "abonadas"
    ])

    facturas_cxc_kw = any(k in q_norm for k in [
        "factura", "facturas", "cxc", "cuentas por cobrar", "por cobrar"
    ])

    cxc_pago_parcial = bool(pago_parcial_kw and facturas_cxc_kw)

    if cxc_pago_parcial:
        vencimientos_rango = False
        aging = False

    # -------------------------
    # CXC-08: saldo abierto de un cliente a una fecha
    # (FIX: evitar dispararse con proveedores/CxP)
    # -------------------------
    saldo_cliente_cxc = bool(
        saldo_kw
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
        and not top_clientes_cxc
        and (
            ("cliente" in q_low) or ("clientes" in q_low)
            or ("cxc" in q_low) or ("cuentas por cobrar" in q_low) or ("por cobrar" in q_low)
        )
        and not (
            ("proveedor" in q_low) or ("proveedores" in q_low)
            or ("cxp" in q_low) or ("cuentas por pagar" in q_low) or ("por pagar" in q_low)
        )
    )

    # -------------------------
    # CXP-01: cuántas facturas CxP abiertas y saldo total al corte
    # -------------------------
    abiertas_kw = any(k in q_low for k in ["abiertas", "abiertos", "pendientes", "sin pagar", "no pagadas"])
    conteo_kw = any(k in q_low for k in ["cuántas", "cuantas", "cantidad", "numero", "número", "count", "total"])
    facturas_kw = "factura" in q_low or "facturas" in q_low
    resumen_kw = any(k in q_low for k in ["saldo total", "total", "monto total", "resumen"])

    cxp_abiertas_resumen = bool(
        (cxp or ("cuentas por pagar" in q_low) or ("por pagar" in q_low) or ("cxp" in q_low))
        and (facturas_kw or "cuentas por pagar" in q_low or "cxp" in q_low)
        and (abiertas_kw or abierto_kw or conteo_kw)
        and (conteo_kw or "cuántas facturas" in q_low or "cuantas facturas" in q_low)
        and (resumen_kw or saldo_kw)
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
        and not ("cliente" in q_low or "clientes" in q_low or "cxc" in q_low)
        and not top_kw  # evita chocar con top proveedores
    )

    # -------------------------
    # CXP-02: Aging CxP a una fecha
    # -------------------------
    aging_cxp = bool(
        cxp
        and aging
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
    )

    # -------------------------
    # CXP-03: Top proveedores CxP por saldo a una fecha
    # -------------------------
    proveedores_kw = ("proveedor" in q_low) or ("proveedores" in q_low)

    top_proveedores_cxp = bool(
        top_kw and proveedores_kw and saldo_kw
        and (abierto_kw or "cxp" in q_low or "cuentas por pagar" in q_low or "por pagar" in q_low)
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
        and not ("cliente" in q_low or "clientes" in q_low or "cxc" in q_low)
    )

    # ✅ FIX: si es ranking de proveedores, nunca debe activar saldo_cliente_cxc
    if top_proveedores_cxp:
        saldo_cliente_cxc = False
        cxp_abiertas_resumen = False

    # -------------------------
    # CXP-05: saldo abierto con proveedor específico a una fecha
    # -------------------------
    con_kw = " con " in q_low  # "saldo con X"
    saldo_proveedor_cxp = bool(
        cxp
        and saldo_kw
        and (abierto_kw or "cxp" in q_low or "cuentas por pagar" in q_low or "por pagar" in q_low)
        and _has_any_date(q_low)
        and not _has_two_dates(q_low)
        and con_kw
        and not top_proveedores_cxp
        and not cxp_abiertas_resumen
    )

    # -------------------------
    # Ajustes / fuerzas de módulo
    # -------------------------
    if vencen_hoy_cxc and not cxc and not cxp:
        cxc = True

    if vencimientos_rango and not (cxc or cxp):
        cxc = True

    if top_clientes_cxc or vencen_hoy_cxc or cxc_pago_parcial or saldo_cliente_cxc:
        cxc = True

    if cxp_abiertas_resumen or aging_cxp or top_proveedores_cxp or saldo_proveedor_cxp:
        cxp = True

    # ---------------------------------------------------------
    # Cortafuego anti-cruce (heurística)
    # ---------------------------------------------------------
    cxc_specific = any([vencimientos_rango, top_clientes_cxc, vencen_hoy_cxc, cxc_pago_parcial, saldo_cliente_cxc])
    cxp_specific = any([cxp_abiertas_resumen, aging_cxp, top_proveedores_cxp, saldo_proveedor_cxp])

    if cxp_specific and not cxc_specific:
        cxc = False
        vencimientos_rango = False
        top_clientes_cxc = False
        vencen_hoy_cxc = False
        cxc_pago_parcial = False
        saldo_cliente_cxc = False

    if cxc_specific and not cxp_specific:
        cxp = False
        cxp_abiertas_resumen = False
        aging_cxp = False
        top_proveedores_cxp = False
        saldo_proveedor_cxp = False

    # -------------------------
    # Si ya hay señales claras -> NO LLM
    # -------------------------
    if (
        cxc or cxp or informe or aging
        or vencimientos_rango or top_clientes_cxc or vencen_hoy_cxc
        or cxc_pago_parcial or saldo_cliente_cxc
        or cxp_abiertas_resumen or aging_cxp or top_proveedores_cxp or saldo_proveedor_cxp
    ):
        return Intent(
            cxc=cxc,
            cxp=cxp,
            informe=informe,
            aging=aging,
            vencimientos_rango=vencimientos_rango,
            top_clientes_cxc=top_clientes_cxc,
            vencen_hoy_cxc=vencen_hoy_cxc,
            cxc_pago_parcial=cxc_pago_parcial,
            saldo_cliente_cxc=saldo_cliente_cxc,
            cxp_abiertas_resumen=cxp_abiertas_resumen,
            aging_cxp=aging_cxp,
            top_proveedores_cxp=top_proveedores_cxp,
            saldo_proveedor_cxp=saldo_proveedor_cxp,
            reason="Heurística por palabras clave",
        )

    # ---------------------------------------------------------
    # 2) Si es ambiguo, preguntar al LLM
    # ---------------------------------------------------------
    llm = get_chat_model()
    prompt = ChatPromptTemplate.from_messages([
        (
            "system",
            """Eres un router financiero. Debes clasificar la pregunta en flags booleanos:
- cxc = true si requiere Cuentas por Cobrar (DSO, aging, facturas, clientes)
- cxp = true si requiere Cuentas por Pagar (DPO, aging, pagos, proveedores)
- informe = true si pide 'informe ejecutivo', 'BSC', 'resumen gerencial', etc.
- aging = true si pide información sobre aging (buckets, antigüedad, vencidos)

- vencimientos_rango = true si pide cuántas facturas vencen en un rango de fechas (2 fechas). (CXC-03)
- top_clientes_cxc = true si pide ranking/top de clientes por saldo CxC abierto a una fecha. (CXC-04)
- vencen_hoy_cxc = true si pide facturas CxC que vencen hoy o en una fecha específica (1 fecha). (CXC-06)
- cxc_pago_parcial = true si pide facturas CxC con pago parcial. (CXC-07)
- saldo_cliente_cxc = true si pide el saldo abierto de un cliente específico a una fecha. (CXC-08)

- cxp_abiertas_resumen = true si pide cuántas facturas CxP están abiertas/no pagadas al corte y el saldo total, a una fecha. (CXP-01)
- aging_cxp = true si pide aging/buckets de CxP a una fecha. (CXP-02)
- top_proveedores_cxp = true si pide ranking/top de proveedores por saldo CxP abierto a una fecha. (CXP-03)
- saldo_proveedor_cxp = true si pide el saldo abierto de un proveedor específico a una fecha. (CXP-05)

Si la pregunta es ambigua, activa cxc=true y cxp=true.

RESPONDE SOLO un JSON con EXACTAMENTE estas llaves:
cxc, cxp, informe, aging,
vencimientos_rango, top_clientes_cxc, vencen_hoy_cxc, cxc_pago_parcial, saldo_cliente_cxc,
cxp_abiertas_resumen, aging_cxp, top_proveedores_cxp, saldo_proveedor_cxp, reason.
No agregues campos adicionales ni texto extra.
"""
        ),
        (
            "human",
            """Pregunta: {question}

Devuelve SOLO el JSON final (sin comentarios, sin texto extra)."""
        ),
    ])

    try:
        msg = (prompt | llm).invoke({"question": question})
        content = getattr(msg, "content", str(msg))
        obj = _extract_json(content)

        cxc = _coerce_bool(obj.get("cxc"))
        cxp = _coerce_bool(obj.get("cxp"))
        informe = _coerce_bool(obj.get("informe"))
        aging = _coerce_bool(obj.get("aging"))

        vencimientos_rango = _coerce_bool(obj.get("vencimientos_rango"))
        top_clientes_cxc = _coerce_bool(obj.get("top_clientes_cxc"))
        vencen_hoy_cxc = _coerce_bool(obj.get("vencen_hoy_cxc"))
        cxc_pago_parcial = _coerce_bool(obj.get("cxc_pago_parcial"))
        saldo_cliente_cxc = _coerce_bool(obj.get("saldo_cliente_cxc"))

        cxp_abiertas_resumen = _coerce_bool(obj.get("cxp_abiertas_resumen"))
        aging_cxp = _coerce_bool(obj.get("aging_cxp"))
        top_proveedores_cxp = _coerce_bool(obj.get("top_proveedores_cxp"))
        saldo_proveedor_cxp = _coerce_bool(obj.get("saldo_proveedor_cxp"))

        reason = str(obj.get("reason") or "").strip()

        # Fallback mínimo si el LLM no devolvió nada útil
        if not (
            cxc or cxp or informe or aging
            or vencimientos_rango or top_clientes_cxc or vencen_hoy_cxc
            or cxc_pago_parcial or saldo_cliente_cxc
            or cxp_abiertas_resumen or aging_cxp or top_proveedores_cxp or saldo_proveedor_cxp
        ):
            cxc = True
            cxp = True
            reason = "Fallback ambiguo: ambos"

        # fuerzas
        if top_clientes_cxc or vencen_hoy_cxc or cxc_pago_parcial or saldo_cliente_cxc:
            cxc = True
        if cxp_abiertas_resumen or aging_cxp or top_proveedores_cxp or saldo_proveedor_cxp:
            cxp = True

        # ---------------------------------------------------------
        # Cortafuego anti-cruce (LLM)
        # ---------------------------------------------------------
        cxc_specific = any([vencimientos_rango, top_clientes_cxc, vencen_hoy_cxc, cxc_pago_parcial, saldo_cliente_cxc])
        cxp_specific = any([cxp_abiertas_resumen, aging_cxp, top_proveedores_cxp, saldo_proveedor_cxp])

        if cxp_specific and not cxc_specific:
            cxc = False
            vencimientos_rango = False
            top_clientes_cxc = False
            vencen_hoy_cxc = False
            cxc_pago_parcial = False
            saldo_cliente_cxc = False

        if cxc_specific and not cxp_specific:
            cxp = False
            cxp_abiertas_resumen = False
            aging_cxp = False
            top_proveedores_cxp = False
            saldo_proveedor_cxp = False

        return Intent(
            cxc=cxc,
            cxp=cxp,
            informe=informe,
            aging=aging,
            vencimientos_rango=vencimientos_rango,
            top_clientes_cxc=top_clientes_cxc,
            vencen_hoy_cxc=vencen_hoy_cxc,
            cxc_pago_parcial=cxc_pago_parcial,
            saldo_cliente_cxc=saldo_cliente_cxc,
            cxp_abiertas_resumen=cxp_abiertas_resumen,
            aging_cxp=aging_cxp,
            top_proveedores_cxp=top_proveedores_cxp,
            saldo_proveedor_cxp=saldo_proveedor_cxp,
            reason=reason,
        )

    except Exception as e:
        return Intent(
            cxc=True,
            cxp=True,
            informe=False,
            aging=False,
            vencimientos_rango=False,
            top_clientes_cxc=False,
            vencen_hoy_cxc=False,
            cxc_pago_parcial=False,
            saldo_cliente_cxc=False,
            cxp_abiertas_resumen=False,
            aging_cxp=False,
            top_proveedores_cxp=False,
            saldo_proveedor_cxp=False,
            reason=f"Fallback por error LLM: {e}",
        )

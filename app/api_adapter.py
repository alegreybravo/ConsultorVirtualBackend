# app/api_adapter.py
from __future__ import annotations

from typing import Any, Dict, Optional, List
from datetime import datetime

from .api_models import ChatResponse, PeriodInfo, KPIBlock, Hallazgo, Orden


# =========================================================
# Helpers
# =========================================================
def _metric(metrics: Dict[str, Any], *keys: str) -> Optional[float]:
    for k in keys:
        if k in metrics and metrics[k] is not None:
            try:
                return float(metrics[k])
            except (TypeError, ValueError):
                return None
    return None


def _norm_msg(s: Any) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _as_dict_or_none(x: Any) -> Optional[Dict[str, Any]]:
    return x if isinstance(x, dict) else None


def _money(v: Any) -> float:
    try:
        return float(v if v is not None else 0)
    except Exception:
        return 0.0


def _fmt_date(x: Any) -> str:
    """
    Acepta '2025-10-01', '2025-10-01T00:00:00', datetime, etc. y devuelve YYYY-MM-DD.
    """
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.date().isoformat()
    s = str(x).strip()
    if not s:
        return ""
    if "T" in s:
        return s.split("T", 1)[0]
    if " " in s and len(s) >= 10:
        return s[:10]
    return s


# =========================================================
# Texto principal
# =========================================================
def build_answer_text(result: Dict[str, Any], intent: Optional[Dict[str, Any]] = None) -> str:
    """
    Prioridades:
      0) intent.top_clientes_cxc -> Top clientes por saldo abierto al corte
      0b) intent.vencen_hoy_cxc -> Facturas CxC que vencen en una fecha (CXC-06)
      0c) intent.cxc_pago_parcial -> Facturas CxC con pago parcial (CXC-07)
      1) intent.vencimientos_rango -> texto con conteo/saldo del rango
      2) intent.aging -> texto formateado con buckets
      3) resumen_ejecutivo
    """
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}
    ctx = exec_pack.get("executive_context") or {}

    intent = intent or {}

    # -----------------------------------------------------
    # 0) Top clientes por saldo CxC abierto (CXC-04)
    # Espera: executive_context.top_clientes_cxc
    # -----------------------------------------------------
    if intent.get("top_clientes_cxc") is True:
        tc = ctx.get("top_clientes_cxc") or {}
        if isinstance(tc, dict) and tc:
            as_of = _fmt_date(tc.get("as_of")) or ""
            limit = int(tc.get("limit") or 5)
            rows = tc.get("rows") or []

            if isinstance(rows, list) and rows:
                header_date = as_of if as_of else "corte"
                lines = [f"Top {limit} clientes por saldo CxC abierto al {header_date}:"]

                rank = 1
                for r in rows:
                    if not isinstance(r, dict):
                        continue

                    cid = r.get("id_entidad_cliente")
                    nombre = str(r.get("cliente_nombre") or "").strip()
                    label_cliente = nombre if nombre else f"Cliente #{cid}"

                    cnt = int(r.get("count") or 0)
                    saldo = _money(r.get("saldo_total"))

                    label = "factura" if cnt == 1 else "facturas"
                    lines.append(f"{rank}) {label_cliente}: ₡{saldo:,.2f} ({cnt} {label})")
                    rank += 1

                if len(lines) > 1:
                    return "\n".join(lines)

            header_date = as_of if as_of else "ese corte"
            return f"No se encontraron clientes con saldo CxC abierto al {header_date}."

    # -----------------------------------------------------
    # 0b) Facturas CxC que vencen en una fecha (CXC-06)
    # Espera: executive_context.<algo> con {date, count, total, rows:[...]}
    # -----------------------------------------------------
    if intent.get("vencen_hoy_cxc") is True:
        pack = (
            ctx.get("cxc_invoices_due_on")
            or ctx.get("cxc_due_on")
            or ctx.get("invoices_due_on")
            or ctx.get("vencen_hoy_cxc")
            or {}
        )

        if not isinstance(pack, dict) or not pack:
            return "No hay facturas de CxC que venzan en esa fecha."

        fecha = _fmt_date(pack.get("date") or pack.get("as_of") or pack.get("on") or "")
        rows = pack.get("rows") or pack.get("invoices") or []
        total = _money(pack.get("total") or pack.get("saldo_total") or 0)
        count = int(pack.get("count") or (len(rows) if isinstance(rows, list) else 0) or 0)

        if not isinstance(rows, list) or len(rows) == 0 or count == 0:
            label_fecha = fecha or "esa fecha"
            return f"No hay facturas de CxC que venzan en {label_fecha}."

        label_fecha = fecha or "esa fecha"
        lines = [f"Facturas CxC que vencen en {label_fecha} ({count}):"]

        i = 1
        for r in rows:
            if not isinstance(r, dict):
                continue

            num = (
                r.get("numero_factura")
                or r.get("num_factura")
                or r.get("consecutivo")
                or r.get("no_factura")
                or r.get("id_cxc")
            )
            cliente = str(
                r.get("cliente_nombre")
                or r.get("nombre_legal")
                or r.get("cliente")
                or ""
            ).strip()
            saldo = _money(r.get("saldo") or r.get("saldo_total") or r.get("monto_pendiente") or 0)

            label_cliente = cliente if cliente else "Cliente"
            label_num = f"Factura {num}" if num not in (None, "", 0) else "Factura"

            lines.append(f"{i}) {label_num} - {label_cliente}: ₡{saldo:,.2f}")
            i += 1

        if total > 0:
            lines.append(f"Total saldo: ₡{total:,.2f}")

        return "\n".join(lines)

    # -----------------------------------------------------
    # 0c) Facturas CxC con pago parcial (CXC-07)
    # Espera: executive_context.cxc_pago_parcial con {count, rows, total_saldo_pendiente}
    # -----------------------------------------------------
    if intent.get("cxc_pago_parcial") is True or intent.get("pago_parcial_cxc") is True:
        pack = (
            ctx.get("cxc_pago_parcial")
            or ctx.get("cxc_partial_payments")
            or ctx.get("cxc_partial")
            or {}
        )

        if not isinstance(pack, dict) or not pack:
            return "No hay facturas de CxC con pagos parciales en el período."

        rows = pack.get("rows") or pack.get("invoices") or []
        count = int(pack.get("count") or (len(rows) if isinstance(rows, list) else 0) or 0)

        if count == 0 or not isinstance(rows, list) or len(rows) == 0:
            return "No hay facturas de CxC con pagos parciales en el período."

        total_saldo_pendiente = _money(
            pack.get("total_saldo_pendiente")
            or pack.get("total_saldo")
            or pack.get("total")
            or 0
        )

        lines = [f"Facturas CxC con pago parcial ({count}):"]

        for i, r in enumerate(rows, start=1):
            if not isinstance(r, dict):
                continue

            cliente = (
                r.get("cliente")
                or r.get("cliente_nombre")
                or r.get("nombre_legal")
                or r.get("razon_social")
                or "Cliente"
            )

            monto_original = _money(
                r.get("monto_original")
                or r.get("original")
                or r.get("total_factura")
                or r.get("monto_total")
                or 0
            )
            monto_pagado = _money(
                r.get("monto_pagado")
                or r.get("pagado")
                or r.get("total_pagado")
                or r.get("abono")
                or 0
            )
            saldo_pendiente = _money(
                r.get("saldo_pendiente")
                or r.get("saldo")
                or r.get("monto_pendiente")
                or 0
            )

            lines.append(
                f"{i}) {cliente}: "
                f"Original ₡{monto_original:,.2f}, "
                f"Pagado ₡{monto_pagado:,.2f}, "
                f"Saldo ₡{saldo_pendiente:,.2f}"
            )

        if total_saldo_pendiente > 0:
            lines.append(f"Saldo pendiente total: ₡{total_saldo_pendiente:,.2f}")

        return "\n".join(lines)

    # -----------------------------------------------------
    # 1) Vencimientos en rango (CXC-03)
    # Espera: executive_context.due_range_summary
    # -----------------------------------------------------
    if intent.get("vencimientos_rango") is True:
        dr = ctx.get("due_range_summary") or {}
        if isinstance(dr, dict) and dr:
            start = _fmt_date(dr.get("start"))
            end = _fmt_date(dr.get("end"))
            count = int(dr.get("count") or 0)

            saldo_total = _money(dr.get("total"))
            if saldo_total == 0.0:
                saldo_total = _money(dr.get("saldo_total"))

            etiqueta = "CxP" if intent.get("cxp") else "CxC"

            return (
                f"Entre {start} y {end} vencen {count} facturas {etiqueta} "
                f"con un saldo total de ₡{saldo_total:,.2f}."
            )

    # -----------------------------------------------------
    # 2) Aging
    # -----------------------------------------------------
    if intent.get("aging") is True:
        aging = ctx.get("aging_summary") or {}
        buckets = (aging.get("buckets_overdue") or {}) if isinstance(aging, dict) else {}

        if isinstance(aging, dict) and aging:
            total_current = _money(aging.get("total_current"))
            b_1_30 = _money(buckets.get("overdue_1_30"))
            b_31_60 = _money(buckets.get("overdue_31_60"))
            b_61_90 = _money(buckets.get("overdue_61_90"))
            b_90p = _money(buckets.get("overdue_90_plus"))

            return (
                "Aging de Cuentas por Cobrar:\n"
                f"- No vencido: ₡{total_current:,.2f}\n"
                f"- 1–30 días: ₡{b_1_30:,.2f}\n"
                f"- 31–60 días: ₡{b_31_60:,.2f}\n"
                f"- 61–90 días: ₡{b_61_90:,.2f}\n"
                f"- 90+ días: ₡{b_90p:,.2f}"
            )

    # -----------------------------------------------------
    # 3) Resumen ejecutivo
    # -----------------------------------------------------
    resumen = exec_pack.get("resumen_ejecutivo")
    if isinstance(resumen, str) and resumen.strip():
        return resumen.strip()

    return (
        "El backend generó un informe estructurado, "
        "pero no se encontró un resumen ejecutivo para mostrar."
    )


# =========================================================
# Adapter principal hacia el frontend
# =========================================================
def build_frontend_payload(result: Dict[str, Any], include_raw: bool) -> ChatResponse:
    # -----------------------------------------------------
    # Período resuelto (✅ preferir date_range cuando aplica)
    # -----------------------------------------------------
    meta_root = (result.get("_meta") or {})
    period_meta = meta_root.get("period_resolved") or {}
    intent_meta = meta_root.get("intent") or {}
    date_range_meta = meta_root.get("date_range") or {}

    use_range = (
        (intent_meta.get("vencimientos_rango") is True or intent_meta.get("cxc_pago_parcial") is True)
        and isinstance(date_range_meta, dict)
        and date_range_meta.get("start")
        and date_range_meta.get("end")
    )

    meta = date_range_meta if use_range else period_meta

    period = None
    if meta:
        period = PeriodInfo(
            text=meta.get("text", ""),
            start=meta.get("start", ""),
            end=meta.get("end", ""),
            granularity=meta.get("granularity", ""),
            tz=meta.get("tz", ""),
        )

    # -----------------------------------------------------
    # KPIs
    # -----------------------------------------------------
    gerente = result.get("gerente") or {}
    exec_pack = gerente.get("executive_decision_bsc") or {}

    metrics: Dict[str, Any] = {}
    metrics.update(result.get("metrics") or {})
    metrics.update(exec_pack.get("kpis") or {})

    kpis = KPIBlock(
        dso=_metric(metrics, "dso", "DSO"),
        dpo=_metric(metrics, "dpo", "DPO"),
        ccc=_metric(metrics, "ccc", "CCC"),
    )

    # -----------------------------------------------------
    # Admin pack
    # -----------------------------------------------------
    admin = result.get("administrativo") or result.get("av_administrativo") or {}

    # -----------------------------------------------------
    # Hallazgos
    # -----------------------------------------------------
    gerente_hallazgos = exec_pack.get("hallazgos") or []
    admin_hallazgos = admin.get("hallazgos") or []

    admin_by_msg: Dict[str, Dict[str, Any]] = {}
    if isinstance(admin_hallazgos, list):
        for h in admin_hallazgos:
            if isinstance(h, dict):
                msg = h.get("msg", "")
                admin_by_msg[_norm_msg(msg)] = {
                    "id": h.get("id"),
                    "msg": msg,
                    "severity": h.get("severity"),
                }
            elif isinstance(h, str) and h.strip():
                admin_by_msg[_norm_msg(h)] = {"id": None, "msg": h.strip(), "severity": None}

    hallazgos: List[Hallazgo] = []
    seen_msgs = set()

    def _push_hallazgo(msg: str, fallback_id: Optional[str] = None, fallback_sev: Optional[str] = "info"):
        k = _norm_msg(msg)
        if not k or k in seen_msgs:
            return
        seen_msgs.add(k)

        enriched = admin_by_msg.get(k)
        if enriched:
            hallazgos.append(
                Hallazgo(
                    id=enriched.get("id") or fallback_id,
                    msg=str(enriched.get("msg") or msg).strip(),
                    severity=enriched.get("severity") or fallback_sev,
                )
            )
        else:
            hallazgos.append(
                Hallazgo(
                    id=fallback_id,
                    msg=msg.strip(),
                    severity=fallback_sev,
                )
            )

    if isinstance(gerente_hallazgos, list) and gerente_hallazgos:
        for i, h in enumerate(gerente_hallazgos, start=1):
            if isinstance(h, str) and h.strip():
                _push_hallazgo(h, fallback_id=f"H{i}", fallback_sev="info")
            elif isinstance(h, dict):
                msg = str(h.get("msg", "")).strip()
                if msg:
                    _push_hallazgo(
                        msg,
                        fallback_id=h.get("id") or f"H{i}",
                        fallback_sev=h.get("severity") or "info",
                    )
    else:
        if isinstance(admin_hallazgos, list):
            for i, h in enumerate(admin_hallazgos, start=1):
                if isinstance(h, dict):
                    msg = str(h.get("msg", "")).strip()
                    if msg:
                        _push_hallazgo(
                            msg,
                            fallback_id=h.get("id") or f"H{i}",
                            fallback_sev=h.get("severity") or "info",
                        )
                elif isinstance(h, str) and h.strip():
                    _push_hallazgo(h, fallback_id=f"H{i}", fallback_sev="info")

    # -----------------------------------------------------
    # Órdenes
    # -----------------------------------------------------
    orders_src = admin.get("orders")
    if not orders_src:
        orders_src = (
            exec_pack.get("ordenes_prioritarias")
            or exec_pack.get("orders")
            or exec_pack.get("ordenes")
            or []
        )

    ordenes: List[Orden] = []
    if isinstance(orders_src, list):
        for o in orders_src:
            if isinstance(o, dict):
                ordenes.append(
                    Orden(
                        title=str(o.get("title", "")),
                        owner=o.get("owner"),
                        kpi=o.get("kpi"),
                        due=o.get("due"),
                        priority=o.get("priority"),
                        impacto=o.get("impacto"),
                    )
                )

    # -----------------------------------------------------
    # Texto principal (con intent)
    # -----------------------------------------------------
    answer_text = build_answer_text(result, intent=intent_meta)

    return ChatResponse(
        answer=answer_text,
        period=period,
        kpis=kpis,
        resumen_ejecutivo=exec_pack.get("resumen_ejecutivo"),
        executive_context=_as_dict_or_none(exec_pack.get("executive_context")),
        hallazgos=hallazgos,
        ordenes=ordenes,
        raw=result if include_raw else None,
    )

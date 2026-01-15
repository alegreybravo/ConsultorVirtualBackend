# app/dates/period_resolver.py
from __future__ import annotations
from datetime import datetime, timedelta
import re
import calendar
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Costa_Rica")

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}
QUARTERS = {"q1": (1, 3), "q2": (4, 6), "q3": (7, 9), "q4": (10, 12)}

def _end_of_month(year: int, month: int) -> datetime:
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day, 23, 59, 59, tzinfo=TZ)

def _start_of_month(year: int, month: int) -> datetime:
    return datetime(year, month, 1, 0, 0, 0, tzinfo=TZ)

def _current_now() -> datetime:
    return datetime.now(TZ)

def resolve_period(nl: str | None, override: dict | None = None) -> dict:
    """
    Resuelve el período como rango [start, end] en TZ America/Costa_Rica.
    Precedencia: override (param) > nlp > default (mes actual).

    override esperado:
        {"start": ISO8601, "end": ISO8601, "text"?: str, "granularity"?: str}
    """
    # 0) Param override
    if override and override.get("start") and override.get("end"):
        return {
            "text": override.get("text", "param"),
            "start": datetime.fromisoformat(override["start"]).astimezone(TZ),
            "end": datetime.fromisoformat(override["end"]).astimezone(TZ),
            "granularity": override.get("granularity", "custom"),
            "source": "param",
            "tz": str(TZ)
        }

    text = (nl or "").lower().strip()
    now = _current_now()

    # 1) Rango explícito: "del 5 al 20 de octubre de 2025"
    m = re.search(r"del?\s*(\d{1,2})\s*al?\s*(\d{1,2})\s*de?\s*([a-záéíóú]+)(?:\s*de?\s*(\d{4}))?", text)
    if m and m.group(3) in SPANISH_MONTHS:
        d1, d2 = int(m.group(1)), int(m.group(2))
        month = SPANISH_MONTHS[m.group(3)]
        year = int(m.group(4) or now.year)
        start = datetime(year, month, d1, 0, 0, 0, tzinfo=TZ)
        end   = datetime(year, month, d2, 23, 59, 59, tzinfo=TZ)
        return {"text": m.group(0), "start": start, "end": end, "granularity": "range", "source": "nlp", "tz": str(TZ)}

    # 1.b) Fecha puntual → usar el MES de esa fecha como ventana
    # ISO: 2025-10-29
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", text)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return {
            "text": f"fecha:{y:04d}-{mo:02d}-{d:02d}",
            "start": _start_of_month(y, mo),
            "end": _end_of_month(y, mo),
            "granularity": "month",
            "source": "nlp",
            "tz": str(TZ)
        }
        # 1.c) Year-Month: 2025-10  -> ventana mensual
    m = re.search(r"\b(\d{4})-(\d{2})\b", text)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return {
                "text": f"{y:04d}-{mo:02d}",
                "start": _start_of_month(y, mo),
                "end": _end_of_month(y, mo),
                "granularity": "month",
                "source": "nlp",
                "tz": str(TZ)
            }


    # LatAm: 29/10/2025 o 29/10/25
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:  # 25 -> 2025
            y += 2000
        return {
            "text": f"fecha:{y:04d}-{mo:02d}-{d:02d}",
            "start": _start_of_month(y, mo),
            "end": _end_of_month(y, mo),
            "granularity": "month",
            "source": "nlp",
            "tz": str(TZ)
        }

    # Español: 29 de octubre de 2025 (o sin 'de 2025' → asume año actual)
    m = re.search(r"\b(\d{1,2})\s+de\s+([a-záéíóú]+)(?:\s+de\s+(\d{4}))?\b", text)
    if m and m.group(2) in SPANISH_MONTHS:
        d = int(m.group(1))
        mo = SPANISH_MONTHS[m.group(2)]
        y = int(m.group(3) or now.year)
        return {
            "text": f"fecha:{y:04d}-{mo:02d}-{d:02d}",
            "start": _start_of_month(y, mo),
            "end": _end_of_month(y, mo),
            "granularity": "month",
            "source": "nlp",
            "tz": str(TZ)
        }

    # 2) QX YYYY
    m = re.search(r"(q[1-4])\s*(\d{4})", text)
    if m:
        q = m.group(1)
        y = int(m.group(2))
        m1, m2 = QUARTERS[q]
        start = _start_of_month(y, m1)
        end   = _end_of_month(y, m2)
        return {"text": m.group(0), "start": start, "end": end, "granularity": "quarter", "source": "nlp", "tz": str(TZ)}

    # 3) "agosto 2025" | "octubre 2024"
    m = re.search(r"([a-záéíóú]+)\s+(\d{4})", text)
    if m and m.group(1) in SPANISH_MONTHS:
        y = int(m.group(2)); mo = SPANISH_MONTHS[m.group(1)]
        return {"text": m.group(0), "start": _start_of_month(y, mo), "end": _end_of_month(y, mo), "granularity": "month", "source": "nlp", "tz": str(TZ)}

    # 4) Solo mes: "agosto" / "octubre"
    m = re.search(r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|setiembre|octubre|noviembre|diciembre)\b", text)
    if m:
        mo = SPANISH_MONTHS[m.group(1)]
        y = now.year
        # Si el mes es muy “futuro” comparado con hoy, usa el año anterior (regla determinista simple)
        if mo > now.month + 1:
            y = now.year - 1
        return {"text": m.group(1), "start": _start_of_month(y, mo), "end": _end_of_month(y, mo), "granularity": "month", "source": "nlp", "tz": str(TZ)}

    # 5) Relativas
    if "esta semana" in text or "de esta semana" in text:
        # Semana ISO (lunes a domingo)
        weekday = (now.weekday() + 6) % 7  # 0=lunes
        start = (now - timedelta(days=weekday)).replace(hour=0, minute=0, second=0, microsecond=0)
        end   = (start + timedelta(days=6)).replace(hour=23, minute=59, second=59)
        return {"text": "esta semana", "start": start, "end": end, "granularity": "week", "source": "nlp", "tz": str(TZ)}
    if "este mes" in text or "de este mes" in text:
        return {"text": "este mes", "start": _start_of_month(now.year, now.month), "end": _end_of_month(now.year, now.month), "granularity": "month", "source": "nlp", "tz": str(TZ)}
    if "mes pasado" in text or "del mes pasado" in text:
        prev_y, prev_m = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
        return {"text": "mes pasado", "start": _start_of_month(prev_y, prev_m), "end": _end_of_month(prev_y, prev_m), "granularity": "month", "source": "nlp", "tz": str(TZ)}
    if "hoy" in text:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end   = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return {"text": "hoy", "start": start, "end": end, "granularity": "day", "source": "nlp", "tz": str(TZ)}
    if "últimos 30 días" in text or "ultimos 30 dias" in text:
        start = (now - timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)
        end   = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return {"text": "últimos 30 días", "start": start, "end": end, "granularity": "rolling_30d", "source": "nlp", "tz": str(TZ)}

    # 6) Default: mes actual
    return {
        "text": "auto: mes actual",
        "start": _start_of_month(now.year, now.month),
        "end": _end_of_month(now.year, now.month),
        "granularity": "month",
        "source": "default",
        "tz": str(TZ),
        "warning": "period_auto_default"
    }

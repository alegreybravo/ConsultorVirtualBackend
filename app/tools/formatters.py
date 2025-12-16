# app/tools/formatters.py
from __future__ import annotations
from typing import Any, Optional


def format_currency(value: Optional[float | int], symbol: str = "₡") -> str:
    """
    Formatea un número como moneda: 80899.99 -> '₡80,899.99'.
    Si value es None, devuelve 'N/D'.
    """
    if value is None:
        return "N/D"
    try:
        return f"{symbol}{float(value):,.2f}"
    except Exception:
        return "N/D"


def format_days(value: Optional[float | int]) -> str:
    """
    Formatea días: 31 -> '31.0 días'.
    Si value es None, devuelve 'N/D'.
    """
    if value is None:
        return "N/D"
    try:
        return f"{float(value):.1f} días"
    except Exception:
        return "N/D"


def format_number(value: Optional[float | int], decimals: int = 2) -> str:
    """
    Formato genérico de números con separador de miles.
    """
    if value is None:
        return "N/D"
    try:
        fmt = f"{{:,.{decimals}f}}"
        return fmt.format(float(value))
    except Exception:
        return "N/D"

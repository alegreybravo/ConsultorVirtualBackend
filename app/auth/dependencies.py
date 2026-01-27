# app/auth/dependencies.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException
from firebase_admin import auth


@dataclass(frozen=True)
class CurrentUser:
    uid: str
    email: Optional[str]
    claims: Dict[str, Any]


def get_current_user(authorization: str = Header(default="")) -> CurrentUser:
    """
    Espera: Authorization: Bearer <Firebase ID Token>
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Falta header Authorization")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Authorization debe ser Bearer <token>")

    token = parts[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token vacío")

    try:
        decoded = auth.verify_id_token(token)
    except Exception:
        # No tiramos detalles internos de seguridad
        raise HTTPException(status_code=401, detail="Token inválido o expirado")

    uid = str(decoded.get("uid") or decoded.get("sub") or "")
    if not uid:
        raise HTTPException(status_code=401, detail="Token sin uid")

    return CurrentUser(
        uid=uid,
        email=decoded.get("email"),
        claims=decoded,  # incluye custom claims si los usas
    )

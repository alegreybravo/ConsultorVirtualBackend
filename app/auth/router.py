# app/auth/router.py
from __future__ import annotations

from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Body, HTTPException
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.auth.dependencies import get_current_user, CurrentUser
from app.auth.service import sync_usuario  # ✅ viene del service

router = APIRouter(prefix="/auth", tags=["auth"])


# Dependency para DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/register")
def register(
    user: CurrentUser = Depends(get_current_user),
    db: Session = Depends(get_db),
    nombre: Optional[str] = Body(default=None, embed=True),
) -> Dict[str, Any]:
    """
    Sincroniza el usuario autenticado con Firebase
    en la tabla agente_virtual.usuarios
    """
    try:
        usuario = sync_usuario(
            db=db,
            firebase_uid=user.uid,
            email=user.email,
            nombre=nombre,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise HTTPException(status_code=500, detail="Error guardando usuario")

    return {
        "ok": True,
        "user": {
            "id": usuario.id,
            "firebase_uid": usuario.firebase_uid,
            "email": usuario.email,
            "nombre": usuario.nombre,
            "rol": usuario.rol,
            "estado": usuario.estado,
        },
    }

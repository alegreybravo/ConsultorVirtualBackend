# app/auth/service.py
from sqlalchemy.orm import Session
from app.auth.repository import upsert_usuario


def sync_usuario(
    db: Session,
    firebase_uid: str,
    email: str | None,
    nombre: str | None,
):
    return upsert_usuario(
        db=db,
        firebase_uid=firebase_uid,
        email=email,
        nombre=nombre,
    )

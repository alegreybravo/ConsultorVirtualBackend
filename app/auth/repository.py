# app/auth/repository.py
from sqlalchemy import text
from sqlalchemy.orm import Session


def upsert_usuario(
    db: Session,
    firebase_uid: str,
    email: str | None,
    nombre: str | None,
):
    q = text("""
        INSERT INTO agente_virtual.usuarios (firebase_uid, email, nombre, estado)
        VALUES (:uid, :email, COALESCE(:nombre, ''), 'activo')
        ON CONFLICT (firebase_uid)
        DO UPDATE SET
            email  = EXCLUDED.email,
            nombre = CASE
                      WHEN EXCLUDED.nombre <> '' THEN EXCLUDED.nombre
                      ELSE agente_virtual.usuarios.nombre
                    END
        RETURNING id, firebase_uid, email, nombre, rol, estado
    """)

    return db.execute(
        q,
        {
            "uid": firebase_uid,
            "email": email,
            "nombre": nombre,
        },
    ).fetchone()

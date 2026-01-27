# app/auth/firebase_admin.py
import os
import firebase_admin
from firebase_admin import credentials


def init_firebase() -> None:
    """
    Inicializa Firebase Admin SDK una sola vez.
    Requiere variable de entorno:
      - FIREBASE_SERVICE_ACCOUNT=/ruta/al/serviceAccount.json
    """
    if firebase_admin._apps:
        return  # ya inicializado

    sa_path = os.getenv("FIREBASE_SERVICE_ACCOUNT")
    if not sa_path:
        raise RuntimeError(
            "Falta FIREBASE_SERVICE_ACCOUNT en el .env (ruta al service account JSON)."
        )

    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)

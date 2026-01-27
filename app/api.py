# app/api.py
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Depends
from typing import Any, Dict

from app.graph_lc import run_query
from app.api_models import ChatRequest, ChatResponse
from app.api_adapter import build_frontend_payload

# ✅ Firebase init + auth dependency
from app.auth.firebase_admin import init_firebase
from app.auth.dependencies import get_current_user, CurrentUser

# ✅ NUEVO: incluir router de auth (/auth/register)
from app.auth.router import router as auth_router


app = FastAPI(
    title="API Agente Virtual",
    description="API para consultar el agente virtual",
    version="0.1.0",
)

# ✅ registrar endpoints de auth
app.include_router(auth_router)


@app.on_event("startup")
def _startup():
    init_firebase()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat", response_model=ChatResponse)
async def chat(
    body: ChatRequest,
    debug: bool = False,
    user: CurrentUser = Depends(get_current_user),
):
    question = body.question.strip()
    period_str = (body.period or "").strip() or None

    meta = {
        "auth": {
            "uid": user.uid,
            "email": user.email,
            "claims": user.claims,
        }
    }

    result: Dict[str, Any] = run_query(question, period_str, meta=meta)
    return build_frontend_payload(result, include_raw=debug)

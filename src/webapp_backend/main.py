# file: src/webapp_backend/main.py
import logging
import os
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from supabase import Client, create_client

from .cards_service import build_feed_for_user_paginated
from .profile_service import get_onboarding_state, save_onboarding

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Пути
# ==========

THIS_DIR = Path(__file__).resolve().parent          # .../src/webapp_backend
ROOT_DIR = THIS_DIR.parents[2]                      # .../eyye-tg-bot
WEBAPP_DIR = ROOT_DIR / "webapp"
INDEX_HTML_PATH = WEBAPP_DIR / "index.html"

if not INDEX_HTML_PATH.exists():
    logger.warning("index.html not found at %s", INDEX_HTML_PATH)

# ==========
# Supabase
# ==========

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized in webapp_backend")
else:
    logger.warning("Supabase URL/KEY are not set. /api/feed and /api/profile will not work.")


# ==========
# FastAPI app
# ==========

app = FastAPI(title="EYYE WebApp Backend")

# Статика: webapp/ -> /static/*
app.mount(
    "/static",
    StaticFiles(directory=str(WEBAPP_DIR)),
    name="static",
)

# CORS на будущее, сейчас всё из того же домена
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========
# Pydantic модели
# ==========


class OnboardingRequest(BaseModel):
    tg_id: int
    city: Optional[str] = None
    selected_topics: List[str] = []


# ==========
# Маршруты
# ==========


@app.get("/ping")
async def ping() -> dict:
    return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/", response_class=HTMLResponse)
async def index(tg_id: str | None = None) -> HTMLResponse:
    """
    Отдаём index.html WebApp. tg_id читается на фронте из Telegram WebApp initData
    или из query-параметра для локальной отладки.
    """
    if not INDEX_HTML_PATH.exists():
        raise HTTPException(status_code=500, detail="index.html not found")

    try:
        html = INDEX_HTML_PATH.read_text(encoding="utf-8")
    except Exception:
        logger.exception("Failed to read index.html")
        raise HTTPException(status_code=500, detail="failed to read index.html")

    return HTMLResponse(content=html)


@app.get("/api/feed")
async def api_feed(
    tg_id: int = Query(..., alias="tg_id"),
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> dict:
    """
    Основной endpoint для WebApp:
    возвращает персональную ленту карточек для пользователя с поддержкой пагинации.

    Параметры:
      - tg_id: ID пользователя из Telegram
      - limit: сколько карточек вернуть
      - offset: сколько карточек пропустить от начала персонализированного списка
    """
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    items, debug, cursor = build_feed_for_user_paginated(
        supabase=supabase,
        user_id=tg_id,
        limit=limit,
        offset=offset,
    )
    return {
        "items": items,
        "cursor": cursor,
        "debug": debug,
    }


@app.get("/api/profile")
async def api_get_profile(
    tg_id: int = Query(..., alias="tg_id"),
) -> Dict[str, Any]:
    """
    Эндпоинт для WebApp, чтобы понять, надо ли показывать онбординг.

    Возвращает, например:
    {
      "has_onboarding": bool,
      "city": str | None,
      "selected_topics": [...]
    }
    """
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    profile_state = get_onboarding_state(supabase, tg_id=tg_id)
    return profile_state


@app.post("/api/profile/onboarding")
async def api_profile_onboarding(payload: OnboardingRequest) -> dict:
    """
    Сохранение онбординга: город и выбранные темы в user_profiles.structured_profile.
    """
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    save_onboarding(
        supabase,
        tg_id=payload.tg_id,
        city=payload.city,
        selected_topics=payload.selected_topics,
    )
    return {"status": "ok"}

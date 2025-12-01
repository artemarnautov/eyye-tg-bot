# file: src/webapp_backend/main.py
import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from supabase import Client, create_client

from .cards_service import get_personalized_cards_for_user, DEFAULT_FEED_TAGS
from .profile_service import (
    build_and_save_structured_profile,
    get_or_build_profile_for_feed,
)

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized in webapp_backend")
else:
    logger.warning("Supabase credentials are not set; backend will run without DB")

# ===== FastAPI и статика =====

app = FastAPI(title="EYYE WebApp Backend", version="0.1.0")

# WebApp крутится в том же origin, так что CORS почти не нужен,
# но оставим минимальные настройки.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # при желании можно зажать до конкретного домена
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ROOT_DIR = Path(__file__).resolve().parents[2]  # .../eyye-tg-bot/
WEBAPP_DIR = ROOT_DIR / "webapp"

if WEBAPP_DIR.exists():
    app.mount(
        "/webapp",
        StaticFiles(directory=str(WEBAPP_DIR), html=True),
        name="webapp",
    )
else:
    logger.warning("WEBAPP_DIR %s does not exist; static files not mounted", WEBAPP_DIR)


@app.get("/ping")
async def ping() -> Dict[str, str]:
    return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/")
async def index() -> FileResponse:
    """
    Отдаём index.html WebApp.
    Внутри Telegram WebApp будет открываться этот роут с ?tg_id=...
    """
    index_file = WEBAPP_DIR / "index.html"
    if not index_file.exists():
        raise HTTPException(status_code=500, detail="index.html not found")
    return FileResponse(str(index_file))


# ===== Модели запросов =====

class CityUpdate(BaseModel):
    tg_id: int
    city: str


class TopicsUpdate(BaseModel):
    tg_id: int
    topics: List[str]


# ===== API профиля =====

@app.post("/api/profile/city")
async def update_city(payload: CityUpdate) -> Dict[str, Any]:
    """
    Сохраняем/обновляем город пользователя в user_profiles.
    """
    if not supabase:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    data = {
        "user_id": payload.tg_id,
        "location_city": payload.city.strip() or None,
    }

    try:
        resp = (
            supabase.table("user_profiles")
            .upsert(data, on_conflict="user_id")
            .execute()
        )
        logger.info(
            "Upsert user_profiles city for user_id=%s: %s",
            payload.tg_id,
            getattr(resp, "data", None),
        )
    except Exception as e:
        logger.exception("Error updating city for user_id=%s", payload.tg_id)
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "ok"}


@app.post("/api/profile/topics")
async def update_topics(payload: TopicsUpdate) -> Dict[str, Any]:
    """
    Сохраняем выбранные темы пользователя в user_topic_weights.
    Параллельно в фоне запускаем построение structured_profile через OpenAI.
    """
    if not supabase:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    user_id = payload.tg_id
    topics = [t.strip() for t in payload.topics if t.strip()]

    try:
        # Сначала очищаем старые веса
        supabase.table("user_topic_weights").delete().eq("user_id", user_id).execute()

        if topics:
            rows = [
                {"user_id": user_id, "tag": tag, "weight": 1.0}
                for tag in topics
            ]
            supabase.table("user_topic_weights").insert(rows).execute()

        logger.info("Updated user_topic_weights for user_id=%s: %s", user_id, topics)
    except Exception:
        logger.exception("Error updating user_topic_weights for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail="Failed to update topics")

    # Вытащим город из user_profiles (если есть),
    # чтобы построить structured_profile с учётом города.
    city: Optional[str] = None
    try:
        resp = (
            supabase.table("user_profiles")
            .select("location_city")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
        data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
        if data:
            city = data[0].get("location_city")
    except Exception:
        logger.exception("Error loading city for structured_profile, user_id=%s", user_id)

    # В фоне строим structured_profile (через OpenAI или fallback).
    # Это не блокирует ответ фронту.
    asyncio.create_task(
        asyncio.to_thread(
            build_and_save_structured_profile,
            supabase,
            user_id,
            topics or DEFAULT_FEED_TAGS,
            "ru",
        )
    )

    return {"status": "ok"}


# ===== API ленты =====

@app.get("/api/feed")
async def api_feed(
    tg_id: int = Query(..., description="Telegram user id"),
    limit: int = Query(15, ge=1, le=50),
) -> JSONResponse:
    """
    Главный эндпоинт ленты для WebApp.

    Логика:
    1) Берём веса интересов пользователя (user_topic_weights).
    2) Из них и/или из DEFAULT_FEED_TAGS строим base_tags.
    3) Загружаем structured_profile (если есть) или fallback-профиль.
    4) cards_service:
       - берёт кандидатов из cards по тегам,
       - при нехватке генерирует новые карточки через OpenAI,
       - ранжирует и возвращает TOP-N.
    """
    if not supabase:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    # 1) Извлекаем динамические веса интересов пользователя
    from .cards_service import get_user_topic_weights  # локальный импорт, чтобы избежать циклов

    topic_weights = get_user_topic_weights(supabase, tg_id)
    base_tags = list(topic_weights.keys()) or DEFAULT_FEED_TAGS

    # 2) Строим профиль для ленты (structured_profile или fallback)
    profile_dict = get_or_build_profile_for_feed(supabase, tg_id, base_tags)

    # 3) Берём персональные карточки
    cards = await asyncio.to_thread(
        get_personalized_cards_for_user,
        supabase,
        tg_id,
        profile_dict,
        "ru",
        limit,
    )

    if not cards:
        return JSONResponse(
            {
                "items": [],
                "debug": {
                    "reason": "no_cards",
                    "base_tags": base_tags,
                },
            }
        )

    # Отдаём только те поля, которые нужны фронту (можно расширить по мере надобности).
    items: List[Dict[str, Any]] = []
    for card in cards:
        items.append(
            {
                "id": card.get("id"),
                "title": card.get("title"),
                "body": card.get("body"),
                "tags": card.get("tags") or [],
                "category": card.get("category"),
                "importance_score": card.get("importance_score"),
                "created_at": card.get("created_at"),
                "source_type": card.get("source_type"),
                "source_ref": card.get("source_ref"),
            }
        )

    return JSONResponse(
        {
            "items": items,
            "debug": {
                "base_tags": base_tags,
                "used_structured_profile": bool(profile_dict),
            },
        }
    )

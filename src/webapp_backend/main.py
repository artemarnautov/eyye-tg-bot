# file: src/webapp_backend/main.py
import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from supabase import Client, create_client

# ==========================
# Инициализация окружения
# ==========================

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        # Если что-то пойдёт не так — просто логируем и работаем без персонализации
        logging.exception("Failed to init Supabase client in webapp_backend: %s", e)
        supabase = None
else:
    logging.warning("SUPABASE_URL or SUPABASE_KEY is not set for webapp_backend")

# ==========================
# Пути к фронту
# ==========================

BASE_DIR = Path(__file__).resolve().parents[2]  # /root/eyye-tg-bot/src/webapp_backend/main.py -> /root/eyye-tg-bot
WEBAPP_DIR = BASE_DIR / "webapp"

# ==========================
# FastAPI приложение
# ==========================

app = FastAPI(title="EYYE WebApp Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # на проде можно сузить
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Раздаём статику (JS/CSS)
app.mount("/webapp", StaticFiles(directory=str(WEBAPP_DIR)), name="webapp")


# ==========================
# Вспомогательные функции
# ==========================

def _load_user_profile(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Синхронная версия load_user_profile из бота.
    Нужна, чтобы понять, какие теги интересов есть у пользователя.
    """
    if not supabase:
        return None

    try:
        resp = (
            supabase.table("user_profiles")
            .select("*")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logging.exception("webapp_backend: error loading user_profile for user_id=%s", user_id)
        return None

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return None

    row = data[0]
    if not isinstance(row, dict):
        return None
    return row


def _extract_interest_tags_from_profile(profile: Dict[str, Any]) -> List[str]:
    """
    Берём interests_as_tags из structured_profile, если он есть.
    Если structured_profile — строка, парсим как JSON.
    """
    structured = profile.get("structured_profile")

    structured_obj: Optional[Dict[str, Any]]
    if structured is None:
        structured_obj = None
    elif isinstance(structured, str):
        try:
            structured_obj = json.loads(structured)
        except Exception:
            logging.exception(
                "webapp_backend: failed to parse structured_profile JSON for user_id=%s",
                profile.get("user_id"),
            )
            structured_obj = None
    elif isinstance(structured, dict):
        structured_obj = structured
    else:
        structured_obj = None

    tags: List[str] = []

    if structured_obj and isinstance(structured_obj, dict):
        raw_tags = structured_obj.get("interests_as_tags") or []
        if isinstance(raw_tags, list):
            for t in raw_tags:
                s = str(t).strip()
                if s:
                    tags.append(s)

    # Если в structured_profile тегов нет, можно потом добавить fallback из raw_interests
    # (пока не делаем, чтобы не плодить дублирующую логику).
    # Убираем дубликаты, сохраняя порядок
    return list(dict.fromkeys(tags))


def _fetch_cards_for_tags(tags: List[str], limit: int) -> List[Dict[str, Any]]:
    """
    Простая выборка карточек из таблицы cards.
    Если есть теги — берём карточки, у которых tags пересекаются с нашими тегами.
    Если тегов нет или Supabase недоступен — возвращаем просто свежие карточки.
    """
    if not supabase:
        logging.warning("webapp_backend: Supabase is not configured, /api/feed will return empty list")
        return []

    try:
        query = (
            supabase.table("cards")
            .select("id, title, body, tags, importance_score, created_at")
            .eq("is_active", True)
        )

        if tags:
            # overlaps(tags, tags_array) -> оператор && в Postgres
            query = query.overlaps("tags", tags)

        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logging.exception("webapp_backend: error fetching cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return []

    # Нормализуем поля, чтобы фронту было проще
    items: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        items.append(
            {
                "id": row.get("id"),
                "title": row.get("title") or "",
                "body": row.get("body") or "",
                "tags": row.get("tags") or [],
                "importance_score": row.get("importance_score") or 1.0,
                "created_at": row.get("created_at"),
            }
        )
    return items


# ==========================
# Эндпоинты
# ==========================

@app.get("/ping")
def ping() -> Dict[str, Any]:
    return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/", response_class=FileResponse)
def index() -> FileResponse:
    """
    Отдаём index.html для WebApp.
    """
    index_path = WEBAPP_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=500, detail="webapp/index.html not found")
    return FileResponse(str(index_path))


@app.get("/api/feed")
def api_feed(
    tg_id: int = Query(..., description="Telegram user id"),
    limit: int = Query(20, ge=1, le=50),
) -> JSONResponse:
    """
    Минимальный фид для WebApp:
    - по tg_id ищем user_profiles;
    - вытаскиваем interests_as_tags из structured_profile;
    - по этим тегам достаём карточки из таблицы cards;
    - если тегов нет — берём просто свежие карточки.
    Пока без динамических весов и без генерации через OpenAI.
    """
    if tg_id <= 0:
        raise HTTPException(status_code=400, detail="tg_id must be positive integer")

    tags: List[str] = []

    profile = _load_user_profile(tg_id)
    if profile:
        tags = _extract_interest_tags_from_profile(profile)

    items = _fetch_cards_for_tags(tags, limit=limit)

    return JSONResponse({"items": items})

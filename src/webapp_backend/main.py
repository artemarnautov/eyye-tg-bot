# file: webapp_backend/main.py
import logging
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
from supabase import Client, create_client

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

logger = logging.getLogger("eyye.webapp_backend")
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized in webapp_backend")
    except Exception:
        logger.exception("Failed to initialize Supabase client in webapp_backend")
        supabase = None
else:
    logger.warning("SUPABASE_URL / SUPABASE_KEY are not set. API will run in degraded mode.")

# Базовые настройки ленты
DEFAULT_FEED_TAGS: List[str] = ["world_news", "business", "tech", "uk_students"]
DEFAULT_FEED_LIMIT: int = 20

# Пути к статике
BASE_DIR = Path(__file__).resolve().parents[2]  # /root/eyye-tg-bot
WEBAPP_DIR = BASE_DIR / "webapp"

app = FastAPI(title="EYYE WebApp Backend", version="0.1.0")

if WEBAPP_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(WEBAPP_DIR), html=False), name="static")
else:
    logger.warning("WEBAPP_DIR %s does not exist; static files will not be served", WEBAPP_DIR)


# ==========================
# Pydantic-модели
# ==========================

class CityUpdate(BaseModel):
    user_id: int
    city: Optional[str] = None


class TopicsUpdate(BaseModel):
    user_id: int
    tags: List[str]


# ==========================
# Вспомогательные функции
# ==========================

def _require_supabase() -> Client:
    if not supabase:
        raise HTTPException(status_code=500, detail="Supabase is not configured")
    return supabase


def _load_user_topic_tags(user_id: int) -> Dict[str, float]:
    client = _require_supabase()
    try:
        resp = (
            client.table("user_topic_weights")
            .select("tag, weight")
            .eq("user_id", user_id)
            .execute()
        )
    except Exception:
        logger.exception("Error loading user_topic_weights for user_id=%s", user_id)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return {}

    result: Dict[str, float] = {}
    for row in data:
        tag = row.get("tag")
        if not tag:
            continue
        try:
            weight = float(row.get("weight", 0.0))
        except (TypeError, ValueError):
            weight = 0.0
        if weight != 0.0:
            result[str(tag)] = weight
    return result


def _fetch_cards_for_tags(tags: List[str], limit: int) -> List[Dict[str, Any]]:
    client = _require_supabase()

    try:
        query = (
            client.table("cards")
            .select("id, title, body, tags, importance_score, created_at")
            .eq("is_active", True)
        )

        if tags:
            query = query.overlaps("tags", tags)

        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    return data or []


# ==========================
# Роуты
# ==========================

@app.get("/ping")
async def ping() -> Dict[str, Any]:
    return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    """
    Отдаём index.html WebApp.
    """
    index_path = WEBAPP_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    try:
        html = index_path.read_text(encoding="utf-8")
    except Exception:
        logger.exception("Failed to read index.html from %s", index_path)
        raise HTTPException(status_code=500, detail="Failed to load WebApp")
    return HTMLResponse(content=html)


@app.post("/api/profile/city")
async def update_city(payload: CityUpdate) -> JSONResponse:
    """
    Обновляем город пользователя в user_profiles.
    Если записи нет — создаём.
    """
    client = _require_supabase()

    data: Dict[str, Any] = {
        "user_id": payload.user_id,
        "location_city": payload.city or None,
    }

    try:
        resp = (
            client.table("user_profiles")
            .upsert(data, on_conflict="user_id")
            .execute()
        )
        logger.info("Upsert user_profiles city for user_id=%s: %s", payload.user_id, resp)
    except Exception:
        logger.exception("Failed to upsert user_profiles.city for user_id=%s", payload.user_id)
        raise HTTPException(status_code=500, detail="Failed to update city")

    return JSONResponse({"status": "ok"})


@app.post("/api/profile/topics")
async def update_topics(payload: TopicsUpdate) -> JSONResponse:
    """
    Обновляем веса тем пользователя:
    - удаляем старые записи в user_topic_weights
    - создаём новые (weight = 1.0) для переданных тегов
    """
    client = _require_supabase()

    tags = [t.strip() for t in payload.tags if t.strip()]
    try:
        # Удаляем старые веса
        resp_del = (
            client.table("user_topic_weights")
            .delete()
            .eq("user_id", payload.user_id)
            .execute()
        )
        logger.info(
            "Deleted old user_topic_weights for user_id=%s: %s",
            payload.user_id,
            resp_del,
        )

        if tags:
            rows = [
                {"user_id": payload.user_id, "tag": tag, "weight": 1.0}
                for tag in tags
            ]
            resp_ins = client.table("user_topic_weights").insert(rows).execute()
            logger.info(
                "Inserted %d user_topic_weights rows for user_id=%s: %s",
                len(rows),
                payload.user_id,
                resp_ins,
            )
    except Exception:
        logger.exception("Failed to update user_topic_weights for user_id=%s", payload.user_id)
        raise HTTPException(status_code=500, detail="Failed to update topics")

    return JSONResponse({"status": "ok"})


@app.get("/api/feed")
async def get_feed(
    tg_id: int = Query(..., description="Telegram user id"),
    limit: int = Query(DEFAULT_FEED_LIMIT, ge=1, le=100),
) -> JSONResponse:
    """
    Главный эндпоинт ленты для WebApp.

    Логика:
    - читаем теги интересов пользователя из user_topic_weights;
    - если тегов нет — используем DEFAULT_FEED_TAGS;
    - достаём карточки из cards с пересечением по tags;
    - сортируем по importance_score + перекрытию тегов;
    - возвращаем JSON с массивом items.
    """
    _ = _require_supabase()

    topic_weights = _load_user_topic_tags(tg_id)
    tags: List[str] = list(topic_weights.keys())

    if not tags:
        tags = DEFAULT_FEED_TAGS

    cards = _fetch_cards_for_tags(tags, limit=limit * 2)

    ranked: List[Dict[str, Any]] = []
    for card in cards:
        try:
            importance = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            importance = 1.0

        card_tags = card.get("tags") or []
        if not isinstance(card_tags, list):
            card_tags = []

        overlap = len(set(card_tags) & set(tags))
        score = importance + 0.3 * overlap

        card_copy = dict(card)
        card_copy["_score"] = score
        ranked.append(card_copy)

    ranked.sort(key=lambda c: c.get("_score", 0.0), reverse=True)
    for c in ranked:
        c.pop("_score", None)

    items = ranked[:limit]

    return JSONResponse({"items": items})

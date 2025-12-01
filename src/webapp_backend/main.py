# file: src/webapp_backend/main.py
import os
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from supabase import Client, create_client

# ==========================
# Инициализация окружения
# ==========================

load_dotenv()

logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase client initialized in webapp_backend")
    except Exception as e:
        # Если что-то пойдёт не так — просто логируем и работаем без персонализации
        logging.exception("Failed to init Supabase client in webapp_backend: %s", e)
        supabase = None
else:
    logging.warning("SUPABASE_URL or SUPABASE_KEY is not set for webapp_backend")

# ==========================
# Константы фида и персонализации
# ==========================

# Дефолтные теги, если у пользователя ещё нет профиля/интересов
DEFAULT_FEED_TAGS: List[str] = ["top_news", "uk_students", "world_news"]

# На сколько часов назад смотрим новости (ограничение свежести)
FEED_MAX_AGE_HOURS: int = 48

# Сколько кандидатов вытаскиваем из БД на один запрос фида
CANDIDATE_MULTIPLIER: int = 3
MAX_CANDIDATES: int = 100

# Ограничения весов для user_topic_weights
MAX_TOPIC_WEIGHT: float = 5.0
MIN_TOPIC_WEIGHT: float = 0.0

# Насколько разные типы событий увеличивают вес тегов
EVENT_WEIGHT_DELTAS: Dict[str, float] = {
    "card_view": 0.1,
    "card_read_full": 0.3,
    "card_click": 0.3,
    "card_like": 0.5,
}

# ==========================
# Пути к фронту
# ==========================

BASE_DIR = Path(__file__).resolve().parents[2]  # /root/eyye-tg-bot
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
# Pydantic-модели для API
# ==========================


class OnboardingRequest(BaseModel):
    tg_id: int
    location_city: Optional[str] = None
    location_country: Optional[str] = None
    raw_interests_text: Optional[str] = None
    selected_topics: Optional[List[str]] = None


class TelemetryEvent(BaseModel):
    tg_id: int
    event_type: str
    card_id: Any
    timestamp: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


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

    Для MVP считаем, что WebApp онбординг может сам положить простую
    structured_profile с полем interests_as_tags.
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

    # Убираем дубликаты, сохраняя порядок
    return list(dict.fromkeys(tags))


def _load_user_topic_weights(user_id: int) -> Dict[str, float]:
    """
    Загружаем веса по тегам для пользователя из таблицы user_topic_weights.
    Формат: {tag: weight}
    """
    if not supabase:
        return {}

    try:
        resp = (
            supabase.table("user_topic_weights")
            .select("tag, weight")
            .eq("user_id", user_id)
            .execute()
        )
    except Exception:
        logging.exception("webapp_backend: error loading user_topic_weights for user_id=%s", user_id)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return {}

    weights: Dict[str, float] = {}
    for row in data:
        if not isinstance(row, dict):
            continue
        tag = row.get("tag")
        if not isinstance(tag, str):
            continue
        tag_clean = tag.strip()
        if not tag_clean:
            continue
        try:
            w = float(row.get("weight") or 0.0)
        except (TypeError, ValueError):
            w = 0.0
        weights[tag_clean] = w

    return weights


def _fetch_cards_for_tags(
    tags: List[str],
    candidate_limit: int,
    min_created_at_iso: Optional[str] = None,
) -> List[Dict[str, Any]]:
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

        if min_created_at_iso:
            query = query.gte("created_at", min_created_at_iso)

        resp = query.order("created_at", desc=True).limit(candidate_limit).execute()
    except Exception:
        logging.exception("webapp_backend: error fetching cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return []

    # Нормализуем поля, чтобы фронту и скорингу было проще
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


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    interest_tags: List[str],
    topic_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Считаем простой скор для карточек:
    - базовый importance_score;
    - бонус за совпадение тегов с интересами (interests_as_tags);
    - бонус за веса user_topic_weights;
    - множитель за свежесть (чем старее карточка, тем меньше).
    """
    now = datetime.now(timezone.utc)
    interest_set = set(interest_tags or [])

    for card in cards:
        try:
            base = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            base = 1.0

        card_tags_raw = card.get("tags") or []
        card_tags: List[str] = []
        for t in card_tags_raw:
            s = str(t).strip()
            if s:
                card_tags.append(s)

        # Бонус за пересечение с интересами
        overlap = interest_set.intersection(card_tags)
        interest_bonus = 0.5 * len(overlap)

        # Бонус по user_topic_weights
        topic_bonus = 0.0
        for t in card_tags:
            w = topic_weights.get(t)
            if w:
                topic_bonus += 0.2 * float(w)

        # Множитель за свежесть (0.1–1.2)
        freshness_factor = 1.0
        created_raw = card.get("created_at")
        if created_raw:
            try:
                created_dt = datetime.fromisoformat(str(created_raw).replace("Z", "+00:00"))
                age_hours = (now - created_dt).total_seconds() / 3600.0
                if age_hours >= 0:
                    freshness_factor = max(
                        0.1,
                        min(1.2, 1.2 - age_hours / FEED_MAX_AGE_HOURS),
                    )
            except Exception:
                # Если не смогли распарсить дату — не рушим скоринг
                pass

        score = (base + interest_bonus + topic_bonus) * freshness_factor
        card["score"] = score

    # Сортируем по скору по убыванию
    cards.sort(key=lambda c: c.get("score", 0.0), reverse=True)
    return cards


def _build_tags_from_onboarding(
    raw_text: Optional[str],
    selected_topics: Optional[List[str]],
) -> List[str]:
    """
    Строим простой список тегов из выбранных топиков и (чуть-чуть) из текста.
    Это fallback-профиль, чтобы фид начал работать сразу, без OpenAI.
    """
    tags: List[str] = []

    # 1) Из выбранных топиков делаем слаг: "Business News" -> "business_news"
    if selected_topics:
        for topic in selected_topics:
            if not topic:
                continue
            slug = (
                str(topic)
                .strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )
            if slug and slug not in tags:
                tags.append(slug)

    # 2) Минимальные эвристики по тексту (совсем простые, чтобы не усложнять)
    if raw_text:
        lower = raw_text.lower()

        def _add(tag: str) -> None:
            if tag not in tags:
                tags.append(tag)

        if any(word in lower for word in ["football", "soccer", "premier league"]):
            _add("sports_football")
        if any(word in lower for word in ["nba", "basketball"]):
            _add("sports_basketball")
        if any(word in lower for word in ["startup", "business"]):
            _add("business")
        if any(word in lower for word in ["crypto", "bitcoin", "ethereum"]):
            _add("crypto")
        if any(word in lower for word in ["ai", "artificial intelligence", "machine learning"]):
            _add("ai")
        if any(word in lower for word in ["university", "uni", "student", "campus"]):
            _add("students_life")

    # Если совсем ничего не нашли — подставляем дефолтные теги
    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    return list(dict.fromkeys(tags))


def _get_card_tags(card_id: Any) -> List[str]:
    """
    Получаем теги карточки по её id — нужно для телеметрии.
    """
    if not supabase:
        return []

    try:
        resp = (
            supabase.table("cards")
            .select("tags")
            .eq("id", card_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logging.exception("webapp_backend: error loading card tags for card_id=%s", card_id)
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return []

    row = data[0]
    if not isinstance(row, dict):
        return []

    raw_tags = row.get("tags") or []
    tags: List[str] = []
    for t in raw_tags:
        s = str(t).strip()
        if s:
            tags.append(s)
    return list(dict.fromkeys(tags))


def _increment_topic_weight(user_id: int, tag: str, delta: float) -> None:
    """
    Увеличиваем вес тега для пользователя в таблице user_topic_weights.
    При отсутствии строки — создаём; при наличии — обновляем.
    """
    if not supabase:
        return

    tag_clean = str(tag).strip()
    if not tag_clean:
        return

    try:
        # 1) Пытаемся найти существующую запись
        resp = (
            supabase.table("user_topic_weights")
            .select("id, weight")
            .eq("user_id", user_id)
            .eq("tag", tag_clean)
            .limit(1)
            .execute()
        )
    except Exception:
        logging.exception(
            "webapp_backend: error reading user_topic_weights for user_id=%s, tag=%s",
            user_id,
            tag_clean,
        )
        return

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    existing_row = data[0] if data else None

    # 2) Считаем новый вес
    if existing_row and isinstance(existing_row, dict):
        try:
            current = float(existing_row.get("weight") or 0.0)
        except (TypeError, ValueError):
            current = 0.0
        new_weight = current + delta
        new_weight = max(MIN_TOPIC_WEIGHT, min(MAX_TOPIC_WEIGHT, new_weight))

        try:
            supabase.table("user_topic_weights").update(
                {"weight": new_weight}
            ).eq("user_id", user_id).eq("tag", tag_clean).execute()
        except Exception:
            logging.exception(
                "webapp_backend: error updating user_topic_weights for user_id=%s, tag=%s",
                user_id,
                tag_clean,
            )
    else:
        # Нет записи — создаём новую
        new_weight = max(MIN_TOPIC_WEIGHT, min(MAX_TOPIC_WEIGHT, delta))
        row = {
            "user_id": user_id,
            "tag": tag_clean,
            "weight": new_weight,
        }
        try:
            supabase.table("user_topic_weights").insert(row).execute()
        except Exception:
            logging.exception(
                "webapp_backend: error inserting user_topic_weights for user_id=%s, tag=%s",
                user_id,
                tag_clean,
            )


def _log_user_event(event: TelemetryEvent) -> None:
    """
    Пишем сырое событие в user_events, если таблица есть.
    Если таблицы нет — просто молча игнорируем ошибки.
    """
    if not supabase:
        return

    payload = {
        "user_id": event.tg_id,
        "event_type": event.event_type,
        "card_id": event.card_id,
        "meta": event.meta or {},
        # Если фронт не прислал timestamp — используем серверное время
        "created_at": event.timestamp or datetime.now(timezone.utc).isoformat(),
    }

    try:
        supabase.table("user_events").insert(payload).execute()
    except Exception:
        # Логируем в debug, чтобы не заспамить ошибки, если таблицы нет
        logging.debug("webapp_backend: user_events insert failed (table may not exist)", exc_info=True)


def _process_telemetry_event(event: TelemetryEvent) -> None:
    """
    Обрабатываем событие:
    - логируем его в user_events (best-effort);
    - обновляем веса тегов в user_topic_weights.
    """
    _log_user_event(event)

    delta = EVENT_WEIGHT_DELTAS.get(event.event_type)
    if not delta:
        # Неизвестный тип события — просто залогировали, веса не трогаем
        return

    card_tags = _get_card_tags(event.card_id)
    if not card_tags:
        return

    for tag in card_tags:
        _increment_topic_weight(event.tg_id, tag, delta)


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
    Фид для WebApp:
    - по tg_id ищем user_profiles;
    - вытаскиваем interests_as_tags из structured_profile;
    - подгружаем user_topic_weights;
    - по этим тегам достаём карточки из таблицы cards за последние FEED_MAX_AGE_HOURS;
    - считаем скор для карточек;
    - возвращаем топ-N.
    """
    if tg_id <= 0:
        raise HTTPException(status_code=400, detail="tg_id must be positive integer")

    # 1) Загружаем профиль и интересы
    profile = _load_user_profile(tg_id)
    interest_tags: List[str] = []
    if profile:
        interest_tags = _extract_interest_tags_from_profile(profile)

    # Если тегов нет — используем дефолтные
    if not interest_tags:
        interest_tags = list(DEFAULT_FEED_TAGS)

    # 2) Загружаем динамические веса по тегам
    topic_weights = _load_user_topic_weights(tg_id)

    # 3) Вытаскиваем кандидатов из cards
    candidate_limit = min(limit * CANDIDATE_MULTIPLIER, MAX_CANDIDATES)
    min_created_at_iso: Optional[str] = None
    try:
        min_created_at_iso = (
            datetime.now(timezone.utc) - timedelta(hours=FEED_MAX_AGE_HOURS)
        ).isoformat()
    except Exception:
        min_created_at_iso = None

    raw_cards = _fetch_cards_for_tags(
        interest_tags,
        candidate_limit=candidate_limit,
        min_created_at_iso=min_created_at_iso,
    )

    # 4) Считаем скор и берём топ-N
    scored_cards = _score_cards_for_user(raw_cards, interest_tags, topic_weights)
    items = scored_cards[:limit]

    return JSONResponse({"items": items})


@app.post("/api/profile/onboarding")
def api_profile_onboarding(payload: OnboardingRequest) -> JSONResponse:
    """
    Онбординг профиля из WebApp.

    Делает простое:
    - строит fallback-теги из выбранных тем и текста;
    - формирует минимальный structured_profile с interests_as_tags;
    - upsert в user_profiles по user_id = tg_id.

    OpenAI пока НЕ дергаем — это можно будет добавить позже отдельным воркером.
    """
    if payload.tg_id <= 0:
        raise HTTPException(status_code=400, detail="tg_id must be positive integer")

    if not supabase:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    tags = _build_tags_from_onboarding(
        raw_text=payload.raw_interests_text,
        selected_topics=payload.selected_topics,
    )

    structured_profile = {
        "source": "webapp_onboarding_fallback_v1",
        "location_city": payload.location_city,
        "location_country": payload.location_country,
        "interests_as_tags": tags,
        "selected_topics": payload.selected_topics or [],
    }

    row = {
        "user_id": payload.tg_id,
        "raw_interests": (payload.raw_interests_text or "").strip(),
        "location_city": payload.location_city,
        "location_country": payload.location_country,
        "structured_profile": structured_profile,
    }

    # upsert по user_id
    try:
        try:
            supabase.table("user_profiles").upsert(row, on_conflict="user_id").execute()
        except TypeError:
            # Если версия supabase-py не поддерживает on_conflict
            supabase.table("user_profiles").upsert(row).execute()
    except Exception:
        logging.exception(
            "webapp_backend: error upserting user_profiles for user_id=%s",
            payload.tg_id,
        )
        raise HTTPException(status_code=500, detail="failed to save profile")

    return JSONResponse(
        {
            "status": "ok",
            "user_id": payload.tg_id,
            "interests_as_tags": tags,
        }
    )


@app.get("/api/profile")
def api_profile(
    tg_id: int = Query(..., description="Telegram user id"),
) -> JSONResponse:
    """
    Краткое состояние профиля для WebApp:
    - есть ли профиль;
    - базовая инфа (локация, сырые интересы);
    - interests_as_tags (эффективные теги).
    """
    if tg_id <= 0:
        raise HTTPException(status_code=400, detail="tg_id must be positive integer")

    profile = _load_user_profile(tg_id)
    if not profile:
        return JSONResponse({"exists": False})

    interest_tags = _extract_interest_tags_from_profile(profile)

    return JSONResponse(
        {
            "exists": True,
            "user_id": profile.get("user_id"),
            "location_city": profile.get("location_city"),
            "location_country": profile.get("location_country"),
            "raw_interests": profile.get("raw_interests") or "",
            "interests_as_tags": interest_tags,
        }
    )


@app.post("/api/telemetry")
def api_telemetry(event: TelemetryEvent) -> JSONResponse:
    """
    Принимаем телеметрию с фронта:
    - card_view / card_read_full / card_click / card_like
    - обновляем user_topic_weights + опционально логируем в user_events.
    """
    if event.tg_id <= 0:
        raise HTTPException(status_code=400, detail="tg_id must be positive integer")

    if not supabase:
        # На случай, если бэкенд поднят без Supabase — не ломаем фронт
        logging.warning("webapp_backend: telemetry received but Supabase is not configured")
        return JSONResponse(
            {
                "status": "ok",
                "detail": "telemetry ignored (Supabase is not configured)",
            }
        )

    try:
        _process_telemetry_event(event)
    except Exception:
        logging.exception("webapp_backend: error processing telemetry event")

    return JSONResponse({"status": "ok"})

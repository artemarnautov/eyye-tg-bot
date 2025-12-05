# file: src/webapp_backend/cards_service.py
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from supabase import Client

from .profile_service import get_interest_tags_for_user
from .openai_client import generate_cards_for_tags, is_configured as openai_is_configured

logger = logging.getLogger(__name__)

FEED_CARDS_LIMIT_DEFAULT = int(os.getenv("FEED_CARDS_LIMIT", "20"))
FEED_MAX_CARD_AGE_HOURS = int(os.getenv("FEED_MAX_CARD_AGE_HOURS", "48"))

LLM_CARD_GENERATION_ENABLED = (
    os.getenv("LLM_CARD_GENERATION_ENABLED", "true").lower() in ("1", "true", "yes")
)

DEFAULT_FEED_TAGS: List[str] = ["world_news", "business", "tech", "uk_students"]

# Дефолтный источник только для чисто LLM-карточек,
# когда у нас нет реального канала/СМИ.
DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")


def _fetch_candidate_cards(
    supabase: Client,
    tags: List[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Берём кандидатов из таблицы cards:
    - только is_active = true
    - только достаточно свежие (created_at >= now - FEED_MAX_CARD_AGE_HOURS)
    - если есть теги, используем overlaps(tags, tags_array).
    """
    now = datetime.now(timezone.utc)
    min_created_at = now - timedelta(hours=FEED_MAX_CARD_AGE_HOURS)
    min_created_at_str = min_created_at.isoformat()

    try:
        query = (
            supabase.table("cards")
            .select(
                "id,source_type,source_ref,title,body,tags,category,"
                "language,importance_score,created_at,is_active,meta"
            )
            .eq("is_active", True)
            .gte("created_at", min_created_at_str)
        )

        if tags:
            query = query.overlaps("tags", tags)

        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    return data or []


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
) -> List[Dict[str, Any]]:
    """
    Простейший скор для карточек:
    importance_score + бонус за совпадение тегов + бонус за свежесть.
    """
    now = datetime.now(timezone.utc)
    base_tag_set = set(base_tags)

    scored: List[Tuple[float, Dict[str, Any]]] = []

    for card in cards:
        card_tags = card.get("tags") or []
        if not isinstance(card_tags, list):
            card_tags = []

        try:
            importance = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            importance = 1.0

        profile_bonus = 0.0
        for t in card_tags:
            if t in base_tag_set:
                profile_bonus += 0.3

        recency_bonus = 0.0
        created_at = card.get("created_at")
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age_hours = (now - dt).total_seconds() / 3600.0
                if age_hours < FEED_MAX_CARD_AGE_HOURS:
                    recency_bonus = (FEED_MAX_CARD_AGE_HOURS - age_hours) / FEED_MAX_CARD_AGE_HOURS
            except Exception:
                pass

        score = importance + profile_bonus + recency_bonus
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]


def _insert_cards_into_db(
    supabase: Client,
    cards: List[Dict[str, Any]],
    *,
    language: str = "ru",
    source_type: str = "llm",
    fallback_source_name: str | None = None,
    source_ref: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Вставляем сгенерированные/переформатированные карточки в таблицу cards.

    Приоритет источника:
    1) c["source_name"] / c["source"] / c["channel_name"], если модель вернула.
    2) fallback_source_name (например, название телеграм-канала, из которого мы спарсили пост).
    3) DEFAULT_SOURCE_NAME ("EYYE • AI-подборка") — только если нет реального источника.

    language / source_type / source_ref:
    - language: язык карточки ("ru", "en", ...)
    - source_type: "telegram", "rss", "llm" и т.п.
    - source_ref: например, ссылка или message_id канала.
    """
    if not cards:
        return []

    payload: List[Dict[str, Any]] = []
    for c in cards:
        title = (c.get("title") or "").strip()
        body = (c.get("body") or "").strip()
        tags = c.get("tags") or []
        if not isinstance(tags, list):
            tags = [str(tags)]

        if not title or not body:
            continue

        try:
            importance = float(c.get("importance_score", 1.0))
        except (TypeError, ValueError):
            importance = 1.0

        # 1) Пытаемся взять источник из ответа модели / препроцессора
        raw_source_name = (
            c.get("source_name")
            or c.get("source")
            or c.get("channel_name")
            or c.get("channel_title")
        )

        # 2) Если модель ничего не дала — используем fallback_source_name
        if not raw_source_name and fallback_source_name:
            raw_source_name = fallback_source_name

        # 3) Если вообще ничего нет — используем дефолтный источник для чистого LLM
        if not raw_source_name:
            raw_source_name = DEFAULT_SOURCE_NAME

        source_name = str(raw_source_name).strip()

        # Если у самой карточки есть source_ref/url — используем его как референс
        card_source_ref = c.get("source_ref") or c.get("url") or c.get("link")
        final_source_ref = source_ref or card_source_ref

        meta: Dict[str, Any] = {
            "source_name": source_name,
        }

        payload.append(
            {
                "title": title,
                "body": body,
                "tags": [str(t).strip() for t in tags if t],
                "importance_score": importance,
                "is_active": True,
                "source_type": source_type,
                "source_ref": final_source_ref,
                "language": language,
                "meta": meta,
            }
        )

    if not payload:
        return []

    try:
        resp = supabase.table("cards").insert(payload).execute()
    except Exception:
        logger.exception("Error inserting generated cards into Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    data = data or []
    logger.info("Inserted %d generated cards into DB", len(data))
    return data


def build_feed_for_user(
    supabase: Client | None,
    user_id: int,
    limit: int | None = None,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Основная точка входа для /api/feed.

    Возвращает:
    - items: список карточек для пользователя
    - debug: отладочная информация (reason, base_tags и т.п.)

    offset — сколько карточек пропустить (для "следующих" порций ленты).
    """
    debug: Dict[str, Any] = {}
    debug["offset"] = offset

    if supabase is None:
        debug["reason"] = "no_supabase"
        debug["base_tags"] = []
        return [], debug

    if offset < 0:
        offset = 0

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(limit, 1), 50)

    # 1. Теги интересов пользователя
    base_tags = get_interest_tags_for_user(supabase, user_id)
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS
    debug["base_tags"] = base_tags

    # 2. Пробуем взять карточки из БД
    # Берём с запасом: (limit + offset) * 3, чтоб хватило на пропуск offset
    fetch_limit = (limit + offset) * 3

    candidates = _fetch_candidate_cards(
        supabase, base_tags, limit=fetch_limit
    )

    if candidates:
        ranked = _score_cards_for_user(candidates, base_tags)
        total = len(ranked)

        start = min(offset, total)
        end = min(start + limit, total)
        page = ranked[start:end]

        debug["reason"] = "cards_from_db"
        debug["candidates"] = total
        debug["returned"] = len(page)
        return page, debug

    # 3. В БД пусто — пробуем сгенерировать через OpenAI
    if LLM_CARD_GENERATION_ENABLED and openai_is_configured():
        # Берём побольше, с учётом offset, на всякий случай
        need_count = max((limit + offset) * 2, 20)
        logger.info(
            "No cards in DB for user_id=%s. Generating ~%d cards via OpenAI.",
            user_id,
            need_count,
        )
        generated = generate_cards_for_tags(
            tags=base_tags,
            language="ru",
            count=need_count,
        )
        if generated:
            # Для чисто LLM-карточек у нас пока нет реального канала,
            # поэтому fallback_source_name не передаём — дальше сработает DEFAULT_SOURCE_NAME.
            inserted = _insert_cards_into_db(
                supabase,
                generated,
                language="ru",
                source_type="llm",
            )
            if inserted:
                ranked = _score_cards_for_user(inserted, base_tags)
                total = len(ranked)

                start = min(offset, total)
                end = min(start + limit, total)
                page = ranked[start:end]

                debug["reason"] = "generated_via_openai"
                debug["generated"] = total
                debug["returned"] = len(page)
                return page, debug

    # 4. Не удалось ничего добыть
    debug["reason"] = "no_cards"
    debug["returned"] = 0
    return [], debug



def build_feed_for_user_paginated(
    supabase: Client | None,
    user_id: int,
    limit: int | None = None,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Обёртка над build_feed_for_user с поддержкой пагинации по offset.

    Возвращает:
    - items: карточки для текущей "страницы" (offset + limit)
    - debug: отладочная информация (reason, base_tags, offset, limit, has_more)
    - cursor: метаданные пагинации (offset, next_offset, has_more)
    """
    # Базовые защиты от странных значений
    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(limit, 1), 50)
    offset = max(0, int(offset))

    # Сколько карточек попросить у базовой функции:
    # берём offset + limit, но не больше 50 (build_feed_for_user всё равно ограничивает)
    internal_limit = min(limit + offset, 50)

    # Используем уже существующую логику персонализации/ранкинга
    all_items, base_debug = build_feed_for_user(
        supabase=supabase,
        user_id=user_id,
        limit=internal_limit,
    )

    # Берём только нужный "срез"
    page_items = all_items[offset: offset + limit]

    # Проверяем, есть ли ещё карточки дальше
    has_more = len(all_items) > offset + len(page_items)
    next_offset = offset + len(page_items) if has_more else None

    cursor: Dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
        "has_more": has_more,
    }

    debug: Dict[str, Any] = {
        **(base_debug or {}),
        "offset": offset,
        "limit": limit,
        "has_more": has_more,
    }

    return page_items, debug, cursor

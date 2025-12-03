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
            .select("id,title,body,tags,importance_score,created_at")
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
) -> List[Dict[str, Any]]:
    """
    Вставляем сгенерированные карточки в таблицу cards.
    Отправляем только очевидные поля, чтобы не упереться в несовпадение схемы.
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

        payload.append(
            {
                "title": title,
                "body": body,
                "tags": [str(t).strip() for t in tags if t],
                "importance_score": importance,
                "is_active": True,
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
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Основная точка входа для /api/feed.

    Возвращает:
    - items: список карточек для пользователя
    - debug: отладочная информация (reason, base_tags и т.п.)
    """
    debug: Dict[str, Any] = {}

    if supabase is None:
        debug["reason"] = "no_supabase"
        debug["base_tags"] = []
        return [], debug

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(limit, 1), 50)

    # 1. Теги интересов пользователя
    base_tags = get_interest_tags_for_user(supabase, user_id)
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS
    debug["base_tags"] = base_tags

    # 2. Пробуем взять карточки из БД
    candidates = _fetch_candidate_cards(
        supabase, base_tags, limit=limit * 3
    )

    if candidates:
        ranked = _score_cards_for_user(candidates, base_tags)
        debug["reason"] = "cards_from_db"
        debug["candidates"] = len(candidates)
        return ranked[:limit], debug

    # 3. В БД пусто — пробуем сгенерировать через OpenAI
    if LLM_CARD_GENERATION_ENABLED and openai_is_configured():
        need_count = max(limit * 2, 20)
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
            inserted = _insert_cards_into_db(supabase, generated)
            if inserted:
                ranked = _score_cards_for_user(inserted, base_tags)
                debug["reason"] = "generated_via_openai"
                debug["generated"] = len(inserted)
                return ranked[:limit], debug

    # 4. Не удалось ничего добыть
    debug["reason"] = "no_cards"
    return [], debug

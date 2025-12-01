# file: src/webapp_backend/cards_service.py
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import Client

from .openai_client import call_openai_chat

logger = logging.getLogger(__name__)

FEED_CARDS_LIMIT = int(os.getenv("FEED_CARDS_LIMIT", "15"))
FEED_MAX_CARD_AGE_HOURS = float(os.getenv("FEED_MAX_CARD_AGE_HOURS", "48"))
FEED_OPENAI_COOLDOWN_SECONDS = int(os.getenv("FEED_OPENAI_COOLDOWN_SECONDS", "60"))

DEFAULT_FEED_TAGS: List[str] = [
    "world_news",
    "business",
    "tech",
    "uk_students",
]

_last_feed_openai_call: Dict[int, datetime] = {}


def _is_allowed_feed_openai_call(user_id: int) -> bool:
    """
    Простейший rate-limit для генерации НОВЫХ карточек через OpenAI.
    """
    if FEED_OPENAI_COOLDOWN_SECONDS <= 0:
        return True

    now = datetime.now(timezone.utc)
    last = _last_feed_openai_call.get(user_id)
    if not last:
        _last_feed_openai_call[user_id] = now
        return True

    delta = (now - last).total_seconds()
    if delta >= FEED_OPENAI_COOLDOWN_SECONDS:
        _last_feed_openai_call[user_id] = now
        return True

    return False


def get_user_topic_weights(supabase: Optional[Client], user_id: int) -> Dict[str, float]:
    """
    Читаем user_topic_weights и возвращаем {tag: weight}.
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
        logger.exception("Error loading user_topic_weights for user_id=%s", user_id)
        return {}

    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
    result: Dict[str, float] = {}
    for row in data:
        tag = row.get("tag")
        if not tag:
            continue
        try:
            w = float(row.get("weight", 0.0))
        except (TypeError, ValueError):
            w = 0.0
        if w != 0.0:
            result[str(tag)] = w
    return result


def _extract_interest_tags_from_profile(profile_dict: Dict[str, Any]) -> List[str]:
    """
    Берём interests_as_tags из structured_profile / fallback-профиля.
    """
    tags = profile_dict.get("interests_as_tags") or []
    if not isinstance(tags, list):
        tags = []
    normalized: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s:
            normalized.append(s)
    return list(dict.fromkeys(normalized))


def fetch_candidate_cards(
    supabase: Optional[Client],
    tags: List[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """
    Кандидаты из таблицы cards:
    - если есть теги — берём карточки, у которых tags пересекаются с нашими тегами;
    - если тегов нет — свежие карточки по created_at.
    """
    if not supabase:
        logger.warning("Supabase is not configured, fetch_candidate_cards -> []")
        return []

    try:
        query = supabase.table("cards").select("*").eq("is_active", True)

        if tags:
            query = query.overlaps("tags", tags)

        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
    return data


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
    topic_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Скоринг карточек:
    - importance_score,
    - совпадение по тегам профиля,
    - динамические веса,
    - свежесть.
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

        dyn_bonus = 0.0
        for t in card_tags:
            dyn_bonus += topic_weights.get(t, 0.0)

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

        score = importance + profile_bonus + dyn_bonus + recency_bonus
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]


# -------- Работа с "кривым" JSON от OpenAI (по мотивам старого бота) --------

CARD_OBJECT_RE = re.compile(
    r'\{\s*"id"\s*:\s*"(?P<id>[^"]+)"(?P<body>.*?)\}',
    re.DOTALL,
)


def _parse_openai_cards_from_text(content: str) -> List[Dict[str, Any]]:
    if not content:
        return []

    cards: List[Dict[str, Any]] = []

    def _extract_str(block: str, field: str) -> Optional[str]:
        m = re.search(rf'"{field}"\s*:\s*"([^"]*)"', block)
        if m:
            return m.group(1).strip() or None
        return None

    def _extract_float(block: str, field: str, default: float = 1.0) -> float:
        m = re.search(rf'"{field}"\s*:\s*([0-9]+(\.[0-9]+)?)', block)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return default
        return default

    for idx, m in enumerate(CARD_OBJECT_RE.finditer(content), start=1):
        block = m.group(0)

        card_id = m.group("id") or f"item_{idx}"

        title = _extract_str(block, "title") or "Новость для тебя"
        summary = _extract_str(block, "summary") or ""
        topic = _extract_str(block, "topic")
        tag = _extract_str(block, "tag")
        importance = _extract_float(block, "importance", 1.0)

        if not title and not summary:
            continue

        cards.append(
            {
                "id": card_id,
                "title": title,
                "summary": summary,
                "topic": topic,
                "tag": tag,
                "importance": importance,
            }
        )

    return cards


def _generate_cards_for_tags_via_openai_sync(
    tags: List[str],
    language: str,
    count: int,
) -> List[Dict[str, Any]]:
    """
    Генерация новых карточек через OpenAI.
    Сейчас это «искусственные» карточки, но архитектурно
    это то же звено пайплайна, что будет переписывать
    реальные новости из Telegram / Wikipedia.
    """
    if not tags:
        tags = DEFAULT_FEED_TAGS

    system_prompt = (
        "Ты – движок новостной ленты EYYE.\n"
        "Твоя задача – сгенерировать короткие новостные карточки в одном стиле.\n"
        "Каждая карточка: заголовок и 2–4 абзаца текста.\n"
        "Пиши на языке, указанном в параметрах (ru или en).\n"
        "Отвечай строго валидным JSON без лишнего текста."
    )

    user_payload = {
        "language": language,
        "count": count,
        "tags": tags,
        "requirements": [
            "Карточки должны быть интересными и понятными.",
            "Не выдумывай факты про конкретных людей, лучше обобщай тенденции.",
            "Избегай кликбейта, но делай заголовки цепляющими.",
        ],
        "output_format": {
            "cards": [
                {
                    "title": "string",
                    "body": "string",
                    "tags": ["string"],
                    "category": "string",
                    "importance_score": 1.0,
                }
            ]
        },
    }

    payload: Dict[str, Any] = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        "max_output_tokens": 1200,
        "temperature": 0.7,
        "response_format": {"type": "json_object"},
    }

    started = time.monotonic()
    resp_json = call_openai_chat(payload)
    elapsed = time.monotonic() - started
    logger.info("OpenAI card generation call finished in %.2fs", elapsed)

    if not resp_json:
        return []

    choices = resp_json.get("choices")
    if not isinstance(choices, list) or not choices:
        logger.error("No choices in OpenAI card generation response")
        return []

    message = choices[0].get("message") or {}
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        logger.error("Empty content in OpenAI cards response")
        return []

    logger.debug(
        "OpenAI cards raw content (first 200 chars): %s",
        content[:200].replace("\n", " "),
    )

    raw_cards: List[Dict[str, Any]] = []

    try:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("Parsed card JSON is not an object")

        raw_cards = parsed.get("cards") or parsed.get("items")
        if not isinstance(raw_cards, list) or not raw_cards:
            raise ValueError("No 'cards' or 'items' list in card JSON")

    except json.JSONDecodeError:
        logger.exception(
            "Failed to parse OpenAI card generation response as JSON. "
            "Trying to salvage items from raw text."
        )
        raw_cards = _parse_openai_cards_from_text(content)
        if not raw_cards:
            logger.error("Salvage parser did not find any valid card items.")
            return []
        logger.warning(
            "Salvage parser recovered %d card items from broken JSON.",
            len(raw_cards),
        )
    except Exception:
        logger.exception("Failed to parse OpenAI card generation response")
        return []

    result: List[Dict[str, Any]] = []
    for c in raw_cards:
        if not isinstance(c, dict):
            continue

        title = str(c.get("title", "")).strip()
        body = str(c.get("body") or c.get("summary") or "").strip()
        if not title or not body:
            continue

        card_tags = c.get("tags") or c.get("tag") or tags
        if not isinstance(card_tags, list):
            card_tags = [str(card_tags)] if card_tags else tags

        category = c.get("category") or c.get("topic") or None
        try:
            importance = float(c.get("importance_score", c.get("importance", 1.0)))
        except (TypeError, ValueError):
            importance = 1.0

        result.append(
            {
                "source_type": "llm",
                "source_ref": None,
                "title": title,
                "body": body,
                "tags": [str(t).strip() for t in card_tags if t],
                "category": category,
                "language": language,
                "importance_score": importance,
                "meta": {
                    # в будущем сюда будут добавляться ссылки на Telegram / Wikipedia,
                    # которые мы переписываем.
                    "generated_for_tags": tags,
                },
            }
        )

    return result


def _insert_cards_into_db(
    supabase: Optional[Client],
    cards: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not cards:
        return []
    if not supabase:
        logger.warning("Supabase is not configured, skip inserting cards into DB")
        return []

    try:
        resp = supabase.table("cards").insert(cards).execute()
    except Exception:
        logger.exception("Error inserting cards into DB")
        return []

    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
    logger.info("Inserted %d cards into DB", len(data))
    return data


def get_personalized_cards_for_user(
    supabase: Optional[Client],
    user_id: int,
    profile_dict: Dict[str, Any],
    language: str = "ru",
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Основная функция для /api/feed:
    - берём интересы пользователя (interests_as_tags + user_topic_weights),
    - вытаскиваем кандидатов из cards,
    - при нехватке карт генерируем новые через OpenAI,
    - возвращаем TOP-N с учётом скоринга.
    """
    if not supabase:
        logger.warning("Supabase is not configured, cannot build personalized cards")
        return []

    if limit is None:
        limit = FEED_CARDS_LIMIT

    base_tags = _extract_interest_tags_from_profile(profile_dict)
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS

    topic_weights = get_user_topic_weights(supabase, user_id)

    candidates = fetch_candidate_cards(supabase, base_tags, limit=limit * 3)

    if len(candidates) < limit and _is_allowed_feed_openai_call(user_id):
        need = max(limit * 2 - len(candidates), limit)
        logger.info(
            "Not enough cards in DB for user_id=%s (have %d). Generating ~%d new cards via OpenAI.",
            user_id,
            len(candidates),
            need,
        )
        new_cards = _generate_cards_for_tags_via_openai_sync(base_tags, language, need)
        inserted = _insert_cards_into_db(supabase, new_cards)
        candidates.extend(inserted)

    if not candidates:
        return []

    ranked = _score_cards_for_user(candidates, base_tags, topic_weights)
    return ranked[:limit]

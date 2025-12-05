# file: src/webapp_backend/cards_service.py
import logging
import os
import re
from difflib import SequenceMatcher
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

# Максимальное количество карточек, которое мы вообще готовы тащить в ранжирование
FEED_MAX_FETCH_LIMIT = int(os.getenv("FEED_MAX_FETCH_LIMIT", "300"))

# Широкое окно по времени для "добора" карточек, если в пределах 48 часов мало
FEED_WIDE_AGE_HOURS = int(os.getenv("FEED_WIDE_AGE_HOURS", "240"))  # 10 дней

# Ограничения для "тикток-подобного" разведения
FEED_MAX_TOPIC_RUN = int(os.getenv("FEED_MAX_TOPIC_RUN", "3"))  # максимум подряд по одной теме
FEED_MAX_SOURCE_RUN = int(os.getenv("FEED_MAX_SOURCE_RUN", "2"))  # максимум подряд по одному источнику

# Порог схожести заголовков (0..1), выше которого считаем карточки дублями
FEED_TITLE_SIMILARITY_THRESHOLD = float(
    os.getenv("FEED_TITLE_SIMILARITY_THRESHOLD", "0.88")
)

# Максимальная длина нормализованного текста для грубой дедупликации по body
FEED_BODY_DEDUP_PREFIX_LEN = int(os.getenv("FEED_BODY_DEDUP_PREFIX_LEN", "200"))

# Дефолтный источник только для чисто LLM-карточек,
# когда у нас нет реального канала/СМИ.
DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")


# ==========================
# Вспомогательные функции: нормализация и анти-дубли
# ==========================


def _normalize_title_for_dedup(title: str) -> str:
    """
    Превращаем заголовок в канонический вид для сравнения:
    - нижний регистр
    - убираем всё, кроме букв/цифр/пробелов
    - сжимаем пробелы
    """
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t, flags=re.UNICODE)
    return t.strip()


def _normalize_body_for_dedup(body: str, max_len: int = FEED_BODY_DEDUP_PREFIX_LEN) -> str:
    """
    Грубая нормализация текста карточки для дедупликации:
    - нижний регистр
    - убираем пунктуацию
    - берём только первые max_len символов
    """
    if not body:
        return ""
    t = body.lower()
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t, flags=re.UNICODE)
    t = t.strip()
    if max_len > 0:
        t = t[:max_len]
    return t


def _titles_too_similar(a_norm: str, b_norm: str, threshold: float) -> bool:
    """
    Проверка, что два уже нормализованных заголовка "слишком похожи".

    Эвристики:
    - полное совпадение
    - один — подстрока другого и при этом длина короткого >= 0.8 длины длинного
    - SequenceMatcher().ratio() >= threshold
    """
    if not a_norm or not b_norm:
        return False

    if a_norm == b_norm:
        return True

    shorter, longer = (a_norm, b_norm) if len(a_norm) <= len(b_norm) else (b_norm, a_norm)
    if len(shorter) >= 12 and shorter in longer:
        if len(shorter) / max(len(longer), 1) >= 0.8:
            return True

    ratio = SequenceMatcher(None, a_norm, b_norm).ratio()
    return ratio >= threshold


def _is_duplicate_card(
    title_norm: str,
    body_norm: str,
    seen_titles: List[str],
    seen_bodies: List[str],
    *,
    title_threshold: float,
) -> bool:
    """
    Проверка, что карточка почти дубликат уже выбранных.
    - по заголовку: похожесть >= title_threshold
    - по тексту: одинаковый нормализованный префикс body
    """
    for prev in seen_titles:
        if _titles_too_similar(prev, title_norm, title_threshold):
            return True

    if body_norm:
        for prev in seen_bodies:
            if body_norm == prev:
                return True

    return False


def _get_primary_topic(card: Dict[str, Any], base_tags: List[str]) -> str:
    """
    Выбираем "основную тему" карточки:
    1) пересечение с интересами пользователя (base_tags)
    2) пересечение с DEFAULT_FEED_TAGS
    3) первый тег из card["tags"], если есть
    4) "other"
    """
    tags = card.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    tags_norm = []
    for t in tags:
        if isinstance(t, str):
            v = t.strip().lower()
            if v:
                tags_norm.append(v)

    base_set = {t.lower() for t in (base_tags or [])}
    default_set = {t.lower() for t in DEFAULT_FEED_TAGS}

    for t in tags_norm:
        if t in base_set:
            return t

    for t in tags_norm:
        if t in default_set:
            return t

    if tags_norm:
        return tags_norm[0]

    return "other"


def _get_source_key(card: Dict[str, Any]) -> str:
    """
    Выделяем "источник" карточки для разведения:
    1) meta.source_name
    2) source_ref (например, Telegram-ссылка)
    3) combination(source_type, language)
    """
    meta = card.get("meta") or {}
    source_name = meta.get("source_name")

    if isinstance(source_name, str) and source_name.strip():
        return source_name.strip()

    source_ref = card.get("source_ref")
    if isinstance(source_ref, str) and source_ref.strip():
        return source_ref.strip()

    source_type = str(card.get("source_type") or "unknown")
    language = str(card.get("language") or "xx")
    return f"{source_type}:{language}"


def _postprocess_ranked_cards(
    ranked: List[Dict[str, Any]],
    base_tags: List[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Постпроцессинг уже отсортированного списка:
    - убираем дубли по заголовку/тексту
    - разводим по темам и источникам (ограничиваем длину "ранов")

    Алгоритм:
      1) Идём по ranked сверху вниз (от максимального score к минимальному).
      2) Для каждой карточки:
         - нормализуем title/body, проверяем на дубликат с уже выбранными;
         - считаем primary_topic и source_key;
         - если подряд слишком много карточек с тем же topic/source — откладываем в deferred;
           иначе добавляем в результирующий список.
      3) Второй проход по deferred:
         - вставляем оставшиеся карточки, если они не дубли, но уже без жёстких ограничений
           по "ранам" тем/источников.
    """
    if not ranked:
        return [], {
            "initial": 0,
            "after_dedup_and_diversity": 0,
            "removed_as_duplicates": 0,
            "deferred_count": 0,
            "used_deferred": 0,
        }

    seen_title_norms: List[str] = []
    seen_body_norms: List[str] = []

    result: List[Dict[str, Any]] = []
    deferred: List[Tuple[Dict[str, Any], str, str, str]] = []  # (card, title_norm, body_norm, topic)

    last_topic: str | None = None
    topic_run_length: int = 0

    last_source: str | None = None
    source_run_length: int = 0

    removed_as_duplicates = 0

    # --- Первый проход: деды и разведение ---
    for card in ranked:
        title = (card.get("title") or "").strip()
        body = (card.get("body") or "").strip()

        if not title and not body:
            # Совсем пустые карточки нам не нужны
            removed_as_duplicates += 1
            continue

        title_norm = _normalize_title_for_dedup(title)
        body_norm = _normalize_body_for_dedup(body)

        # Анти-дубли
        if _is_duplicate_card(
            title_norm,
            body_norm,
            seen_title_norms,
            seen_body_norms,
            title_threshold=FEED_TITLE_SIMILARITY_THRESHOLD,
        ):
            removed_as_duplicates += 1
            continue

        topic = _get_primary_topic(card, base_tags)
        source_key = _get_source_key(card)

        violates_topic = (
            last_topic is not None
            and topic == last_topic
            and topic_run_length >= FEED_MAX_TOPIC_RUN
        )
        violates_source = (
            last_source is not None
            and source_key == last_source
            and source_run_length >= FEED_MAX_SOURCE_RUN
        )

        if violates_topic or violates_source:
            # Карточка ок по контенту, но сейчас ломает разнообразие — откладываем
            deferred.append((card, title_norm, body_norm, topic))
            continue

        # Принимаем карточку в выдачу
        result.append(card)
        seen_title_norms.append(title_norm)
        if body_norm:
            seen_body_norms.append(body_norm)

        if topic == last_topic:
            topic_run_length += 1
        else:
            last_topic = topic
            topic_run_length = 1

        if source_key == last_source:
            source_run_length += 1
        else:
            last_source = source_key
            source_run_length = 1

    # --- Второй проход: пробуем использовать отложенные карточки ---
    used_deferred = 0
    for card, title_norm, body_norm, topic in deferred:
        # Повторно проверяем только на дубли (разведение тут уже мягче)
        if _is_duplicate_card(
            title_norm,
            body_norm,
            seen_title_norms,
            seen_body_norms,
            title_threshold=FEED_TITLE_SIMILARITY_THRESHOLD,
        ):
            removed_as_duplicates += 1
            continue

        result.append(card)
        seen_title_norms.append(title_norm)
        if body_norm:
            seen_body_norms.append(body_norm)

        used_deferred += 1

    debug_post: Dict[str, Any] = {
        "initial": len(ranked),
        "after_dedup_and_diversity": len(result),
        "removed_as_duplicates": removed_as_duplicates,
        "deferred_count": len(deferred),
        "used_deferred": used_deferred,
    }

    return result, debug_post


# ==========================
# Получение кандидатов из Supabase
# ==========================


def _fetch_candidate_cards(
    supabase: Client,
    tags: List[str],
    limit: int,
    *,
    max_age_hours: int,
) -> List[Dict[str, Any]]:
    """
    Берём кандидатов из таблицы cards:
    - только is_active = true
    - только достаточно свежие (created_at >= now - max_age_hours)
    - если есть теги, используем overlaps(tags, tags_array).
    """
    if limit <= 0:
        return []

    now = datetime.now(timezone.utc)

    query = (
        supabase.table("cards")
        .select(
            "id,source_type,source_ref,title,body,tags,category,"
            "language,importance_score,created_at,is_active,meta"
        )
        .eq("is_active", True)
    )

    if max_age_hours > 0:
        min_created_at = now - timedelta(hours=max_age_hours)
        query = query.gte("created_at", min_created_at.isoformat())

    if tags:
        query = query.overlaps("tags", tags)

    try:
        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    return data or []


# ==========================
# Скоринг карточек
# ==========================


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
                # Не ломаем весь скор, если одна карточка с кривой датой
                pass

        score = importance + profile_bonus + recency_bonus
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]


# ==========================
# Вставка сгенерированных карточек
# ==========================


def _insert_cards_into_db(
    supabase: Client,
    cards: List[Dict[str, Any]],
    *,
    language: str | None = "ru",
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
    - language: язык карточки ("ru", "en", ...) — можно указать по умолчанию
      или положить в саму карточку c["language"].
    - source_type: "telegram", "rss", "llm" и т.п.
    - source_ref: например, ссылка или message_id канала.
    """
    if not cards:
        return []

    payload: List[Dict[str, Any]] = []

    # Язык по умолчанию, если в самой карточке не указан
    default_lang: str | None
    if isinstance(language, str):
        default_lang = language.strip() or None
    else:
        default_lang = None

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

        # Язык карточки: сначала из самой карточки, потом дефолт, потом "ru"
        card_lang_raw = c.get("language")
        if isinstance(card_lang_raw, str):
            card_lang = card_lang_raw.strip() or None
        else:
            card_lang = None
        final_language = card_lang or default_lang or "ru"

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
                "language": final_language,
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


# ==========================
# Основная логика построения ленты
# ==========================


def build_feed_for_user(
    supabase: Client | None,
    user_id: int,
    limit: int | None = None,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Основная точка входа для /api/feed.

    Возвращает:
    - items: список карточек для пользователя (страница с учётом offset/limit)
    - debug: отладочная информация:
        {
          "reason": "...",
          "base_tags": [...],
          "offset": int,
          "limit": int,
          "total_candidates": int,
          "returned": int,
          "has_more": bool,
          "next_offset": int | None,
          "phases": [
            {"stage": "...", "tags_count": int, "age_hours": int, "fetched": int},
            ...
          ],
          "postprocess": {
            "initial": int,
            "after_dedup_and_diversity": int,
            "removed_as_duplicates": int,
            "deferred_count": int,
            "used_deferred": int,
          }
        }

    offset — сколько карточек пропустить (для "следующих" порций ленты).
    """
    debug: Dict[str, Any] = {
        "offset": offset,
    }

    if supabase is None:
        debug["reason"] = "no_supabase"
        debug["base_tags"] = []
        debug["limit"] = limit or FEED_CARDS_LIMIT_DEFAULT
        debug["total_candidates"] = 0
        debug["returned"] = 0
        debug["has_more"] = False
        debug["next_offset"] = None
        debug["postprocess"] = {
            "initial": 0,
            "after_dedup_and_diversity": 0,
            "removed_as_duplicates": 0,
            "deferred_count": 0,
            "used_deferred": 0,
        }
        return [], debug

    if offset < 0:
        offset = 0

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    # Ограничиваем размер страницы, но не количество кандидатов
    limit = min(max(limit, 1), 50)

    debug["limit"] = limit

    # 1. Теги интересов пользователя
    base_tags = get_interest_tags_for_user(supabase, user_id)
    used_default_tags = False
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS
        used_default_tags = True

    debug["base_tags"] = base_tags
    debug["used_default_tags"] = used_default_tags

    # 2. Собираем кандидатов несколькими "слоями":
    #    сначала строго по тегам пользователя и свежести,
    #    потом при необходимости расширяем выборку за счёт дефолтных тегов и более широкого окна по времени.
    # Берём с запасом: (limit + offset) * 3, чтобы хватило на пропуск offset.
    fetch_limit = (limit + offset) * 3
    fetch_limit = max(fetch_limit, limit)  # на всякий случай
    fetch_limit = min(fetch_limit, FEED_MAX_FETCH_LIMIT)

    mixed_tags = sorted({*base_tags, *DEFAULT_FEED_TAGS})

    phases_config = [
        {
            "stage": "base_tags_recent",
            "tags": base_tags,
            "age_hours": FEED_MAX_CARD_AGE_HOURS,
        },
    ]

    # Если пользовательские теги отличаются от "дефолтных" — пробуем домешать дефолтные
    if mixed_tags != base_tags:
        phases_config.append(
            {
                "stage": "mixed_with_default_recent",
                "tags": mixed_tags,
                "age_hours": FEED_MAX_CARD_AGE_HOURS,
            }
        )

    # Широкое окно по времени с пользовательскими + дефолтными тегами
    phases_config.append(
        {
            "stage": "mixed_with_default_wide",
            "tags": mixed_tags,
            "age_hours": FEED_WIDE_AGE_HOURS,
        }
    )

    candidates_by_id: Dict[str, Dict[str, Any]] = {}
    phases_debug: List[Dict[str, Any]] = []

    for phase in phases_config:
        # Если мы уже собрали достаточно кандидатов, нет смысла продолжать
        if len(candidates_by_id) >= fetch_limit:
            break

        tags = phase["tags"] or []
        age_hours = int(phase["age_hours"])
        stage_name = phase["stage"]

        # Сколько ещё карточек нужно добрать в этом "слое"
        remaining = fetch_limit - len(candidates_by_id)
        if remaining <= 0:
            break

        if not tags:
            phases_debug.append(
                {
                    "stage": stage_name,
                    "tags_count": 0,
                    "age_hours": age_hours,
                    "fetched": 0,
                    "skipped": True,
                }
            )
            continue

        fetched = _fetch_candidate_cards(
            supabase=supabase,
            tags=tags,
            limit=remaining,
            max_age_hours=age_hours,
        )

        for card in fetched:
            cid = card.get("id")
            if cid is None:
                continue
            key = str(cid)
            if key not in candidates_by_id:
                candidates_by_id[key] = card

        phases_debug.append(
            {
                "stage": stage_name,
                "tags_count": len(tags),
                "age_hours": age_hours,
                "fetched": len(fetched),
                "unique_after_phase": len(candidates_by_id),
            }
        )

    candidates: List[Dict[str, Any]] = list(candidates_by_id.values())
    total_candidates = len(candidates)
    debug["phases"] = phases_debug
    debug["total_candidates_raw"] = total_candidates

    # 3. Если в БД ничего не нашли — пробуем сгенерировать карточки через OpenAI.
    if total_candidates == 0:
        if LLM_CARD_GENERATION_ENABLED and openai_is_configured():
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
                inserted = _insert_cards_into_db(
                    supabase,
                    generated,
                    language="ru",
                    source_type="llm",
                )
                candidates = inserted or []
                total_candidates = len(candidates)
                debug["reason"] = "generated_via_openai"
                debug["generated"] = total_candidates
            else:
                debug["reason"] = "no_cards"
                debug["total_candidates"] = 0
                debug["returned"] = 0
                debug["has_more"] = False
                debug["next_offset"] = None
                debug["postprocess"] = {
                    "initial": 0,
                    "after_dedup_and_diversity": 0,
                    "removed_as_duplicates": 0,
                    "deferred_count": 0,
                    "used_deferred": 0,
                }
                return [], debug
        else:
            debug["reason"] = "no_cards"
            debug["total_candidates"] = 0
            debug["returned"] = 0
            debug["has_more"] = False
            debug["next_offset"] = None
            debug["postprocess"] = {
                "initial": 0,
                "after_dedup_and_diversity": 0,
                "removed_as_duplicates": 0,
                "deferred_count": 0,
                "used_deferred": 0,
            }
            return [], debug
    else:
        debug["reason"] = "cards_from_db"

    # 4. Ранжируем
    ranked_raw = _score_cards_for_user(candidates, base_tags)
    total_ranked_raw = len(ranked_raw)

    # 5. Постпроцессинг: анти-дубли + разведение
    ranked, post_debug = _postprocess_ranked_cards(ranked_raw, base_tags)
    total_ranked = len(ranked)

    # 6. Пагинация
    start = min(offset, total_ranked)
    end = min(start + limit, total_ranked)
    page = ranked[start:end]

    has_more = total_ranked > end
    next_offset = end if has_more else None

    debug["total_candidates"] = total_ranked
    debug["returned"] = len(page)
    debug["has_more"] = has_more
    debug["next_offset"] = next_offset
    debug["postprocess"] = {
        **post_debug,
        "total_ranked_raw": total_ranked_raw,
    }

    return page, debug


def build_feed_for_user_paginated(
    supabase: Client | None,
    user_id: int,
    limit: int | None = None,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Обёртка над build_feed_for_user с явными метаданными пагинации.

    Возвращает:
    - items: карточки для текущей "страницы"
    - debug: отладочная информация (reason, base_tags, offset, limit, has_more, ...)
    - cursor: метаданные пагинации (offset, next_offset, has_more)
    """
    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(limit, 1), 50)
    offset = max(0, int(offset))

    # Используем базовую функцию, которая уже умеет учитывать offset/limit
    items, base_debug = build_feed_for_user(
        supabase=supabase,
        user_id=user_id,
        limit=limit,
        offset=offset,
    )

    has_more = bool(base_debug.get("has_more"))
    next_offset = base_debug.get("next_offset")

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

    return items, debug, cursor

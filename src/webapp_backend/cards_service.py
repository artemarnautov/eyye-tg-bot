# file: src/webapp_backend/cards_service.py
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Set
import hashlib

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

# Дефолтный источник только для чисто LLM-карточек,
# когда у нас нет реального канала/СМИ.
DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")

# Ограничения диверсификации
FEED_MAX_TOPIC_RUN = int(os.getenv("FEED_MAX_TOPIC_RUN", "3"))
FEED_MAX_SOURCE_RUN = int(os.getenv("FEED_MAX_SOURCE_RUN", "2"))

# Настройки "памяти" о просмотренных карточках
FEED_SEEN_EXCLUDE_DAYS = int(os.getenv("FEED_SEEN_EXCLUDE_DAYS", "7"))
FEED_SEEN_SESSION_GRACE_MINUTES = int(os.getenv("FEED_SEEN_SESSION_GRACE_MINUTES", "30"))
FEED_SEEN_MAX_ROWS = int(os.getenv("FEED_SEEN_MAX_ROWS", "5000"))

# Сколько шума добавляем в скор для "рандома"
FEED_RANDOMNESS_STRENGTH = float(os.getenv("FEED_RANDOMNESS_STRENGTH", "0.15"))


def _normalize_title_for_dedup(title: str) -> str:
    """
    Канонизация заголовка для анти-дублей:
    - нижний регистр
    - убираем знаки пунктуации
    - схлопываем пробелы
    - обрезаем до 120 символов
    """
    if not title:
        return ""
    import re

    t = title.lower()
    t = re.sub(r"[^\w\s]", " ", t, flags=re.UNICODE)  # только буквы/цифры/пробелы
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]


def _fetch_candidate_cards(
    supabase: Client,
    tags: List[str],
    limit: int,
    *,
    max_age_hours: int,
    exclude_card_ids: Set[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Берём кандидатов из таблицы cards:
    - только is_active = true
    - только достаточно свежие (created_at >= now - max_age_hours)
    - если есть теги, используем overlaps(tags, tags_array).
    - exclude_card_ids: жёстко исключаем карточки, которые юзер уже давно видел.
      (свежие просмотры из текущей сессии не исключаем, чтобы не ломать offset-пагинацию)
    """
    if limit <= 0:
        return []

    now = datetime.now(timezone.utc)

    # Делаем небольшой оверсемплинг, чтобы после фильтрации по exclude_card_ids
    # у нас оставалось примерно limit карточек.
    oversample = limit
    if exclude_card_ids:
        oversample += min(limit, len(exclude_card_ids))
    query_limit = min(oversample, FEED_MAX_FETCH_LIMIT)

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
        resp = query.order("created_at", desc=True).limit(query_limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    cards = data or []

    if not exclude_card_ids:
        return cards

    exclude_card_ids = set(str(cid) for cid in exclude_card_ids)
    filtered: List[Dict[str, Any]] = []
    for card in cards:
        cid = card.get("id")
        if cid is None:
            continue
        if str(cid) in exclude_card_ids:
            continue
        filtered.append(card)

    return filtered


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
    *,
    user_id: int | None = None,
    recent_seen_ids: Set[str] | None = None,
) -> List[Dict[str, Any]]:
    """
    Скор карточек:
    - importance_score
    - бонус за совпадение тегов
    - бонус за свежесть
    - небольшая детерминированная "рандомизация" по user_id+card_id
    - небольшой штраф за совсем свежие просмотры (в рамках текущей сессии)
    """
    now = datetime.now(timezone.utc)
    base_tag_set = set(base_tags)
    recent_seen_ids = {str(cid) for cid in (recent_seen_ids or set())}

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

        # Небольшой штраф, если карточку только что видел (в рамках текущей сессии)
        card_id_str = str(card.get("id"))
        seen_penalty = 0.0
        if card_id_str in recent_seen_ids:
            seen_penalty = 0.3

        # Детерминированный "рандом" на основе user_id + card_id
        random_bonus = 0.0
        if user_id is not None and FEED_RANDOMNESS_STRENGTH > 0:
            key = f"{user_id}:{card_id_str}"
            h = hashlib.sha256(key.encode("utf-8")).hexdigest()
            # Берём первые 8 символов -> int -> [0,1)
            rnd01 = int(h[:8], 16) / 0xFFFFFFFF
            rnd_centered = rnd01 - 0.5  # в диапазоне [-0.5, 0.5]
            random_bonus = rnd_centered * FEED_RANDOMNESS_STRENGTH

        score = importance + profile_bonus + recency_bonus + random_bonus - seen_penalty
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]


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


def _get_seen_card_ids_for_user(
    supabase: Client,
    user_id: int,
) -> Tuple[Set[str], Set[str], Dict[str, Any]]:
    """
    Достаём из user_seen_cards карточки, которые пользователь уже видел.

    Возвращаем:
    - exclude_ids: карточки, которые видели ДАВНО (старше grace-периода) и которые
      мы вообще не хотим показывать (в рамках окна FEED_SEEN_EXCLUDE_DAYS).
    - recent_ids: карточки, которые пользователь видел только что (в рамках grace-периода),
      их не исключаем из кандидатов, чтобы не ломать пагинацию с offset.
    - debug: немного статистики.
    """
    exclude_ids: Set[str] = set()
    recent_ids: Set[str] = set()
    debug: Dict[str, Any] = {}

    if FEED_SEEN_EXCLUDE_DAYS <= 0:
        return exclude_ids, recent_ids, debug

    now = datetime.now(timezone.utc)
    min_seen_at = now - timedelta(days=FEED_SEEN_EXCLUDE_DAYS)
    grace_delta = timedelta(minutes=FEED_SEEN_SESSION_GRACE_MINUTES)

    try:
        resp = (
            supabase.table("user_seen_cards")
            .select("card_id,last_seen_at")
            .eq("user_id", user_id)
            .gte("last_seen_at", min_seen_at.isoformat())
            .order("last_seen_at", desc=True)
            .limit(FEED_SEEN_MAX_ROWS)
            .execute()
        )
    except Exception:
        logger.exception("Error fetching user_seen_cards from Supabase")
        return exclude_ids, recent_ids, debug

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []

    for row in rows:
        cid = row.get("card_id")
        if not cid:
            continue
        cid_str = str(cid)
        ts_raw = row.get("last_seen_at")
        ts: datetime | None = None
        if isinstance(ts_raw, str):
            try:
                ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            except Exception:
                ts = None

        if ts is None:
            exclude_ids.add(cid_str)
            continue

        if now - ts <= grace_delta:
            recent_ids.add(cid_str)
        else:
            exclude_ids.add(cid_str)

    debug["rows"] = len(rows)
    debug["exclude_ids"] = len(exclude_ids)
    debug["recent_ids"] = len(recent_ids)
    debug["window_days"] = FEED_SEEN_EXCLUDE_DAYS
    debug["grace_minutes"] = FEED_SEEN_SESSION_GRACE_MINUTES

    return exclude_ids, recent_ids, debug


def _mark_cards_as_seen(
    supabase: Client,
    user_id: int,
    cards: List[Dict[str, Any]],
) -> None:
    """
    Записываем выданные карточки в user_seen_cards.
    Для MVP — просто insert, дубликаты фильтруем на уровне Python при чтении.
    """
    if not cards:
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    payload: List[Dict[str, Any]] = []
    for card in cards:
        cid = card.get("id")
        if cid is None:
            continue
        payload.append(
            {
                "user_id": user_id,
                "card_id": cid,
                "last_seen_at": now_iso,
            }
        )

    if not payload:
        return

    try:
        supabase.table("user_seen_cards").insert(payload).execute()
    except Exception:
        logger.exception("Error inserting into user_seen_cards")


def _extract_source_and_topic(
    card: Dict[str, Any],
    base_tags: List[str],
) -> Tuple[str, str]:
    """
    Вытаскиваем "источник" (по возможности человекочитаемый) и "основную тему" карточки.
    Это нужно для диверсификации.
    """
    meta = card.get("meta") or {}
    if not isinstance(meta, dict):
        meta = {}

    source = (
        meta.get("source_name")
        or card.get("source_ref")
        or card.get("source_type")
        or "unknown"
    )
    source = str(source)

    card_tags = card.get("tags") or []
    if not isinstance(card_tags, list):
        card_tags = []

    base_tag_set = set(base_tags)
    primary_topic = "other"
    for t in card_tags:
        if t in base_tag_set:
            primary_topic = t
            break
    if primary_topic == "other" and card_tags:
        primary_topic = str(card_tags[0])

    return source, primary_topic


def _apply_dedup_and_diversity(
    ranked: List[Dict[str, Any]],
    base_tags: List[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Постобработка ранжированного списка:
    - удаляем дубли по заголовкам
    - ограничиваем длину "пробега" по одному источнику и теме
    - откладываем конфликтующие карточки в deferred и пытаемся воткнуть их позже
    """
    used_titles: Set[str] = set()
    result: List[Dict[str, Any]] = []
    deferred: List[Dict[str, Any]] = []

    last_source: str | None = None
    last_topic: str | None = None
    source_run = 0
    topic_run = 0

    removed_duplicates = 0
    deferred_count = 0

    for card in ranked:
        title = (card.get("title") or "").strip()
        title_norm = _normalize_title_for_dedup(title)
        if title_norm in used_titles:
            removed_duplicates += 1
            continue

        source, topic = _extract_source_and_topic(card, base_tags)

        if last_source == source:
            source_run += 1
        else:
            last_source = source
            source_run = 1

        if last_topic == topic:
            topic_run += 1
        else:
            last_topic = topic
            topic_run = 1

        if source_run > FEED_MAX_SOURCE_RUN or topic_run > FEED_MAX_TOPIC_RUN:
            deferred.append(card)
            deferred_count += 1
            # откатываем счётчики к предыдущему состоянию (карточка как бы не вставилась)
            if source_run > FEED_MAX_SOURCE_RUN:
                source_run -= 1
            if topic_run > FEED_MAX_TOPIC_RUN:
                topic_run -= 1
            continue

        used_titles.add(title_norm)
        result.append(card)

    # Вторая попытка: пытаемся аккуратно домешать deferred в хвост,
    # не нарушая лимитов по пробегам.
    used_titles_tail = set(used_titles)
    last_source_tail = last_source
    last_topic_tail = last_topic
    source_run_tail = source_run
    topic_run_tail = topic_run

    used_deferred = 0

    for card in deferred:
        title = (card.get("title") or "").strip()
        title_norm = _normalize_title_for_dedup(title)
        if title_norm in used_titles_tail:
            continue

        source, topic = _extract_source_and_topic(card, base_tags)

        if last_source_tail == source:
            sr = source_run_tail + 1
        else:
            sr = 1

        if last_topic_tail == topic:
            tr = topic_run_tail + 1
        else:
            tr = 1

        if sr > FEED_MAX_SOURCE_RUN or tr > FEED_MAX_TOPIC_RUN:
            continue

        used_titles_tail.add(title_norm)
        result.append(card)
        used_deferred += 1

        last_source_tail = source
        last_topic_tail = topic
        source_run_tail = sr
        topic_run_tail = tr

    debug = {
        "initial": len(ranked),
        "after_dedup_and_diversity": len(result),
        "removed_as_duplicates": removed_duplicates,
        "deferred_count": deferred_count,
        "used_deferred": used_deferred,
        "total_ranked_raw": len(ranked),
    }

    return result, debug


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
          ...
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

    # 2. Память о просмотренных карточках
    seen_exclude_ids: Set[str] = set()
    recent_seen_ids: Set[str] = set()
    seen_debug: Dict[str, Any] = {}
    try:
        seen_exclude_ids, recent_seen_ids, seen_debug = _get_seen_card_ids_for_user(
            supabase, user_id
        )
    except Exception:
        logger.exception("Error in _get_seen_card_ids_for_user")
    debug["seen"] = seen_debug

    # 3. Собираем кандидатов несколькими "слоями":
    #    сперва по тегам пользователя и свежести,
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
            exclude_card_ids=seen_exclude_ids,
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
    total_candidates_raw = len(candidates)
    debug["phases"] = phases_debug
    debug["total_candidates_raw"] = total_candidates_raw

    # 4. Если в БД ничего не нашли — пробуем сгенерировать карточки через OpenAI.
    if total_candidates_raw == 0:
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
                total_candidates_raw = len(candidates)
                debug["reason"] = "generated_via_openai"
                debug["generated"] = total_candidates_raw
            else:
                debug["reason"] = "no_cards"
                debug["returned"] = 0
                debug["has_more"] = False
                debug["next_offset"] = None
                return [], debug
        else:
            debug["reason"] = "no_cards"
            debug["returned"] = 0
            debug["has_more"] = False
            debug["next_offset"] = None
            return [], debug
    else:
        debug["reason"] = "cards_from_db"

    # 5. Ранжируем и применяем постобработку (анти-дубли + диверсификация)
    ranked_scored = _score_cards_for_user(
        candidates,
        base_tags,
        user_id=user_id,
        recent_seen_ids=recent_seen_ids,
    )
    ranked, postprocess_debug = _apply_dedup_and_diversity(ranked_scored, base_tags)

    total_ranked = len(ranked)

    # 6. Пагинация по offset/limit
    start = min(offset, total_ranked)
    end = min(start + limit, total_ranked)
    page = ranked[start:end]

    has_more = total_ranked > end
    next_offset = end if has_more else None

    # 7. Обновляем память просмотренных карточек
    try:
        _mark_cards_as_seen(supabase, user_id, page)
    except Exception:
        logger.exception("Error in _mark_cards_as_seen")

    debug["total_candidates"] = total_ranked
    debug["returned"] = len(page)
    debug["has_more"] = has_more
    debug["next_offset"] = next_offset
    debug["postprocess"] = postprocess_debug

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

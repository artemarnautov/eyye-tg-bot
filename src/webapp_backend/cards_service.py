# file: src/webapp_backend/cards_service.py
import logging
import os
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Set

from supabase import Client

from .profile_service import get_interest_tags_for_user
from .openai_client import generate_cards_for_tags, is_configured as openai_is_configured

logger = logging.getLogger(__name__)

# ===================== Теги / топики =====================

MAX_BASE_TAGS = 4  # сколько тегов максимум берём в базовый фильтр

# дефолтный набор, если про юзера ещё ничего не знаем
DEFAULT_BASE_TAGS = ["entertainment", "society", "business", "politics"]


def build_base_tags_from_weights(user_rows: List[Dict[str, Any]]) -> Tuple[List[str], bool, Dict[str, Any]]:
    """
    user_rows: список dict с ключами 'tag' и 'weight' из user_topic_weights.
    Возвращает (base_tags, used_default_tags, debug_info).
    """
    user_rows = user_rows or []
    debug_info: Dict[str, Any] = {
        "count": len(user_rows),
        "top": [],
    }

    if user_rows:
        # сортируем по убыванию веса
        sorted_rows = sorted(
            (r for r in user_rows if r.get("tag")),
            key=lambda r: r.get("weight") or 0.0,
            reverse=True,
        )

        # для debug.top оставим первые 5
        debug_info["top"] = [
            [r["tag"], float(r.get("weight") or 0.0)] for r in sorted_rows[:5]
        ]

        personal_tags: List[str] = []

        # собираем персональные теги (оставляем 1 слот под общие)
        for r in sorted_rows:
            tag = r["tag"]
            if tag not in personal_tags:
                personal_tags.append(tag)
            if len(personal_tags) >= MAX_BASE_TAGS - 1:
                break

        base_tags: List[str] = []

        # сначала персональные
        for tag in personal_tags:
            if len(base_tags) >= MAX_BASE_TAGS:
                break
            if tag not in base_tags:
                base_tags.append(tag)

        # добиваем до MAX_BASE_TAGS дефолтными
        for tag in DEFAULT_BASE_TAGS:
            if len(base_tags) >= MAX_BASE_TAGS:
                break
            if tag not in base_tags:
                base_tags.append(tag)

        return base_tags, False, debug_info

    # если по юзеру нет данных — чистый дефолт
    debug_info["top"] = []
    return DEFAULT_BASE_TAGS[:MAX_BASE_TAGS], True, debug_info


# ===================== Базовые настройки фида =====================

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

# ===================== Память о просмотренных карточках =====================

# Через сколько дней мы перестаём учитывать просмотренные карточки
FEED_SEEN_EXCLUDE_DAYS = int(os.getenv("FEED_SEEN_EXCLUDE_DAYS", "7"))

# Грейс-период на "текущую сессию" (минуты)
FEED_SEEN_SESSION_GRACE_MINUTES = int(
    os.getenv("FEED_SEEN_SESSION_GRACE_MINUTES", "30")
)

# Максимум строк просмотренных карточек, которые мы тащим за раз
FEED_SEEN_MAX_ROWS = int(os.getenv("FEED_SEEN_MAX_ROWS", "5000"))

# Сила рандома в скоре (0.0–0.5; 0.15 — комфортно)
try:
    FEED_RANDOMNESS_STRENGTH = float(os.getenv("FEED_RANDOMNESS_STRENGTH", "0.15"))
except ValueError:
    FEED_RANDOMNESS_STRENGTH = 0.15

# ===================== Вспомогательные функции =====================


def _safe_int_id(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_title_for_duplicate(title: str) -> str:
    """
    Нормализуем заголовок для дедупликации:
    - нижний регистр;
    - убираем пунктуацию;
    - схлопываем пробелы.
    """
    if not title:
        return ""
    t = title.lower()
    # выкидываем простую пунктуацию
    for ch in ",.!?;:«»\"'()[]{}—–-":
        t = t.replace(ch, " ")
    t = " ".join(t.split())
    return t


def _extract_source_key(card: Dict[str, Any]) -> str:
    """
    Ключ источника для диверсификации – в идеале название медиа/канала.
    """
    meta = card.get("meta") or {}
    source_name = (meta.get("source_name") or "").strip()
    if source_name:
        return source_name

    # Фоллбек – тип источника + ссылка
    src_type = (card.get("source_type") or "").strip()
    src_ref = (card.get("source_ref") or "").strip()
    if src_type and src_ref:
        return f"{src_type}:{src_ref}"
    if src_type:
        return src_type
    if src_ref:
        return src_ref
    return "unknown"


def _extract_main_tag(card: Dict[str, Any], base_tags: List[str]) -> str:
    """
    Основной тег карточки – для диверсификации по темам.
    Сначала ищем пересечение с интересами пользователя, потом первый тег.
    """
    tags = card.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    base_set = set(base_tags)
    for t in tags:
        if t in base_set:
            return t
    return tags[0] if tags else "unknown"


# ===================== Работа с таблицей cards =====================


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


# ===================== Память о просмотренных карточках =====================


def _load_seen_cards_for_user(
    supabase: Client,
    user_id: int,
) -> Dict[str, Any]:
    """
    Загружаем из user_seen_cards всё, что пользователь видел за последние FEED_SEEN_EXCLUDE_DAYS.
    Возвращаем:
    {
      "rows": int,
      "exclude_ids": set[int],
      "recent_ids": set[int],
      "window_days": int,
      "grace_minutes": int,
      "error": Optional[str],
    }
    """
    result: Dict[str, Any] = {
        "rows": 0,
        "exclude_ids": set(),  # type: ignore[dict-item]
        "recent_ids": set(),  # type: ignore[dict-item]
        "window_days": FEED_SEEN_EXCLUDE_DAYS,
        "grace_minutes": FEED_SEEN_SESSION_GRACE_MINUTES,
        "error": None,
    }

    if supabase is None:
        result["error"] = "no_supabase"
        return result

    now = datetime.now(timezone.utc)
    window_cutoff = now - timedelta(days=FEED_SEEN_EXCLUDE_DAYS)
    grace_cutoff = now - timedelta(minutes=FEED_SEEN_SESSION_GRACE_MINUTES)

    try:
        resp = (
            supabase.table("user_seen_cards")
            .select("card_id, seen_at")
            .eq("user_id", user_id)
            .gte("seen_at", window_cutoff.isoformat())
            .limit(FEED_SEEN_MAX_ROWS)
            .execute()
        )
    except Exception:
        logger.exception("Error loading seen cards for user_id=%s", user_id)
        result["error"] = "load_failed"
        return result

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []

    exclude_ids: Set[int] = set()
    recent_ids: Set[int] = set()

    for row in rows:
        cid = _safe_int_id(row.get("card_id"))
        if cid is None:
            continue
        exclude_ids.add(cid)

        seen_at = row.get("seen_at")
        dt: datetime | None = None
        if isinstance(seen_at, str):
            try:
                dt = datetime.fromisoformat(seen_at.replace("Z", "+00:00"))
            except Exception:
                dt = None
        if dt and dt >= grace_cutoff:
            recent_ids.add(cid)

    result["rows"] = len(rows)
    result["exclude_ids"] = exclude_ids
    result["recent_ids"] = recent_ids
    return result


def _load_user_topic_weights(
    supabase: Client | None,
    user_id: int,
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """
    Загружаем веса интересов по тегам из user_topic_weights.
    Возвращаем:
    - weights: {tag: weight}
    - rows:   список dict-ов {"tag": ..., "weight": ...} для debug и сборки base_tags.
    """
    weights: Dict[str, float] = {}
    rows: List[Dict[str, Any]] = []

    if supabase is None:
        return weights, rows

    try:
        resp = (
            supabase.table("user_topic_weights")
            .select("tag,weight")
            .eq("tg_id", user_id)
            .execute()
        )
    except Exception:
        logger.exception("Error loading user_topic_weights for user_id=%s", user_id)
        return weights, rows

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)

    for row in data or []:
        tag = str(row.get("tag") or "").strip()
        if not tag:
            continue
        try:
            w = float(row.get("weight") or 0.0)
        except (TypeError, ValueError):
            w = 0.0

        weights[tag] = w
        rows.append(
            {
                "tag": tag,
                "weight": w,
            }
        )

    return weights, rows



def _mark_cards_as_seen(
    supabase: Client | None,
    user_id: int,
    cards: List[Dict[str, Any]],
) -> int:
    """
    Записываем факт просмотра карточек пользователем в user_seen_cards.
    Возвращаем количество вставленных строк (по данным Supabase).
    """
    if supabase is None or not cards:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    payload: List[Dict[str, Any]] = []

    for card in cards:
        cid = _safe_int_id(card.get("id"))
        if cid is None:
            continue
        payload.append(
            {
                "user_id": user_id,
                "card_id": cid,
                "seen_at": now,
            }
        )

    if not payload:
        return 0

    try:
        # Обычно Supabase возвращает вставленные строки,
        # но на всякий случай обрабатываем оба варианта.
        resp = supabase.table("user_seen_cards").insert(payload).execute()
    except Exception:
        logger.exception("Error inserting user_seen_cards for user_id=%s", user_id)
        return 0

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)

    if isinstance(data, list):
        inserted = len(data)
    else:
        # если вернули что-то иное (или returning=minimal) – считаем по payload
        inserted = len(payload)

    logger.info(
        "Marked %d cards as seen for user_id=%s (payload=%d)",
        inserted,
        user_id,
        len(payload),
    )
    return inserted


# ===================== Скоринг и постобработка =====================


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
    user_id: int | None = None,
    user_topic_weights: Dict[str, float] | None = None,
) -> List[Dict[str, Any]]:
    """
    TikTok-lite скоринг:
    - importance_score (базовый вес карточки)
    - персональные веса по тегам из user_topic_weights
    - совпадение тегов с интересами онбординга
    - свежесть
    - лёгкий детерминированный рандом
    """
    now = datetime.now(timezone.utc)
    base_tag_set = set(base_tags)
    today_str = now.strftime("%Y-%m-%d")
    topic_weights = user_topic_weights or {}

    scored: List[Tuple[float, Dict[str, Any]]] = []

    for card in cards:
        card_tags = card.get("tags") or []
        if not isinstance(card_tags, list):
            card_tags = []

        try:
            importance = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            importance = 1.0

        # Сигнал по пользовательским весам тегов (user_topic_weights)
        interest_score = 0.0
        for t in card_tags:
            interest_score += float(topic_weights.get(t, 0.0))

        # Бонус за совпадение с тегами онбординга (простой fallback)
        overlap_count = sum(1 for t in card_tags if t in base_tag_set)
        overlap_bonus = 0.3 * overlap_count

        # Бонус за свежесть (0..1)
        recency_score = 0.0
        created_at = card.get("created_at")
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age_hours = (now - dt).total_seconds() / 3600.0
                if age_hours < FEED_MAX_CARD_AGE_HOURS:
                    recency_score = (
                        FEED_MAX_CARD_AGE_HOURS - age_hours
                    ) / FEED_MAX_CARD_AGE_HOURS
                else:
                    recency_score = 0.0
            except Exception:
                recency_score = 0.0

        # Лёгкий детерминированный рандом для этого пользователя и карточки
        rand_bonus = 0.0
        if FEED_RANDOMNESS_STRENGTH > 0.0:
            cid = _safe_int_id(card.get("id")) or 0
            uid = int(user_id or 0)
            seed_str = f"{uid}:{cid}:{today_str}"
            h = hashlib.sha256(seed_str.encode("utf-8")).digest()
            # Значение в диапазоне [0, 1]
            value = int.from_bytes(h[:4], "big") / float(2**32 - 1)
            # Преобразуем в [-1, 1] и масштабируем
            rand_bonus = (value * 2.0 - 1.0) * FEED_RANDOMNESS_STRENGTH

        # Финальный скор: интересы пользователя доминируют,
        # importance и свежесть — поддерживающие факторы.
        score = (
            importance
            + 1.5 * interest_score
            + overlap_bonus
            + recency_score
            + rand_bonus
        )
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]


def _apply_dedup_and_diversity(
    ranked: List[Dict[str, Any]],
    base_tags: List[str],
    max_consecutive_source: int = 2,
    max_consecutive_tag: int = 2,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Постобработка уже отсортированного списка:
    - убираем дубли по заголовкам;
    - разводим карточки по источникам и основным тегам.

    Возвращаем:
    (список карточек, debug_postprocess)
    """
    if not ranked:
        return [], {
            "initial": 0,
            "after_dedup_and_diversity": 0,
            "removed_as_duplicates": 0,
            "deferred_count": 0,
            "used_deferred": 0,
            "total_ranked_raw": 0,
        }

    total_ranked_raw = len(ranked)

    seen_titles: Set[str] = set()
    selected: List[Dict[str, Any]] = []
    deferred: List[Dict[str, Any]] = []
    removed_duplicates = 0

    def violates_diversity(
        current: List[Dict[str, Any]],
        source_key: str,
        main_tag: str,
    ) -> bool:
        # Смотрим на последние 4 карточки
        window = current[-4:]
        same_source = 0
        same_tag = 0
        for c in window:
            if _extract_source_key(c) == source_key:
                same_source += 1
            if _extract_main_tag(c, base_tags) == main_tag:
                same_tag += 1
        if same_source >= max_consecutive_source or same_tag >= max_consecutive_tag:
            return True
        return False

    # Первый проход: выбираем всё, что не ломает диверсификацию
    for card in ranked:
        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue

        source_key = _extract_source_key(card)
        main_tag = _extract_main_tag(card, base_tags)

        if violates_diversity(selected, source_key, main_tag):
            deferred.append(card)
            continue

        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)

    deferred_count = len(deferred)
    used_deferred = 0

    # Второй проход: пытаемся домешать отложенные карточки
    for card in deferred:
        source_key = _extract_source_key(card)
        main_tag = _extract_main_tag(card, base_tags)
        if violates_diversity(selected, source_key, main_tag):
            continue

        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue

        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)
        used_deferred += 1

    debug_postprocess = {
        "initial": total_ranked_raw,
        "after_dedup_and_diversity": len(selected),
        "removed_as_duplicates": removed_duplicates,
        "deferred_count": deferred_count,
        "used_deferred": used_deferred,
        "total_ranked_raw": total_ranked_raw,
    }

    return selected, debug_postprocess


# ===================== Вставка LLM-карточек в DB =====================


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


# ===================== Основная логика фида =====================


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
    - debug: отладочная информация.
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
        debug["seen"] = {
            "rows": 0,
            "exclude_ids": 0,
            "recent_ids": 0,
            "window_days": FEED_SEEN_EXCLUDE_DAYS,
            "grace_minutes": FEED_SEEN_SESSION_GRACE_MINUTES,
        }
        return [], debug

    if offset < 0:
        offset = 0

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    # Ограничиваем размер страницы, но не количество кандидатов
    limit = min(max(limit, 1), 50)

    debug["limit"] = limit

       # 1. Загружаем веса интересов пользователя по тегам (user_topic_weights)
    user_topic_weights, user_topic_rows = _load_user_topic_weights(supabase, user_id)

    # 1.1. Собираем базовые теги для фида
    base_tags: List[str] = []
    used_default_tags = False

    if user_topic_rows:
        # Есть реальные веса из таблицы user_topic_weights → строим base_tags из них
        base_tags, used_default_tags, user_topics_debug = build_base_tags_from_weights(
            user_topic_rows
        )
    else:
        # Фоллбек: онбординг → дефолтные теги фида
        base_tags = get_interest_tags_for_user(supabase, user_id)
        if not base_tags:
            base_tags = DEFAULT_FEED_TAGS
            used_default_tags = True
        user_topics_debug = {
            "count": 0,
            "top": [],
        }

    debug["base_tags"] = base_tags
    debug["used_default_tags"] = used_default_tags
    debug["user_topic_weights"] = user_topics_debug

    # 1.2. Загружаем просмотренные карточки
    seen_info = _load_seen_cards_for_user(supabase, user_id)
    exclude_ids: Set[int] = seen_info.get("exclude_ids") or set()
    recent_ids: Set[int] = seen_info.get("recent_ids") or set()

    debug["seen"] = {
        "rows": int(seen_info.get("rows") or 0),
        "exclude_ids": len(exclude_ids),
        "recent_ids": len(recent_ids),
        "window_days": FEED_SEEN_EXCLUDE_DAYS,
        "grace_minutes": FEED_SEEN_SESSION_GRACE_MINUTES,
        "error": seen_info.get("error"),
    }

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
    total_candidates_raw = len(candidates)
    debug["phases"] = phases_debug
    debug["total_candidates_raw"] = total_candidates_raw

    # 2.1. Фильтруем уже просмотренные карточки (по user_seen_cards)
    if exclude_ids:
        before_seen = len(candidates)
        filtered: List[Dict[str, Any]] = []
        for c in candidates:
            cid = _safe_int_id(c.get("id"))
            if cid is None:
                filtered.append(c)
                continue
            if cid in exclude_ids:
                continue
            filtered.append(c)
        candidates = filtered
        debug["removed_seen"] = before_seen - len(candidates)

    total_candidates = len(candidates)
    debug["total_candidates"] = total_candidates

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

    # 4. Ранжируем (TikTok-lite) и применяем дедуп/диверсификацию
    ranked_raw = _score_cards_for_user(
        candidates,
        base_tags,
        user_id=user_id,
        user_topic_weights=user_topic_weights,
    )
    ranked, postprocess_debug = _apply_dedup_and_diversity(ranked_raw, base_tags)
    debug["postprocess"] = postprocess_debug

    total_ranked = len(ranked)

    # 5. Пагинация по offset/limit
    start = min(offset, total_ranked)
    end = min(start + limit, total_ranked)
    page = ranked[start:end]

    has_more = total_ranked > end
    next_offset = end if has_more else None

    debug["total_candidates"] = total_ranked
    debug["returned"] = len(page)
    debug["has_more"] = has_more
    debug["next_offset"] = next_offset

    # 6. Отмечаем карточки как просмотренные
    seen_marked = _mark_cards_as_seen(supabase, user_id, page)
    debug["seen"]["marked"] = int(seen_marked)

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

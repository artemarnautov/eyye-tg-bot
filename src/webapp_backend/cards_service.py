# file: src/webapp_backend/cards_service.py
import base64
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from supabase import Client

from .profile_service import get_interest_tags_for_user
from .openai_client import generate_cards_for_tags, is_configured as openai_is_configured

logger = logging.getLogger(__name__)

# ===================== Теги / топики =====================

MAX_BASE_TAGS = 4  # только для debug/top, но не режем реальные интересы профиля
DEFAULT_BASE_TAGS = ["entertainment", "society", "business", "politics"]

# "смежные векторы" (MVP-версия: простая карта соседей)
TAG_NEIGHBORS: Dict[str, List[str]] = {
    "world_news": ["politics", "business", "society"],
    "business": ["finance", "tech", "world_news"],
    "finance": ["business", "tech", "world_news"],
    "tech": ["business", "finance", "science", "gaming"],
    "science": ["tech", "education", "history"],
    "history": ["science", "politics", "society"],
    "politics": ["world_news", "society", "business"],
    "society": ["politics", "world_news", "lifestyle", "city"],
    "entertainment": ["lifestyle", "gaming", "society"],
    "gaming": ["tech", "entertainment"],
    "sports": ["society", "lifestyle"],
    "lifestyle": ["society", "entertainment", "city"],
    "education": ["science", "tech", "uk_students"],
    "city": ["society", "lifestyle"],
    "uk_students": ["education", "world_news"],
}


def build_base_tags_from_weights(
    user_rows: List[Dict[str, Any]],
) -> Tuple[List[str], bool, Dict[str, Any]]:
    """
    user_rows: список dict с ключами 'tag' и 'weight' из user_topic_weights.
    Возвращает (base_tags, used_default_tags, debug_info).
    """
    user_rows = user_rows or []
    debug_info: Dict[str, Any] = {"count": len(user_rows), "top": []}

    if user_rows:
        sorted_rows = sorted(
            (r for r in user_rows if r.get("tag")),
            key=lambda r: r.get("weight") or 0.0,
            reverse=True,
        )

        debug_info["top"] = [[r["tag"], float(r.get("weight") or 0.0)] for r in sorted_rows[:5]]

        personal_tags: List[str] = []
        for r in sorted_rows:
            tag = r["tag"]
            if tag not in personal_tags:
                personal_tags.append(tag)
            if len(personal_tags) >= MAX_BASE_TAGS - 1:
                break

        base_tags: List[str] = []
        for tag in personal_tags:
            if len(base_tags) >= MAX_BASE_TAGS:
                break
            if tag not in base_tags:
                base_tags.append(tag)

        for tag in DEFAULT_BASE_TAGS:
            if len(base_tags) >= MAX_BASE_TAGS:
                break
            if tag not in base_tags:
                base_tags.append(tag)

        return base_tags, False, debug_info

    debug_info["top"] = []
    return DEFAULT_BASE_TAGS[:MAX_BASE_TAGS], True, debug_info


# ===================== Базовые настройки фида =====================

FEED_CARDS_LIMIT_DEFAULT = int(os.getenv("FEED_CARDS_LIMIT", "20"))

# ВАЖНО: "новость не старше 7 дней" — это наш hard cap для time-sensitive карточек
FEED_MAX_CARD_AGE_HOURS = int(os.getenv("FEED_MAX_CARD_AGE_HOURS", "168"))  # 7 дней по умолчанию

LLM_CARD_GENERATION_ENABLED = os.getenv("LLM_CARD_GENERATION_ENABLED", "true").lower() in ("1", "true", "yes")

DEFAULT_FEED_TAGS: List[str] = ["world_news", "business", "tech", "uk_students"]

FEED_MAX_FETCH_LIMIT = int(os.getenv("FEED_MAX_FETCH_LIMIT", "600"))

# wide/deep оставляем как fallback, но "news" всё равно стараемся не показывать глубже 7 дней
FEED_WIDE_AGE_HOURS = int(os.getenv("FEED_WIDE_AGE_HOURS", "2160"))    # 90 дней
FEED_DEEP_AGE_HOURS = int(os.getenv("FEED_DEEP_AGE_HOURS", "8760"))    # 1 год

DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")

# ===================== Память о просмотренных карточках =====================

FEED_SEEN_EXCLUDE_DAYS = int(os.getenv("FEED_SEEN_EXCLUDE_DAYS", "14"))
FEED_SEEN_SESSION_GRACE_MINUTES = int(os.getenv("FEED_SEEN_SESSION_GRACE_MINUTES", "30"))
FEED_SEEN_MAX_ROWS = int(os.getenv("FEED_SEEN_MAX_ROWS", "5000"))

try:
    FEED_RANDOMNESS_STRENGTH = float(os.getenv("FEED_RANDOMNESS_STRENGTH", "0.10"))
except ValueError:
    FEED_RANDOMNESS_STRENGTH = 0.10

# ===================== Настройки для Wikipedia-источника =====================

WIKI_WINDOW_SIZE = int(os.getenv("FEED_WIKI_WINDOW_SIZE", "5"))
WIKI_MAX_IN_WINDOW = int(os.getenv("FEED_WIKI_MAX_IN_WINDOW", "1"))

# ===================== Cursor helpers (NEW: object cursor) =====================

def _encode_cursor_obj(obj: Dict[str, Any]) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

def _decode_cursor_obj(token: Optional[str]) -> Optional[Dict[str, Any]]:
    if token is None:
        return None
    t = str(token).strip()
    if t == "":
        return {}  # пустой cursor = старт blend-ленты
    try:
        pad = "=" * (-len(t) % 4)
        raw = base64.urlsafe_b64decode((t + pad).encode("utf-8")).decode("utf-8")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None

# backward compatibility wrappers (если где-то ещё ожидают before_id)
def _encode_cursor(before_id: Optional[int]) -> Optional[str]:
    if before_id is None:
        return None
    return _encode_cursor_obj({"mode": "chron", "before_id": int(before_id)})

def _decode_cursor(token: Optional[str]) -> Optional[int]:
    obj = _decode_cursor_obj(token)
    if not obj:
        return None
    return _safe_int_id(obj.get("before_id"))


# ===================== Вспомогательные функции =====================

def _safe_int_id(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def _normalize_title_for_duplicate(title: str) -> str:
    if not title:
        return ""
    t = title.lower()
    for ch in ",.!?;:«»\"'()[]{}—–-":
        t = t.replace(ch, " ")
    t = " ".join(t.split())
    return t

def _tg_channel_from_ref(ref: str) -> Optional[str]:
    if not ref:
        return None
    ref = ref.strip()
    if "t.me/" not in ref:
        return None
    try:
        tail = ref.split("t.me/", 1)[1]
        channel = tail.split("/", 1)[0].strip()
        return channel or None
    except Exception:
        return None

def _extract_source_key(card: Dict[str, Any]) -> str:
    meta = card.get("meta") or {}
    src_type = (card.get("source_type") or "").strip().lower()
    src_ref = (card.get("source_ref") or "").strip()

    if src_type == "wikipedia":
        wiki_lang = (meta.get("wiki_lang") or "").strip() or "unknown"
        return f"wikipedia:{wiki_lang}"

    source_name = (meta.get("source_name") or "").strip()
    if source_name:
        return source_name

    if src_type == "telegram" and src_ref:
        ch = _tg_channel_from_ref(src_ref)
        if ch:
            return f"tg:{ch}"

    if src_type and src_ref:
        return f"{src_type}:{src_ref}"
    if src_type:
        return src_type
    if src_ref:
        return src_ref
    return "unknown"

def _extract_main_tag(card: Dict[str, Any], base_tags: List[str]) -> str:
    tags = card.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    base_set = set(base_tags)
    for t in tags:
        if t in base_set:
            return t
    return tags[0] if tags else "unknown"

def _is_wikipedia_card(card: Dict[str, Any]) -> bool:
    src_type = (card.get("source_type") or "").strip().lower()
    return src_type == "wikipedia"

def _is_time_sensitive_news(card: Dict[str, Any]) -> bool:
    """
    MVP-эвристика:
    - всё, что НЕ wikipedia, считаем time-sensitive "news"
    (позже можно усложнить через meta.kind/event_type).
    """
    return not _is_wikipedia_card(card)

def _unique_keep_order(items: List[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for x in items:
        s = str(x or "").strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out

def _expand_with_neighbors(tags: List[str], depth: int = 1) -> List[str]:
    """
    Берём теги + их соседей (depth=1 достаточно для MVP).
    """
    tags = _unique_keep_order(tags)
    out = list(tags)
    if depth <= 0:
        return out

    for t in tags:
        for nb in TAG_NEIGHBORS.get(t, []):
            if nb not in out:
                out.append(nb)
    return out


# ===================== Работа с таблицей cards =====================

def _fetch_candidate_cards(
    supabase: Client,
    tags: List[str],
    limit: int,
    *,
    max_age_hours: int,
    min_age_hours: int = 0,
    before_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Берём кандидатов из таблицы cards:
    - is_active = true
    - created_at в окне: now-max_age_hours <= created_at < now-min_age_hours
    - overlaps(tags, tags_array) если tags задан
    - cursor "chron": id < before_id (если before_id задан)
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

    if min_age_hours > 0:
        max_created_at = now - timedelta(hours=min_age_hours)
        query = query.lt("created_at", max_created_at.isoformat())

    if before_id is not None:
        query = query.lt("id", int(before_id))

    if tags:
        query = query.overlaps("tags", tags)

    try:
        resp = query.order("created_at", desc=True).order("id", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    return data or []


# ===================== Память о просмотренных карточках =====================

def _load_seen_cards_for_user(supabase: Client, user_id: int) -> Dict[str, Any]:
    """
    Загружаем из user_seen_cards всё, что пользователь видел за последние FEED_SEEN_EXCLUDE_DAYS.
    """
    result: Dict[str, Any] = {
        "rows": 0,
        "exclude_ids": set(),  # type: ignore[dict-item]
        "recent_ids": set(),   # type: ignore[dict-item]
        "window_days": FEED_SEEN_EXCLUDE_DAYS,
        "grace_minutes": FEED_SEEN_SESSION_GRACE_MINUTES,
        "error": None,
    }

    now = datetime.now(timezone.utc)
    window_cutoff = now - timedelta(days=FEED_SEEN_EXCLUDE_DAYS)
    grace_cutoff = now - timedelta(minutes=FEED_SEEN_SESSION_GRACE_MINUTES)

    try:
        resp = (
            supabase.table("user_seen_cards")
            .select("card_id, seen_at")
            .eq("user_id", user_id)
            .gte("seen_at", window_cutoff.isoformat())
            .order("seen_at", desc=True)
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
        dt: Optional[datetime] = None
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
    supabase: Optional[Client],
    user_id: int,
) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
    """
    Загружаем веса интересов по тегам из user_topic_weights.
    tg_id в таблице = Telegram ID.
    """
    weights: Dict[str, float] = {}
    rows: List[Dict[str, Any]] = []

    if supabase is None:
        return weights, rows

    try:
        resp = supabase.table("user_topic_weights").select("tag,weight").eq("tg_id", user_id).execute()
    except Exception:
        logger.exception("Error loading user_topic_weights for user_id=%s", user_id)
        return weights, rows

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)

    rows = list(data or [])

    for row in rows:
        tag = str(row.get("tag") or "").strip()
        if not tag:
            continue
        try:
            w = float(row.get("weight") or 0.0)
        except (TypeError, ValueError):
            w = 0.0
        weights[tag] = w

    return weights, rows


def _mark_cards_as_seen(
    supabase: Optional[Client],
    user_id: int,
    cards: List[Dict[str, Any]],
) -> int:
    """
    Записываем просмотр в user_seen_cards через UPSERT (нужен unique index по user_id,card_id).
    """
    if supabase is None or not cards:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    payload: List[Dict[str, Any]] = []

    for card in cards:
        cid = _safe_int_id(card.get("id"))
        if cid is None:
            continue
        payload.append({"user_id": user_id, "card_id": cid, "seen_at": now})

    if not payload:
        return 0

    try:
        resp = supabase.table("user_seen_cards").upsert(payload, on_conflict="user_id,card_id").execute()
    except Exception:
        logger.exception("Error upserting user_seen_cards for user_id=%s", user_id)
        return 0

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)

    return len(data) if isinstance(data, list) else len(payload)


# ===================== Signals / "история" (MVP) =====================

def _load_recent_positive_signals(
    supabase: Client,
    tg_id: int,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Берём "позитивные" сигналы из user_events:
    - like
    - open_source
    - view с dwell_ms >= 8000 (долго читал)
    Возвращаем:
      {
        "seed_card_ids": [...],
        "seed_tags": [...],
        "events_rows": N
      }
    """
    out: Dict[str, Any] = {"seed_card_ids": [], "seed_tags": [], "events_rows": 0}
    try:
        resp = (
            supabase.table("user_events")
            .select("card_id,event_type,dwell_ms,created_at")
            .eq("tg_id", tg_id)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load user_events for tg_id=%s", tg_id)
        return out

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = list(data or [])
    out["events_rows"] = len(rows)

    seed_ids: List[int] = []
    for r in rows:
        cid = _safe_int_id(r.get("card_id"))
        if cid is None:
            continue
        et = str(r.get("event_type") or "").strip()
        if et in ("like", "open_source"):
            seed_ids.append(cid)
            continue
        if et == "view":
            try:
                dwell = int(r.get("dwell_ms") or 0)
            except Exception:
                dwell = 0
            if dwell >= 8000:
                seed_ids.append(cid)

        if len(seed_ids) >= 8:
            break

    seed_ids = _unique_keep_order([str(x) for x in seed_ids])  # type: ignore[arg-type]
    seed_ids_int: List[int] = [int(x) for x in seed_ids if str(x).isdigit()]  # safe

    out["seed_card_ids"] = seed_ids_int

    if not seed_ids_int:
        return out

    # подтягиваем теги этих карточек
    try:
        resp2 = (
            supabase.table("cards")
            .select("id,tags,created_at")
            .in_("id", seed_ids_int)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load seed cards for tg_id=%s", tg_id)
        return out

    data2 = getattr(resp2, "data", None)
    if data2 is None:
        data2 = getattr(resp2, "model", None)
    cards = list(data2 or [])

    tags: List[str] = []
    for c in cards:
        t = c.get("tags") or []
        if isinstance(t, list):
            for x in t:
                s = str(x or "").strip()
                if s:
                    tags.append(s)

    out["seed_tags"] = _unique_keep_order(tags)[:10]
    return out


def _load_recent_read_age_stats(
    supabase: Client,
    tg_id: int,
    limit: int = 30,
) -> Dict[str, Any]:
    """
    "Не уводить пользователя в прошлое":
    считаем средний возраст (age_hours) карточек, которые пользователь видел недавно.
    """
    out: Dict[str, Any] = {"rows": 0, "avg_age_hours": None, "median_age_hours": None}
    now = datetime.now(timezone.utc)

    try:
        resp = (
            supabase.table("user_seen_cards")
            .select("card_id,seen_at")
            .eq("user_id", tg_id)
            .order("seen_at", desc=True)
            .limit(limit)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load user_seen_cards for age stats tg_id=%s", tg_id)
        return out

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = list(data or [])
    out["rows"] = len(rows)
    if not rows:
        return out

    ids = []
    for r in rows:
        cid = _safe_int_id(r.get("card_id"))
        if cid is not None:
            ids.append(cid)
    ids = sorted(set(ids))
    if not ids:
        return out

    try:
        resp2 = supabase.table("cards").select("id,created_at").in_("id", ids).execute()
    except Exception:
        logger.exception("Failed to load cards.created_at for age stats tg_id=%s", tg_id)
        return out

    data2 = getattr(resp2, "data", None)
    if data2 is None:
        data2 = getattr(resp2, "model", None)
    cards = list(data2 or [])

    ages: List[float] = []
    for c in cards:
        ca = c.get("created_at")
        if isinstance(ca, str):
            try:
                dt = datetime.fromisoformat(ca.replace("Z", "+00:00"))
                ages.append(max(0.0, (now - dt).total_seconds() / 3600.0))
            except Exception:
                continue

    if not ages:
        return out

    ages_sorted = sorted(ages)
    out["avg_age_hours"] = sum(ages_sorted) / float(len(ages_sorted))
    mid = len(ages_sorted) // 2
    if len(ages_sorted) % 2 == 1:
        out["median_age_hours"] = ages_sorted[mid]
    else:
        out["median_age_hours"] = (ages_sorted[mid - 1] + ages_sorted[mid]) / 2.0
    return out


# ===================== Скоринг и постобработка =====================

def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
    *,
    user_id: Optional[int] = None,
    user_topic_weights: Optional[Dict[str, float]] = None,
    hot_tags: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    now = datetime.now(timezone.utc)
    base_tag_set = set(base_tags)
    today_str = now.strftime("%Y-%m-%d")
    topic_weights = user_topic_weights or {}
    hot = hot_tags or set()

    scored: List[Tuple[float, Dict[str, Any]]] = []

    for card in cards:
        card_tags = card.get("tags") or []
        if not isinstance(card_tags, list):
            card_tags = []

        try:
            importance = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            importance = 1.0

        # 1) персональный интерес
        interest_score = 0.0
        for t in card_tags:
            interest_score += float(topic_weights.get(t, 0.0))

        # 2) совпадение с базовыми тегами (сильнее)
        overlap_count = sum(1 for t in card_tags if t in base_tag_set)
        overlap_bonus = 0.35 * overlap_count

        # 3) "горячие" теги (продолжение/смежность — мягкий буст)
        hot_bonus = 0.0
        for t in card_tags:
            if t in hot:
                hot_bonus += 0.25
        hot_bonus = min(hot_bonus, 0.75)

        # 4) свежесть
        recency_score = 0.0
        created_at = card.get("created_at")
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age_hours = (now - dt).total_seconds() / 3600.0
                cap = float(FEED_MAX_CARD_AGE_HOURS if _is_time_sensitive_news(card) else max(FEED_WIDE_AGE_HOURS, FEED_MAX_CARD_AGE_HOURS))
                if cap > 0 and age_hours < cap:
                    recency_score = (cap - age_hours) / cap
                else:
                    recency_score = 0.0
            except Exception:
                recency_score = 0.0

        # 5) небольшой детерминированный рандом (чтобы микс был живой, но повторяемый в рамках дня)
        rand_bonus = 0.0
        if FEED_RANDOMNESS_STRENGTH > 0.0:
            cid = _safe_int_id(card.get("id")) or 0
            uid = int(user_id or 0)
            seed_str = f"{uid}:{cid}:{today_str}"
            h = hashlib.sha256(seed_str.encode("utf-8")).digest()
            value = int.from_bytes(h[:4], "big") / float(2**32 - 1)
            rand_bonus = (value * 2.0 - 1.0) * FEED_RANDOMNESS_STRENGTH

        score = (
            importance
            + 1.5 * interest_score
            + overlap_bonus
            + 0.9 * recency_score
            + hot_bonus
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
    if not ranked:
        return [], {
            "initial": 0,
            "after_dedup_and_diversity": 0,
            "removed_as_duplicates": 0,
            "deferred_count": 0,
            "used_deferred": 0,
            "tail_added": 0,
            "total_ranked_raw": 0,
            "wiki_window_size": WIKI_WINDOW_SIZE,
            "wiki_max_in_window": WIKI_MAX_IN_WINDOW,
        }

    total_ranked_raw = len(ranked)

    seen_titles: Set[str] = set()
    seen_fps: Set[str] = set()
    selected: List[Dict[str, Any]] = []
    deferred: List[Dict[str, Any]] = []
    removed_duplicates = 0

    def _fingerprint(card: Dict[str, Any]) -> str:
        t = _normalize_title_for_duplicate((card.get("title") or "").strip())
        b = _normalize_title_for_duplicate(((card.get("body") or "")[:280]).strip())
        s = f"{t}|{b}"
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    def _consecutive_tail_count(current: List[Dict[str, Any]], kind: str, value: str) -> int:
        n = 0
        for c in reversed(current):
            v = _extract_source_key(c) if kind == "source" else _extract_main_tag(c, base_tags)
            if v == value:
                n += 1
            else:
                break
        return n

    def violates(current: List[Dict[str, Any]], card: Dict[str, Any], strict: bool = True) -> bool:
        source_key = _extract_source_key(card)
        main_tag = _extract_main_tag(card, base_tags)

        if _consecutive_tail_count(current, "source", source_key) >= max_consecutive_source:
            return True
        if _consecutive_tail_count(current, "tag", main_tag) >= max_consecutive_tag:
            return True

        if _is_wikipedia_card(card) and WIKI_WINDOW_SIZE > 0:
            wiki_window = current[-WIKI_WINDOW_SIZE:]
            wiki_count = sum(1 for c in wiki_window if _is_wikipedia_card(c))
            if wiki_count >= WIKI_MAX_IN_WINDOW:
                return True

        if strict and current and _is_wikipedia_card(current[-1]) and _is_wikipedia_card(card):
            return True

        return False

    for card in ranked:
        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        fp = _fingerprint(card)

        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue
        if fp in seen_fps:
            removed_duplicates += 1
            continue

        if violates(selected, card, strict=True):
            deferred.append(card)
            continue

        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)
        seen_fps.add(fp)

    still_deferred: List[Dict[str, Any]] = []
    used_deferred = 0

    for card in deferred:
        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        fp = _fingerprint(card)

        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue
        if fp in seen_fps:
            removed_duplicates += 1
            continue

        if violates(selected, card, strict=True):
            still_deferred.append(card)
            continue

        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)
        seen_fps.add(fp)
        used_deferred += 1

    tail_added = 0
    tail_queue = list(still_deferred)
    rotations = 0
    max_rot = max(len(tail_queue) * 2, 50)

    while tail_queue and rotations < max_rot:
        card = tail_queue.pop(0)

        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        fp = _fingerprint(card)

        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue
        if fp in seen_fps:
            removed_duplicates += 1
            continue

        src = _extract_source_key(card)
        if _consecutive_tail_count(selected, "source", src) >= max(max_consecutive_source, 3):
            tail_queue.append(card)
            rotations += 1
            continue

        if selected and _is_wikipedia_card(selected[-1]) and _is_wikipedia_card(card):
            tail_queue.append(card)
            rotations += 1
            continue

        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)
        seen_fps.add(fp)
        tail_added += 1
        rotations = 0

    for card in tail_queue:
        title = (card.get("title") or "").strip()
        norm_title = _normalize_title_for_duplicate(title)
        fp = _fingerprint(card)
        if norm_title and norm_title in seen_titles:
            removed_duplicates += 1
            continue
        if fp in seen_fps:
            removed_duplicates += 1
            continue
        selected.append(card)
        if norm_title:
            seen_titles.add(norm_title)
        seen_fps.add(fp)
        tail_added += 1

    debug_postprocess = {
        "initial": total_ranked_raw,
        "after_dedup_and_diversity": len(selected),
        "removed_as_duplicates": removed_duplicates,
        "deferred_count": len(deferred),
        "used_deferred": used_deferred,
        "tail_added": tail_added,
        "total_ranked_raw": total_ranked_raw,
        "wiki_window_size": WIKI_WINDOW_SIZE,
        "wiki_max_in_window": WIKI_MAX_IN_WINDOW,
    }

    return selected, debug_postprocess


# ===================== Вставка LLM-карточек в DB =====================

def _insert_cards_into_db(
    supabase: Client,
    cards: List[Dict[str, Any]],
    *,
    language: Optional[str] = "ru",
    source_type: str = "llm",
    fallback_source_name: Optional[str] = None,
    source_ref: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not cards:
        return []

    payload: List[Dict[str, Any]] = []

    default_lang: Optional[str]
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

        raw_source_name = c.get("source_name") or c.get("source") or c.get("channel_name") or c.get("channel_title")
        if not raw_source_name and fallback_source_name:
            raw_source_name = fallback_source_name
        if not raw_source_name:
            raw_source_name = DEFAULT_SOURCE_NAME
        source_name = str(raw_source_name).strip()

        card_source_ref = c.get("source_ref") or c.get("url") or c.get("link")
        final_source_ref = source_ref or card_source_ref

        card_lang_raw = c.get("language")
        if isinstance(card_lang_raw, str):
            card_lang = card_lang_raw.strip() or None
        else:
            card_lang = None
        final_language = card_lang or default_lang or "ru"

        meta: Dict[str, Any] = {"source_name": source_name}

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


# ===================== "Интересная" выдача (blend) =====================

def _build_age_bucket_plan(
    *,
    limit: int,
    read_avg_age_hours: Optional[float],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Идея:
    - мешаем новое + чуть более старое, но не уводим глубоко "в прошлое"
    - всё в пределах FEED_MAX_CARD_AGE_HOURS (для news)
    Бакеты:
      A: 0..12h
      B: 12..48h
      C: 48..168h (или меньше, если FEED_MAX_CARD_AGE_HOURS < 168)
    """
    max_h = max(24, FEED_MAX_CARD_AGE_HOURS)
    c_max = min(max_h, 168)

    # базовые доли
    wA, wB, wC = 0.50, 0.30, 0.20

    # если пользователь уже "ушёл назад" (читает старое) — возвращаем больше свежего
    if read_avg_age_hours is not None:
        if read_avg_age_hours > 72:
            wA, wB, wC = 0.65, 0.25, 0.10
        elif read_avg_age_hours > 48:
            wA, wB, wC = 0.58, 0.28, 0.14

    # превращаем доли в counts
    a = max(1, int(round(limit * wA)))
    b = max(1, int(round(limit * wB)))
    c = max(0, limit - a - b)
    if c < 0:
        c = 0
        # подрежем b
        b = max(1, limit - a)
    if a + b + c < limit:
        a = a + (limit - (a + b + c))

    plan = [
        {"name": "fresh_0_12h", "min_age": 0, "max_age": 12, "count": a},
        {"name": "mid_12_48h", "min_age": 12, "max_age": 48, "count": b},
        {"name": "old_48_cap", "min_age": 48, "max_age": c_max, "count": c},
    ]
    debug = {"bucket_counts": {"A": a, "B": b, "C": c}, "max_news_age_hours": FEED_MAX_CARD_AGE_HOURS}
    return plan, debug


def _collect_candidates_blend(
    supabase: Client,
    *,
    base_tags: List[str],
    hot_tags: List[str],
    exclude_ids: Set[int],
    limit: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Собираем кандидатов порциями по age buckets и по тегам:
    - base_tags (профиль)
    - hot_tags (смежные/продолжения по телеметрии)
    """
    debug: Dict[str, Any] = {"stages": []}
    fetch_cap = min(max(limit * 20, 200), FEED_MAX_FETCH_LIMIT)

    tags_personal = _unique_keep_order(base_tags)
    tags_hot = _unique_keep_order(hot_tags)
    tags_mixed = _unique_keep_order(tags_personal + tags_hot + DEFAULT_FEED_TAGS)
    tags_for_query = _expand_with_neighbors(tags_mixed, depth=1)

    # возраст чтения (чтобы не уводить в прошлое)
    read_stats = _load_recent_read_age_stats(supabase, tg_id=int(debug.get("tg_id", 0)) )  # placeholder
    # ^ выше мы не знаем tg_id, поэтому read_stats заполним снаружи (см. build_feed_for_user_cursor)
    # тут просто оставим поле в debug из параметра.

    debug["tags_personal"] = tags_personal
    debug["tags_hot"] = tags_hot
    debug["tags_query_count"] = len(tags_for_query)

    return [], debug  # будет заполнено в build_feed_for_user_cursor (там есть tg_id)


# ===================== Основная логика фида (OFFSET режим) =====================

def build_feed_for_user(
    supabase: Optional[Client],
    user_id: int,
    limit: Optional[int] = None,
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    OFFSET оставляем как fallback/legacy.
    Для реально "бесконечной" выдачи — используем cursor/blend.
    """
    debug: Dict[str, Any] = {"offset": offset}

    if supabase is None:
        debug.update(
            {
                "reason": "no_supabase",
                "base_tags": [],
                "limit": limit or FEED_CARDS_LIMIT_DEFAULT,
                "total_candidates": 0,
                "returned": 0,
                "has_more": False,
                "next_offset": None,
                "seen": {
                    "rows": 0,
                    "exclude_ids": 0,
                    "recent_ids": 0,
                    "window_days": FEED_SEEN_EXCLUDE_DAYS,
                    "grace_minutes": FEED_SEEN_SESSION_GRACE_MINUTES,
                    "error": "no_supabase",
                    "relaxed": False,
                    "marked": 0,
                },
            }
        )
        return [], debug

    try:
        offset = int(offset)
    except (TypeError, ValueError):
        offset = 0
    if offset < 0:
        offset = 0

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(int(limit), 1), 50)
    debug["limit"] = limit
    page_index = offset // limit
    debug["page_index"] = page_index

    user_topic_weights, user_topic_rows = _load_user_topic_weights(supabase, user_id)

    base_tags = get_interest_tags_for_user(supabase, user_id)
    used_default_tags = False
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS
        used_default_tags = True

    if user_topic_rows:
        _top_tags, _, user_topics_debug = build_base_tags_from_weights(user_topic_rows)
    else:
        if user_topic_weights:
            sorted_items = sorted(user_topic_weights.items(), key=lambda kv: kv[1], reverse=True)
            user_topics_debug = {"count": len(user_topic_weights), "top": sorted_items[:20]}
        else:
            user_topics_debug = {"count": 0, "top": []}

    debug["base_tags"] = base_tags
    debug["used_default_tags"] = used_default_tags
    debug["user_topic_weights"] = user_topics_debug
    debug["topic_weights"] = user_topic_weights

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
        "relaxed": False,
        "marked": 0,
    }

    fetch_limit = (limit + offset) * 3
    fetch_limit = max(fetch_limit, limit)
    fetch_limit = min(fetch_limit, FEED_MAX_FETCH_LIMIT)

    mixed_tags = sorted({*base_tags, *DEFAULT_FEED_TAGS})

    phases_config: List[Dict[str, Any]] = [
        {"stage": "personal_recent", "tags": base_tags, "age_hours": FEED_MAX_CARD_AGE_HOURS},
    ]
    if FEED_WIDE_AGE_HOURS > FEED_MAX_CARD_AGE_HOURS:
        phases_config.append({"stage": "personal_wide", "tags": base_tags, "age_hours": FEED_WIDE_AGE_HOURS})
    if mixed_tags and mixed_tags != base_tags:
        phases_config.append({"stage": "mixed_recent", "tags": mixed_tags, "age_hours": FEED_MAX_CARD_AGE_HOURS})
        phases_config.append({"stage": "mixed_wide", "tags": mixed_tags, "age_hours": FEED_WIDE_AGE_HOURS})

    candidates_by_id: Dict[str, Dict[str, Any]] = {}
    phases_debug: List[Dict[str, Any]] = []

    def _run_phases(phases: List[Dict[str, Any]], label: str) -> None:
        nonlocal candidates_by_id, phases_debug
        for phase in phases:
            if len(candidates_by_id) >= fetch_limit:
                break

            tags = phase.get("tags") or []
            age_hours = int(phase.get("age_hours") or 0)
            stage_name = phase.get("stage") or "unknown"

            remaining = fetch_limit - len(candidates_by_id)
            if remaining <= 0:
                break

            fetched = _fetch_candidate_cards(
                supabase=supabase,
                tags=tags,
                limit=remaining,
                max_age_hours=age_hours,
                min_age_hours=0,
                before_id=None,
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
                    "label": label,
                    "tags_count": len(tags),
                    "age_hours": age_hours,
                    "fetched": len(fetched),
                    "unique_after_phase": len(candidates_by_id),
                }
            )

    _run_phases(phases_config, label="initial")

    candidates_all: List[Dict[str, Any]] = list(candidates_by_id.values())
    total_candidates_raw = len(candidates_all)
    debug["phases"] = phases_debug
    debug["total_candidates_raw_initial"] = total_candidates_raw

    required_for_page = offset + limit

    if total_candidates_raw < required_for_page and total_candidates_raw < fetch_limit:
        fallback_phases: List[Dict[str, Any]] = [
            {"stage": "any_recent_wide", "tags": [], "age_hours": FEED_WIDE_AGE_HOURS},
            {"stage": "any_all_time", "tags": [], "age_hours": FEED_DEEP_AGE_HOURS if FEED_DEEP_AGE_HOURS > 0 else 0},
        ]
        _run_phases(fallback_phases, label="fallback")
        candidates_all = list(candidates_by_id.values())
        total_candidates_raw = len(candidates_all)
        debug["total_candidates_raw_after_fallback"] = total_candidates_raw
    else:
        debug["total_candidates_raw_after_fallback"] = total_candidates_raw

    debug["total_candidates_raw"] = total_candidates_raw

    if total_candidates_raw == 0:
        if LLM_CARD_GENERATION_ENABLED and openai_is_configured():
            need_count = max(required_for_page * 2, 20)
            logger.info("No cards in DB for user_id=%s. Generating ~%d cards via OpenAI.", user_id, need_count)
            generated = generate_cards_for_tags(tags=base_tags, language="ru", count=need_count)
            if generated:
                inserted = _insert_cards_into_db(supabase, generated, language="ru", source_type="llm")
                candidates_all = inserted or []
                total_candidates_raw = len(candidates_all)
                debug["reason"] = "generated_via_openai"
                debug["generated"] = total_candidates_raw
            else:
                debug.update({"reason": "no_cards", "returned": 0, "has_more": False, "next_offset": None})
                return [], debug
        else:
            debug.update({"reason": "no_cards", "returned": 0, "has_more": False, "next_offset": None})
            return [], debug
    else:
        debug["reason"] = "cards_from_db"

    if exclude_ids:
        unseen: List[Dict[str, Any]] = []
        for c in candidates_all:
            cid = _safe_int_id(c.get("id"))
            if cid is None or cid not in exclude_ids:
                unseen.append(c)
        unseen_count = len(unseen)
    else:
        unseen = list(candidates_all)
        unseen_count = len(unseen)

    debug["removed_seen"] = total_candidates_raw - unseen_count

    if unseen_count >= required_for_page:
        candidates = unseen
        debug["seen"]["relaxed"] = False
    else:
        candidates = candidates_all
        debug["seen"]["relaxed"] = True

    debug["total_candidates"] = len(candidates)

    ranked_raw = _score_cards_for_user(candidates, base_tags, user_id=user_id, user_topic_weights=user_topic_weights)
    ranked, postprocess_debug = _apply_dedup_and_diversity(ranked_raw, base_tags)
    debug["postprocess"] = postprocess_debug
    debug["total_ranked"] = len(ranked)

    if not ranked:
        debug.update({"reason": "no_ranked_cards", "returned": 0, "has_more": False, "next_offset": None})
        return [], debug

    if offset < len(ranked):
        start = offset
        end = min(start + limit, len(ranked))
        page = ranked[start:end]
        has_more = len(ranked) > end
        next_offset = end if has_more else None
        debug["pagination_mode"] = "linear"
    else:
        page = []
        has_more = False
        next_offset = None
        debug["pagination_mode"] = "end"

    debug["returned"] = len(page)
    debug["has_more"] = has_more
    debug["next_offset"] = next_offset

    debug["seen"]["marked"] = int(_mark_cards_as_seen(supabase, user_id, page))
    return page, debug


# ===================== Cursor режим (blend: реально бесконечный) =====================

def build_feed_for_user_cursor(
    supabase: Optional[Client],
    user_id: int,
    limit: Optional[int] = None,
    cursor: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Optional[str]]:
    """
    Новый курсор по умолчанию: mode=blend
    - не "id < before_id", а "умный микс" по age buckets + персонализация + hot_tags
    - бесконечность достигается за счёт seen-фильтра + постоянного притока новых карточек
    """
    debug: Dict[str, Any] = {"cursor_in": cursor}

    if supabase is None:
        return [], {"reason": "no_supabase", **debug}, None

    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(int(limit), 1), 50)
    debug["limit"] = limit

    cur_obj = _decode_cursor_obj(cursor)
    if cur_obj is None:
        # битый cursor — начинаем заново
        cur_obj = {}
        debug["cursor_bad"] = True
    else:
        debug["cursor_bad"] = False

    mode = str(cur_obj.get("mode") or "blend")
    debug["cursor_mode"] = mode

    # --- user weights + profile tags ---
    user_topic_weights, user_topic_rows = _load_user_topic_weights(supabase, user_id)
    base_tags = get_interest_tags_for_user(supabase, user_id) or DEFAULT_FEED_TAGS
    debug["base_tags"] = base_tags

    if user_topic_rows:
        _top_tags, _, user_topics_debug = build_base_tags_from_weights(user_topic_rows)
    else:
        if user_topic_weights:
            sorted_items = sorted(user_topic_weights.items(), key=lambda kv: kv[1], reverse=True)
            user_topics_debug = {"count": len(user_topic_weights), "top": sorted_items[:20]}
        else:
            user_topics_debug = {"count": 0, "top": []}
    debug["user_topic_weights"] = user_topics_debug

    # --- seen ---
    seen_info = _load_seen_cards_for_user(supabase, user_id)
    exclude_ids: Set[int] = seen_info.get("exclude_ids") or set()
    debug["seen_rows"] = int(seen_info.get("rows") or 0)
    debug["seen_exclude"] = len(exclude_ids)

    # --- hot tags / "история" ---
    pos = _load_recent_positive_signals(supabase, user_id, limit=60)
    seed_tags = pos.get("seed_tags") or []
    hot_tags_list = _expand_with_neighbors(list(seed_tags), depth=1)
    hot_tags_set = set(hot_tags_list)
    debug["signals"] = {
        "events_rows": int(pos.get("events_rows") or 0),
        "seed_card_ids": pos.get("seed_card_ids") or [],
        "seed_tags": seed_tags,
        "hot_tags": hot_tags_list[:12],
    }

    # --- read-age stats (чтобы "не уходил в прошлое") ---
    read_stats = _load_recent_read_age_stats(supabase, user_id, limit=30)
    debug["read_age"] = read_stats

    # ===== mode=chron (legacy) =====
    if mode == "chron":
        before_id = _safe_int_id(cur_obj.get("before_id"))
        debug["before_id"] = before_id

        fetch_limit = min(max(limit * 12, 80), FEED_MAX_FETCH_LIMIT)
        mixed_tags = sorted({*base_tags, *DEFAULT_FEED_TAGS})

        phases_config: List[Dict[str, Any]] = [
            {"stage": "personal_recent", "tags": base_tags, "age_hours": FEED_MAX_CARD_AGE_HOURS},
            {"stage": "mixed_recent", "tags": mixed_tags, "age_hours": FEED_MAX_CARD_AGE_HOURS},
            {"stage": "mixed_wide", "tags": mixed_tags, "age_hours": FEED_WIDE_AGE_HOURS},
            {"stage": "any_all_time", "tags": [], "age_hours": FEED_DEEP_AGE_HOURS if FEED_DEEP_AGE_HOURS > 0 else 0},
        ]

        candidates_by_id: Dict[str, Dict[str, Any]] = {}
        phases_debug: List[Dict[str, Any]] = []

        for phase in phases_config:
            if len(candidates_by_id) >= fetch_limit:
                break
            remaining = fetch_limit - len(candidates_by_id)
            fetched = _fetch_candidate_cards(
                supabase=supabase,
                tags=phase.get("tags") or [],
                limit=remaining,
                max_age_hours=int(phase.get("age_hours") or 0),
                min_age_hours=0,
                before_id=before_id,
            )
            for card in fetched:
                cid = card.get("id")
                if cid is None:
                    continue
                key = str(cid)
                if key not in candidates_by_id:
                    candidates_by_id[key] = card

            phases_debug.append({"stage": phase.get("stage"), "fetched": len(fetched), "unique": len(candidates_by_id)})

        candidates_all = list(candidates_by_id.values())
        debug["phases"] = phases_debug
        debug["total_candidates_raw"] = len(candidates_all)

        if exclude_ids:
            unseen = []
            for c in candidates_all:
                cid = _safe_int_id(c.get("id"))
                if cid is None or cid not in exclude_ids:
                    unseen.append(c)
        else:
            unseen = candidates_all

        candidates = unseen if len(unseen) >= limit else candidates_all
        debug["seen_relaxed"] = len(unseen) < limit

        ranked_raw = _score_cards_for_user(
            candidates,
            base_tags,
            user_id=user_id,
            user_topic_weights=user_topic_weights,
            hot_tags=hot_tags_set,
        )
        ranked, postprocess_debug = _apply_dedup_and_diversity(ranked_raw, base_tags)
        debug["postprocess"] = postprocess_debug

        page = ranked[:limit]
        debug["returned"] = len(page)

        next_before = None
        if page:
            ids = [(_safe_int_id(x.get("id")) or 0) for x in page]
            next_before = min(ids) if ids else None

        next_cursor = _encode_cursor_obj({"mode": "chron", "before_id": next_before}) if next_before else None
        debug["seen_marked"] = int(_mark_cards_as_seen(supabase, user_id, page))
        return page, debug, next_cursor

    # ===== mode=blend (NEW DEFAULT) =====
    seq = _safe_int_id(cur_obj.get("seq")) or 0
    seed = str(cur_obj.get("seed") or datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    debug["seq"] = seq
    debug["seed"] = seed

    # 1) строим "план" по возрастным бакетам
    plan, plan_dbg = _build_age_bucket_plan(limit=limit, read_avg_age_hours=read_stats.get("avg_age_hours"))
    debug["age_plan"] = plan_dbg

    # 2) какие теги используем для retrieval
    #    - персональные (профиль)
    #    - hot (продолжения/смежные)
    #    - дефолтные (чтобы лента не пустела)
    tags_personal = _unique_keep_order(base_tags)
    tags_hot = _unique_keep_order(hot_tags_list)
    tags_mixed = _unique_keep_order(tags_personal + tags_hot + DEFAULT_FEED_TAGS)
    tags_query = _expand_with_neighbors(tags_mixed, depth=1)

    debug["tags_query"] = {
        "personal": tags_personal[:10],
        "hot": tags_hot[:12],
        "mixed_count": len(tags_mixed),
        "query_count": len(tags_query),
    }

    # 3) набираем кандидатов из бакетов (и чуть запасаем, чтобы дедуп/диверсификация не убили страницу)
    candidates_by_id: Dict[str, Dict[str, Any]] = {}
    stages: List[Dict[str, Any]] = []
    fetch_cap = min(max(limit * 25, 250), FEED_MAX_FETCH_LIMIT)

    def _add_fetched(stage: str, fetched: List[Dict[str, Any]]) -> None:
        before = len(candidates_by_id)
        for card in fetched:
            cid = card.get("id")
            if cid is None:
                continue
            key = str(cid)
            if key not in candidates_by_id:
                candidates_by_id[key] = card
        stages.append({"stage": stage, "fetched": len(fetched), "unique_after": len(candidates_by_id), "added": len(candidates_by_id) - before})

    # 3.1) бакеты в пределах 7 дней (news cap)
    for b in plan:
        if len(candidates_by_id) >= fetch_cap:
            break
        need = int(b.get("count") or 0)
        if need <= 0:
            continue
        remaining = fetch_cap - len(candidates_by_id)
        take = min(remaining, max(need * 18, 60))

        fetched = _fetch_candidate_cards(
            supabase=supabase,
            tags=tags_query,
            limit=take,
            max_age_hours=int(b["max_age"]),
            min_age_hours=int(b["min_age"]),
            before_id=None,
        )
        _add_fetched(f"bucket:{b['name']}", fetched)

    # 3.2) fallback если всё равно мало (в пределах wide, но потом мы всё равно отфильтруем старые news)
    if len(candidates_by_id) < max(limit * 6, 120) and len(candidates_by_id) < fetch_cap:
        remaining = fetch_cap - len(candidates_by_id)
        take = min(remaining, max(limit * 20, 160))
        fetched = _fetch_candidate_cards(
            supabase=supabase,
            tags=tags_query,
            limit=take,
            max_age_hours=FEED_WIDE_AGE_HOURS,
            min_age_hours=0,
            before_id=None,
        )
        _add_fetched("fallback:wide", fetched)

    candidates_all = list(candidates_by_id.values())
    debug["retrieval"] = {
        "fetch_cap": fetch_cap,
        "unique_candidates": len(candidates_all),
        "stages": stages,
    }

    # 4) жёсткий фильтр "news не старше 7 дней"
    #    wikipedia оставляем, даже если старее (но обычно она и так свежая по created_at)
    now = datetime.now(timezone.utc)
    news_cutoff = now - timedelta(hours=FEED_MAX_CARD_AGE_HOURS)

    filtered_time: List[Dict[str, Any]] = []
    dropped_old_news = 0
    for c in candidates_all:
        if _is_time_sensitive_news(c):
            ca = c.get("created_at")
            dt = None
            if isinstance(ca, str):
                try:
                    dt = datetime.fromisoformat(ca.replace("Z", "+00:00"))
                except Exception:
                    dt = None
            if dt is not None and dt < news_cutoff:
                dropped_old_news += 1
                continue
        filtered_time.append(c)
    debug["dropped_old_news"] = dropped_old_news

    # 5) seen filter
    if exclude_ids:
        unseen: List[Dict[str, Any]] = []
        for c in filtered_time:
            cid = _safe_int_id(c.get("id"))
            if cid is None or cid not in exclude_ids:
                unseen.append(c)
    else:
        unseen = list(filtered_time)

    candidates = unseen if len(unseen) >= limit else filtered_time
    debug["seen_relaxed"] = len(unseen) < limit
    debug["candidates_after_seen"] = len(candidates)

    # 6) если совсем пусто — OpenAI (как крайний спасатель)
    if not candidates:
        if LLM_CARD_GENERATION_ENABLED and openai_is_configured():
            need_count = max(limit * 3, 20)
            logger.info("No candidates for user_id=%s. Generating ~%d cards via OpenAI.", user_id, need_count)
            generated = generate_cards_for_tags(tags=base_tags, language="ru", count=need_count)
            if generated:
                inserted = _insert_cards_into_db(supabase, generated, language="ru", source_type="llm")
                candidates = inserted or []
                debug["reason"] = "generated_via_openai"
                debug["generated"] = len(candidates)
            else:
                debug["reason"] = "no_cards"
                return [], debug, _encode_cursor_obj({"mode": "blend", "seq": seq + 1, "seed": seed})
        else:
            debug["reason"] = "no_cards"
            return [], debug, _encode_cursor_obj({"mode": "blend", "seq": seq + 1, "seed": seed})
    else:
        debug["reason"] = "blend_from_db"

    # 7) rank + postprocess (важно: hot_tags влияет на "продолжения/смежность")
    ranked_raw = _score_cards_for_user(
        candidates,
        base_tags,
        user_id=user_id,
        user_topic_weights=user_topic_weights,
        hot_tags=hot_tags_set,
    )
    ranked, postprocess_debug = _apply_dedup_and_diversity(ranked_raw, base_tags, max_consecutive_source=2, max_consecutive_tag=2)
    debug["postprocess"] = postprocess_debug
    debug["ranked_total"] = len(ranked)

    page = ranked[:limit]
    debug["returned"] = len(page)

    # 8) next cursor: всегда отдаём следующий seq (blend бесконечный)
    next_cursor = _encode_cursor_obj({"mode": "blend", "seq": seq + 1, "seed": seed})

    debug["seen_marked"] = int(_mark_cards_as_seen(supabase, user_id, page))
    return page, debug, next_cursor


# ===================== Public wrapper =====================

def build_feed_for_user_paginated(
    supabase: Optional[Client],
    user_id: int,
    limit: Optional[int] = None,
    offset: int = 0,
    cursor: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    if limit is None or limit <= 0:
        limit = FEED_CARDS_LIMIT_DEFAULT
    limit = min(max(limit, 1), 50)
    offset = max(0, int(offset))

    if cursor is not None:
        items, debug, next_cursor = build_feed_for_user_cursor(
            supabase=supabase, user_id=user_id, limit=limit, cursor=cursor
        )
        cursor_meta = {
            "mode": "cursor",
            "cursor": cursor,
            "next_cursor": next_cursor,
            "limit": limit,
            "has_more": True,  # blend считаем бесконечным
        }
        return items, debug, cursor_meta

    items, base_debug = build_feed_for_user(
        supabase=supabase,
        user_id=user_id,
        limit=limit,
        offset=offset,
    )

    has_more = bool(base_debug.get("has_more"))
    next_offset = base_debug.get("next_offset")

    cursor_meta = {
        "mode": "offset",
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

    return items, debug, cursor_meta

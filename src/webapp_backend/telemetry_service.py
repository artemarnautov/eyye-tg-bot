# file: src/webapp_backend/telemetry_service.py
import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Literal, Tuple

from pydantic import BaseModel, Field
from supabase import Client

logger = logging.getLogger(__name__)

# ==============================
# Pydantic-модели запросов
# ==============================

# Канонические типы событий, которые используем для модели интересов.
EventType = Literal["view", "like", "dislike", "open_source"]


class Event(BaseModel):
    """
    Одно событие от фронта.

    type:
      - "view"        — просмотр карточки (важен dwell_ms)
      - "like"        — юзер явно лайкнул карточку
      - "dislike"     — юзер явно скрыл/дизлайкнул карточку
      - "open_source" — юзер ткнул на источник (читать подробнее)

    card_id: ID карточки из таблицы cards.
    ts: когда событие произошло (если нет — подставим server now()).
    dwell_ms: длительность просмотра карточки в миллисекундах (важно для type="view").

    Доп. поля (мы их можем принять, но пока не используем для весов):
    position, source, extra — приходят из webapp/telemetry.js
    """

    type: EventType
    card_id: int
    ts: Optional[datetime] = Field(default=None)
    dwell_ms: Optional[int] = Field(default=None, ge=0)

    # optional extras from frontend
    position: Optional[int] = None
    source: Optional[str] = None
    extra: Optional[Any] = None

    # pydantic v2
    model_config = {"extra": "ignore"}

    # pydantic v1
    class Config:
        extra = "ignore"


class EventsRequest(BaseModel):
    """
    Тело POST /api/events.
    tg_id: Telegram ID пользователя.
    events: список событий.
    """

    tg_id: int
    events: List[Event]

    model_config = {"extra": "ignore"}

    class Config:
        extra = "ignore"


# ==============================
# ENV / тюнинг телеметрии
# ==============================

def _env_int(name: str, default: int, min_v: Optional[int] = None, max_v: Optional[int] = None) -> int:
    try:
        v = int(os.getenv(name, str(default)))
    except Exception:
        v = default
    if min_v is not None:
        v = max(min_v, v)
    if max_v is not None:
        v = min(max_v, v)
    return v


def _env_float(name: str, default: float, min_v: Optional[float] = None, max_v: Optional[float] = None) -> float:
    try:
        v = float(os.getenv(name, str(default)))
    except Exception:
        v = default
    if min_v is not None:
        v = max(min_v, v)
    if max_v is not None:
        v = min(max_v, v)
    return v


TELEMETRY_SEEN_MIN_DWELL_MS = _env_int("TELEMETRY_SEEN_MIN_DWELL_MS", 400, 0, 60000)
TELEMETRY_MAX_DWELL_MS = _env_int("TELEMETRY_MAX_DWELL_MS", 120000, 5000, 600000)

DEFAULT_READING_WPM = _env_int("TELEMETRY_DEFAULT_READING_WPM", 210, 80, 450)
READING_WPM_EMA_ALPHA = _env_float("TELEMETRY_READING_WPM_EMA_ALPHA", 0.15, 0.01, 0.50)

READING_WPM_MIN = _env_int("TELEMETRY_READING_WPM_MIN", 90, 60, 200)
READING_WPM_MAX = _env_int("TELEMETRY_READING_WPM_MAX", 380, 220, 600)


# ==============================
# Вспомогалки
# ==============================

_WORD_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9]+")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, v))


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _count_words(text: str) -> int:
    if not text:
        return 0
    return len(_WORD_RE.findall(text))


def _estimate_expected_read_ms(
    *,
    title: str,
    body: str,
    reading_wpm: float,
) -> int:
    wpm = float(reading_wpm or DEFAULT_READING_WPM)
    wpm = max(60.0, min(600.0, wpm))

    words_total = _count_words(title) + _count_words(body)
    effective_words = min(words_total, 260)

    base_ms = 900
    expected_ms = base_ms + int((effective_words / wpm) * 60_000)

    expected_ms = _clamp_int(expected_ms, 900, 25000)
    return expected_ms


def _dedupe_events(events: List[Event]) -> List[Event]:
    if not events:
        return []

    view_by_card: Dict[int, Event] = {}
    last_by_key: Dict[Tuple[str, int], Event] = {}

    for ev in events:
        try:
            cid = int(ev.card_id)
        except Exception:
            continue

        if ev.type == "view":
            prev = view_by_card.get(cid)
            cur_dwell = _safe_int(ev.dwell_ms, 0)
            if cur_dwell < 0:
                cur_dwell = 0
            cur_dwell = min(cur_dwell, TELEMETRY_MAX_DWELL_MS)
            ev.dwell_ms = cur_dwell

            if prev is None:
                view_by_card[cid] = ev
            else:
                prev_dwell = _safe_int(prev.dwell_ms, 0)
                if cur_dwell > prev_dwell:
                    view_by_card[cid] = ev
            continue

        key = (ev.type, cid)
        prev = last_by_key.get(key)
        if prev is None:
            last_by_key[key] = ev
        else:
            if ev.ts and prev.ts:
                if ev.ts >= prev.ts:
                    last_by_key[key] = ev
            else:
                last_by_key[key] = ev

    out: List[Event] = []
    out.extend(view_by_card.values())
    out.extend(last_by_key.values())
    return out


# ==============================
# Работа с БД: cards features
# ==============================

def _fetch_cards_features(
    supabase: Client,
    card_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    if not card_ids:
        return {}

    unique_ids = sorted(set(int(x) for x in card_ids if isinstance(x, int) or str(x).isdigit()))
    if not unique_ids:
        return {}

    try:
        resp = (
            supabase.table("cards")
            .select("id,tags,title,body,language,created_at")
            .in_("id", unique_ids)
            .execute()
        )
    except Exception:
        logger.exception("Failed to fetch cards features for ids=%s", unique_ids)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []

    by_id: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        cid = row.get("id")
        if cid is None:
            continue

        tags_raw = row.get("tags") or []
        if not isinstance(tags_raw, list):
            tags_raw = [tags_raw]
        tags_clean: List[str] = []
        for t in tags_raw:
            s = str(t).strip().lower()
            if s:
                tags_clean.append(s)

        by_id[int(cid)] = {
            "id": int(cid),
            "tags": tags_clean,
            "title": str(row.get("title") or ""),
            "body": str(row.get("body") or ""),
            "language": str(row.get("language") or "").strip().lower() or None,
            "created_at": row.get("created_at"),
        }

    return by_id


# ==============================
# Персональная скорость чтения (best-effort)
# ==============================

def _load_user_reading_profile(supabase: Client, tg_id: int) -> Dict[str, Any]:
    out = {"wpm": float(DEFAULT_READING_WPM), "samples": 0, "key": None, "raw_profile": None}

    for key in ("tg_id", "user_id"):
        try:
            resp = (
                supabase.table("user_profiles")
                .select("structured_profile")
                .eq(key, tg_id)
                .limit(1)
                .execute()
            )
            data = getattr(resp, "data", None)
            if data is None:
                data = getattr(resp, "model", None)
            rows = data or []
            if not rows:
                continue

            prof = rows[0].get("structured_profile") or {}
            if not isinstance(prof, dict):
                prof = {}

            tel = prof.get("telemetry") or {}
            if not isinstance(tel, dict):
                tel = {}

            wpm = tel.get("reading_wpm")
            samples = tel.get("reading_samples")

            try:
                wpm_f = float(wpm) if wpm is not None else float(DEFAULT_READING_WPM)
            except Exception:
                wpm_f = float(DEFAULT_READING_WPM)
            wpm_f = max(float(READING_WPM_MIN), min(float(READING_WPM_MAX), wpm_f))

            out.update(
                {
                    "wpm": wpm_f,
                    "samples": _safe_int(samples, 0),
                    "key": key,
                    "raw_profile": prof,
                }
            )
            return out
        except Exception:
            continue

    return out


def _maybe_update_user_reading_profile(
    supabase: Client,
    tg_id: int,
    *,
    current_profile: Dict[str, Any],
    observed_wpm: Optional[float],
) -> None:
    if observed_wpm is None:
        return

    key = current_profile.get("key")
    prof = current_profile.get("raw_profile")
    if key is None or not isinstance(prof, dict):
        return

    old_wpm = float(current_profile.get("wpm") or DEFAULT_READING_WPM)
    old_wpm = max(float(READING_WPM_MIN), min(float(READING_WPM_MAX), old_wpm))

    obs = float(observed_wpm)
    obs = max(float(READING_WPM_MIN), min(float(READING_WPM_MAX), obs))

    alpha = float(READING_WPM_EMA_ALPHA)
    new_wpm = (1.0 - alpha) * old_wpm + alpha * obs

    tel = prof.get("telemetry")
    if not isinstance(tel, dict):
        tel = {}
        prof["telemetry"] = tel

    samples = _safe_int(tel.get("reading_samples"), 0)
    tel["reading_wpm"] = round(new_wpm, 2)
    tel["reading_samples"] = samples + 1
    tel["reading_updated_at"] = _now_utc().isoformat()

    try:
        supabase.table("user_profiles").update({"structured_profile": prof}).eq(key, tg_id).execute()
    except Exception:
        return


# ==============================
# Скоринг сигналов
# ==============================

def _view_signal_delta(
    *,
    dwell_ms: Optional[int],
    expected_ms: int,
) -> float:
    if dwell_ms is None:
        return 0.0

    d = _clamp_int(int(dwell_ms), 0, TELEMETRY_MAX_DWELL_MS)
    exp = max(900, int(expected_ms))

    if d < 700:
        return -0.7
    if d < 1200:
        return -0.35

    ratio = d / float(exp)

    if ratio < 0.12:
        return -0.35
    if ratio < 0.30:
        return -0.10
    if ratio < 0.60:
        return 0.18
    if ratio < 1.05:
        return 0.85
    if ratio < 1.60:
        return 1.10

    return 1.25


def _delta_for_event(
    ev: Event,
    *,
    card_features: Optional[Dict[str, Any]] = None,
    reading_wpm: float = DEFAULT_READING_WPM,
) -> float:
    if ev.type == "view":
        title = str((card_features or {}).get("title") or "")
        body = str((card_features or {}).get("body") or "")
        expected_ms = _estimate_expected_read_ms(title=title, body=body, reading_wpm=reading_wpm)
        return _view_signal_delta(dwell_ms=ev.dwell_ms, expected_ms=expected_ms)

    if ev.type == "like":
        return 2.0
    if ev.type == "dislike":
        return -2.0
    if ev.type == "open_source":
        return 1.5

    return 0.0


def _extract_observed_wpm_for_profile_update(
    *,
    dwell_ms: Optional[int],
    title: str,
    body: str,
) -> Optional[float]:
    if dwell_ms is None:
        return None
    d = int(dwell_ms)
    if d < 3000 or d > 60000:
        return None

    words = _count_words(title) + _count_words(body)
    if words < 18:
        return None

    minutes = d / 60000.0
    if minutes <= 0:
        return None

    obs_wpm = words / minutes
    if obs_wpm < float(READING_WPM_MIN) or obs_wpm > float(READING_WPM_MAX):
        return None

    return float(obs_wpm)


# ==============================
# user_topic_weights update
# ==============================

def _update_user_topic_weights(
    supabase: Client,
    tg_id: int,
    tag_deltas: Dict[str, float],
) -> None:
    if not tag_deltas:
        return

    try:
        resp = (
            supabase.table("user_topic_weights")
            .select("tag,weight")
            .eq("tg_id", tg_id)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load user_topic_weights for tg_id=%s", tg_id)
        return

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []

    current: Dict[str, float] = {}
    for row in rows:
        tag = str(row.get("tag") or "").strip().lower()
        if not tag:
            continue
        try:
            w = float(row.get("weight") or 0.0)
        except (TypeError, ValueError):
            w = 0.0
        current[tag] = w

    for tag, delta in tag_deltas.items():
        tag_norm = tag.strip().lower()
        if not tag_norm:
            continue

        old = current.get(tag_norm, 0.0)
        new = old + float(delta)

        if new > 10.0:
            new = 10.0
        elif new < -10.0:
            new = -10.0

        try:
            if tag_norm in current:
                (
                    supabase.table("user_topic_weights")
                    .update({"weight": new})
                    .eq("tg_id", tg_id)
                    .eq("tag", tag_norm)
                    .execute()
                )
            else:
                supabase.table("user_topic_weights").insert({"tg_id": tg_id, "tag": tag_norm, "weight": new}).execute()
        except Exception:
            logger.exception("Failed to upsert user_topic_weights for tg_id=%s, tag=%r", tg_id, tag_norm)

    logger.info("Updated user_topic_weights for tg_id=%s, tags=%d", tg_id, len(tag_deltas))


# ==============================
# user_events insert (сырые логи)
# ==============================

def _insert_user_events(
    supabase: Client,
    tg_id: int,
    events: List[Event],
) -> None:
    if not events:
        return

    payload: List[Dict[str, Any]] = []
    for ev in events:
        row: Dict[str, Any] = {
            "tg_id": tg_id,
            "card_id": int(ev.card_id),
            "event_type": ev.type,
        }
        if ev.dwell_ms is not None:
            row["dwell_ms"] = int(_clamp_int(int(ev.dwell_ms), 0, TELEMETRY_MAX_DWELL_MS))
        payload.append(row)

    if not payload:
        return

    try:
        supabase.table("user_events").insert(payload).execute()
    except Exception as e:
        msg = str(e)
        if "PGRST204" in msg or "event_ts" in msg:
            logger.warning("user_events schema mismatch (skipping insert): %s", msg)
            return
        logger.exception("Failed to insert user_events for tg_id=%s", tg_id)


# ==============================
# user_seen_cards upsert
# ==============================

def _insert_seen_cards_from_events(
    supabase: Client,
    tg_id: int,
    events: List[Event],
) -> None:
    if not events:
        return

    now = _now_utc()
    payload: List[Dict[str, Any]] = []

    for ev in events:
        if ev.type != "view":
            continue

        dwell = int(ev.dwell_ms or 0)
        dwell = _clamp_int(dwell, 0, TELEMETRY_MAX_DWELL_MS)

        if dwell < int(TELEMETRY_SEEN_MIN_DWELL_MS):
            continue

        ts = ev.ts or now
        payload.append({"user_id": tg_id, "card_id": int(ev.card_id), "seen_at": ts.isoformat()})

    if not payload:
        return

    try:
        supabase.table("user_seen_cards").upsert(payload, on_conflict="user_id,card_id").execute()
    except Exception:
        logger.exception("Failed to upsert into user_seen_cards for tg_id=%s (rows=%d)", tg_id, len(payload))


# ==============================
# Публичная функция /api/events
# ==============================

def log_events(supabase: Client, payload: EventsRequest) -> None:
    if supabase is None:
        logger.warning("Supabase is None in log_events, skipping")
        return

    tg_id = int(payload.tg_id)
    events_in = payload.events or []
    if not events_in:
        logger.info("log_events called with empty events list (tg_id=%s)", tg_id)
        return

    events = _dedupe_events(events_in)

    _insert_user_events(supabase, tg_id, events)

    card_ids = [int(e.card_id) for e in events]
    cards_by_id = _fetch_cards_features(supabase, card_ids)

    reading_profile = _load_user_reading_profile(supabase, tg_id)
    reading_wpm = float(reading_profile.get("wpm") or DEFAULT_READING_WPM)

    tag_deltas: Dict[str, float] = defaultdict(float)
    best_observed_wpm: Optional[float] = None

    for ev in events:
        card = cards_by_id.get(int(ev.card_id)) or {}
        tags = card.get("tags") or []
        if not isinstance(tags, list):
            tags = []

        delta = _delta_for_event(ev, card_features=card, reading_wpm=reading_wpm)
        if delta != 0.0 and tags:
            for tag in tags:
                tag_norm = str(tag).strip().lower()
                if tag_norm:
                    tag_deltas[tag_norm] += float(delta)

        if ev.type == "view":
            obs = _extract_observed_wpm_for_profile_update(
                dwell_ms=ev.dwell_ms,
                title=str(card.get("title") or ""),
                body=str(card.get("body") or ""),
            )
            if obs is not None:
                if best_observed_wpm is None:
                    best_observed_wpm = obs
                else:
                    if abs(obs - reading_wpm) < abs(best_observed_wpm - reading_wpm):
                        best_observed_wpm = obs

    if tag_deltas:
        _update_user_topic_weights(supabase, tg_id, dict(tag_deltas))

    _insert_seen_cards_from_events(supabase, tg_id, events)

    _maybe_update_user_reading_profile(supabase, tg_id, current_profile=reading_profile, observed_wpm=best_observed_wpm)

    logger.info(
        "Processed events tg_id=%s: raw=%d dedup=%d tags_with_delta=%d reading_wpm=%.1f observed=%s",
        tg_id,
        len(events_in),
        len(events),
        len(tag_deltas),
        reading_wpm,
        None if best_observed_wpm is None else round(best_observed_wpm, 1),
    )

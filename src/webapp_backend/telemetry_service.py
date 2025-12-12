import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Literal

from pydantic import BaseModel, Field
from supabase import Client

logger = logging.getLogger(__name__)

# ==============================
# Pydantic-модели запросов
# ==============================

# Типы событий, которые понимает backend.
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
    dwell_ms: длительность просмотра карточки в миллисекундах
              (нужна только для type="view").
    """

    type: EventType
    card_id: int
    ts: Optional[datetime] = Field(
        default=None,
        description="Время события, если не передано — подставим текущее (UTC).",
    )
    dwell_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Длительность просмотра карточки в миллисекундах.",
    )


class EventsRequest(BaseModel):
    """
    Тело POST /api/events.

    tg_id: Telegram ID пользователя.
    events: список событий.
    """

    tg_id: int
    events: List[Event]


# ==============================
# Вспомогалки для работы с БД
# ==============================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fetch_tags_for_cards(
    supabase: Client,
    card_ids: List[int],
) -> Dict[int, List[str]]:
    """
    Забираем теги для пачки карточек из таблицы cards.

    Возвращаем словарь: {card_id: [tag1, tag2, ...]}.
    """
    if not card_ids:
        return {}

    # Убираем дубликаты, чтобы не долбить БД лишний раз
    unique_ids = sorted(set(card_ids))

    try:
        resp = (
            supabase.table("cards")
            .select("id,tags")
            .in_("id", unique_ids)
            .execute()
        )
    except Exception:
        logger.exception("Failed to fetch card tags for ids=%s", unique_ids)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []

    by_id: Dict[int, List[str]] = {}
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

        by_id[int(cid)] = tags_clean

    return by_id


def _delta_for_view(dwell_ms: Optional[int]) -> float:
    """
    Конвертируем просмотр с определённым dwell_ms в изменение веса интереса.

    Новая логика:

    - < 2 секунд  : лёгкий минус (юзер почти сразу пролистнул)
    - 2–4 секунды : нейтрально (увидел карточку, но не зацепило и не раздражает)
    - 4–10 секунд : нормальное чтение карточки → уверенный плюс
    - > 10 секунд : очень сильный интерес → сильный плюс

    Коэффициенты подобраны так, чтобы:
    - явные действия (like/dislike) оставались сильнее одного просмотра;
    - длительные просмотры всё равно заметно двигали веса по тегам.
    """
    if dwell_ms is None:
        return 0.0

    # < 2 секунд — лёгкий негативный сигнал
    if dwell_ms < 2000:
        return -0.2

    # 2–4 секунды — нейтрально
    if dwell_ms < 4000:
        return 0.0

    # 4–10 секунд — нормальный плюс (прочитал карточку)
    if dwell_ms < 10000:
        return 0.6

    # > 10 секунд — сильный интерес
    return 1.2


def _delta_for_event(ev: Event) -> float:
    """
    Переводим событие в dW по тегам карточки.

    Все "магические коэффициенты" собраны здесь, чтобы потом было удобно
    тюнить поведение фида без переписывания логики.
    """
    if ev.type == "view":
        return _delta_for_view(ev.dwell_ms or 0)

    if ev.type == "like":
        # Явный лайк — сильный плюс.
        return 2.0

    if ev.type == "dislike":
        # Явный дизлайк/скрытие — сильный минус.
        return -2.0

    if ev.type == "open_source":
        # Клик по источнику — сильный интерес к теме.
        return 1.5

    # На всякий случай: неизвестный тип события не влияет на веса.
    return 0.0


def _update_user_topic_weights(
    supabase: Client,
    tg_id: int,
    tag_deltas: Dict[str, float],
) -> None:
    """
    Применяем накопленные dW по тегам к user_topic_weights.

    Схема user_topic_weights предполагается такой:
    - tg_id (int/bigint)
    - tag (text)
    - weight (float8)
    """
    if not tag_deltas:
        return

    # 1. Читаем текущие веса по пользователю
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

    # 2. Применяем дельты и пишем обратно
    for tag, delta in tag_deltas.items():
        tag_norm = tag.strip().lower()
        if not tag_norm:
            continue

        old = current.get(tag_norm, 0.0)
        new = old + delta

        # Лёгкий кламп, чтобы веса не улетели в космос
        if new > 10.0:
            new = 10.0
        elif new < -10.0:
            new = -10.0

        try:
            if tag_norm in current:
                # Уже есть строка — обновляем
                supabase.table("user_topic_weights").update(
                    {"weight": new}
                ).eq("tg_id", tg_id).eq("tag", tag_norm).execute()
            else:
                # Нет строки — создаём
                supabase.table("user_topic_weights").insert(
                    {"tg_id": tg_id, "tag": tag_norm, "weight": new}
                ).execute()
        except Exception:
            logger.exception(
                "Failed to upsert user_topic_weights for tg_id=%s, tag=%r",
                tg_id,
                tag_norm,
            )

    logger.info(
        "Updated user_topic_weights for tg_id=%s, tags=%d",
        tg_id,
        len(tag_deltas),
    )


def _insert_user_events(
    supabase: Client,
    tg_id: int,
    events: List[Event],
) -> None:
    """
    Пишем сырые события в таблицу user_events.

    ВАЖНО (фикс текущего прод-бага):
    - НЕ отправляем event_ts, потому что в твоей таблице user_events сейчас
      НЕТ колонки event_ts (PGRST204).
    - Полагаемся на дефолтный created_at в БД (если он есть).
    """
    if not events:
        return

    payload: List[Dict[str, Any]] = []
    for ev in events:
        row: Dict[str, Any] = {
            "tg_id": tg_id,
            "card_id": ev.card_id,
            "event_type": ev.type,
        }
        # dwell_ms полезен только для view, но колонка может быть nullable — оставляем как есть
        if ev.dwell_ms is not None:
            row["dwell_ms"] = int(ev.dwell_ms)
        payload.append(row)

    if not payload:
        return

    try:
        supabase.table("user_events").insert(payload).execute()
    except Exception as e:
        # Телеметрия не должна ломать UX и не должна спамить огромными трейсами из-за схемы.
        msg = str(e)
        if "PGRST204" in msg or "event_ts" in msg:
            logger.warning("user_events schema mismatch (skipping insert): %s", msg)
            return
        logger.exception("Failed to insert user_events for tg_id=%s", tg_id)


def _insert_seen_cards_from_events(
    supabase: Client,
    tg_id: int,
    events: List[Event],
) -> None:
    """
    Помечаем карточки как увиденные в user_seen_cards,
    если есть событие view с dwell >= 1 секунды.

    FIX:
    - Используем upsert по (user_id, card_id), чтобы не ловить 409 duplicate key.
    """
    if not events:
        return

    now = _now_utc()
    payload: List[Dict[str, Any]] = []

    for ev in events:
        if ev.type != "view":
            continue
        dwell = ev.dwell_ms or 0
        # Карточка считается увиденной при dwell >= 1 секунды
        if dwell < 1000:
            continue

        ts = ev.ts or now
        payload.append(
            {
                "user_id": tg_id,
                "card_id": ev.card_id,
                "seen_at": ts.isoformat(),
            }
        )

    if not payload:
        return

    try:
        supabase.table("user_seen_cards").upsert(
            payload,
            on_conflict="user_id,card_id",
        ).execute()
    except Exception:
        logger.exception(
            "Failed to upsert into user_seen_cards for tg_id=%s (rows=%d)",
            tg_id,
            len(payload),
        )


# ==============================
# Публичная функция для /api/events
# ==============================


def log_events(supabase: Client, payload: EventsRequest) -> None:
    """
    Обрабатываем батч событий от фронта:

    1) Пишем сами события в user_events (сырые логи).
    2) На их основе считаем dW по тегам и обновляем user_topic_weights.
    3) Помечаем карточки как увиденные (user_seen_cards) при view + dwell >= 1 сек.

    Важно: функция специально максимально "мягкая":
    любые ошибки логируются, но не роняют процесс.
    """
    if supabase is None:
        logger.warning("Supabase is None in log_events, skipping")
        return

    tg_id = int(payload.tg_id)
    events = payload.events or []

    if not events:
        logger.info("log_events called with empty events list (tg_id=%s)", tg_id)
        return

    # 1. Пишем сырые события
    _insert_user_events(supabase, tg_id, events)

    # 2. Загружаем теги карточек и накапливаем дельты по тегам
    card_ids = [e.card_id for e in events]
    tags_by_card = _fetch_tags_for_cards(supabase, card_ids)

    tag_deltas: Dict[str, float] = defaultdict(float)

    for ev in events:
        tags = tags_by_card.get(ev.card_id) or []
        if not tags:
            continue

        delta = _delta_for_event(ev)
        if delta == 0.0:
            continue

        for tag in tags:
            tag_norm = str(tag).strip().lower()
            if not tag_norm:
                continue
            tag_deltas[tag_norm] += delta

    # 3. Применяем изменения к user_topic_weights
    if tag_deltas:
        _update_user_topic_weights(supabase, tg_id, dict(tag_deltas))

    # 4. Помечаем карточки как увиденные (для фильтрации в cards_service)
    _insert_seen_cards_from_events(supabase, tg_id, events)

    logger.info(
        "Processed %d events for tg_id=%s (tags_with_delta=%d)",
        len(events),
        tg_id,
        len(tag_deltas),
    )

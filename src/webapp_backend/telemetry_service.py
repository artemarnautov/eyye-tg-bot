# file: src/webapp_backend/telemetry_service.py

from datetime import datetime
from typing import Any, Iterable, Literal, List

from pydantic import BaseModel
from supabase import Client


class EventPayload(BaseModel):
    card_id: int
    event_type: Literal[
        "impression",
        "view",
        "swipe_next",
        "click_more",
        "click_source",
        "like",
        "dislike",
        "share",
    ]
    dwell_ms: int | None = None
    position: int | None = None
    source: str | None = None
    extra: dict[str, Any] | None = None


class EventsRequest(BaseModel):
    tg_id: int
    events: List[EventPayload]


POSITIVE_EVENTS = {"view", "click_more", "click_source", "like", "share"}
NEGATIVE_EVENTS = {"swipe_next", "dislike"}


def log_events(supabase: Client | None, payload: EventsRequest) -> None:
    """
    1) Пишем события в user_events.
    2) Обновляем веса интересов пользователя в user_topic_weights.
    """
    if supabase is None:
        # На уровне /api/events мы и так кидаем 500, но тут на всякий случай
        return

    if not payload.events:
        return

    rows = [
        {
            "tg_id": payload.tg_id,
            "card_id": ev.card_id,
            "event_type": ev.event_type,
            "dwell_ms": ev.dwell_ms,
            "position": ev.position,
            "source": ev.source,
            "extra": ev.extra,
        }
        for ev in payload.events
    ]

    # 1. Логируем события
    supabase.table("user_events").insert(rows).execute()

    # 2. Обновляем веса по тегам
    _update_user_topic_weights(supabase, payload.tg_id, payload.events)


def _event_delta(ev: EventPayload) -> float:
    """Переводим тип события + длительность просмотра в числовой сигнал."""
    base = {
        "impression": 0.1,
        "view": 1.0,
        "swipe_next": -0.7,
        "click_more": 1.5,
        "click_source": 1.5,
        "like": 3.0,
        "dislike": -4.0,
        "share": 3.5,
    }.get(ev.event_type, 0.0)

    dwell_ms = ev.dwell_ms or 0
    # до ~4 секунд → множитель ~1, до 8+ сек → максимум ~2
    if dwell_ms > 0:
        dwell_factor = max(0.5, min(2.0, dwell_ms / 4000.0))
    else:
        dwell_factor = 1.0

    return base * dwell_factor


def _update_user_topic_weights(
    supabase: Client,
    tg_id: int,
    events: Iterable[EventPayload],
) -> None:
    """Инкрементально обновляем веса user_topic_weights по тегам карточек."""

    meaningful_events = [
        ev for ev in events if ev.event_type in (POSITIVE_EVENTS | NEGATIVE_EVENTS)
    ]
    if not meaningful_events:
        return

    # 1. Собираем ID карточек, по которым были сигналы
    card_ids = list({ev.card_id for ev in meaningful_events})
    if not card_ids:
        return

    cards_resp = (
        supabase.table("cards")
        .select("id,tags")
        .in_("id", card_ids)
        .execute()
    )
    cards_by_id = {row["id"]: row for row in (cards_resp.data or [])}

    # 2. Накапливаем дельты по тегам
    deltas: dict[str, float] = {}

    for ev in meaningful_events:
        card = cards_by_id.get(ev.card_id)
        if not card:
            continue

        tags = card.get("tags") or []
        if not tags:
            continue

        delta = _event_delta(ev)
        if delta == 0.0:
            continue

        for tag in tags:
            deltas[tag] = deltas.get(tag, 0.0) + delta

    if not deltas:
        return

    # 3. Тянем текущие веса
    existing_resp = (
        supabase.table("user_topic_weights")
        .select("tag,weight")
        .eq("tg_id", tg_id)
        .in_("tag", list(deltas.keys()))
        .execute()
    )
    existing = {row["tag"]: row["weight"] for row in (existing_resp.data or [])}

    # 4. Обновляем веса с небольшим learning rate и клиппингом
    lr = 0.1
    now = datetime.utcnow().isoformat()

    upsert_rows = []
    for tag, delta in deltas.items():
        old_w = existing.get(tag, 0.0)
        new_w = old_w + lr * delta
        new_w = max(-5.0, min(5.0, new_w))  # клип, чтобы не улетать

        upsert_rows.append(
            {
                "tg_id": tg_id,
                "tag": tag,
                "weight": new_w,
                "updated_at": now,
            }
        )

    supabase.table("user_topic_weights").upsert(
        upsert_rows,
        on_conflict="tg_id,tag",
    ).execute()

# file: src/telegram_ingest/process_telegram_posts.py
import os
import logging
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from supabase import create_client, Client

from webapp_backend.openai_client import normalize_telegram_post

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# === подтягиваем .env, чтобы скрипт работал из systemd/таймера ===
load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")

BATCH_SIZE = int(os.getenv("TELEGRAM_PROCESS_BATCH_SIZE", "50"))

# качество / анти-“сырые карточки”
TELEGRAM_MIN_TEXT_CHARS = int(os.getenv("TELEGRAM_MIN_TEXT_CHARS", "80"))
TELEGRAM_MAX_TEXT_CHARS = int(os.getenv("TELEGRAM_MAX_TEXT_CHARS", "6000"))

# Если OpenAI не отработал и мы получили fallback_raw:
#  - если false: карточку вставляем, но is_active=false (в фид не попадёт)
#  - если true: вставляем активную (не рекомендую, но оставил как тумблер)
TELEGRAM_ALLOW_FALLBACK_RAW_ACTIVE = (
    os.getenv("TELEGRAM_ALLOW_FALLBACK_RAW_ACTIVE", "false").lower() in ("1", "true", "yes")
)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _safe_source_ref(message_url: str, channel_id: Any, tg_message_id: Any) -> str:
    url = (message_url or "").strip()
    if url:
        return url
    # стабильный fallback, чтобы дедуп работал даже без url
    return f"telegram:{channel_id}:{tg_message_id}"


def _fetch_existing_card_id_by_source_ref(source_ref: str) -> Optional[int]:
    """
    Идемпотентность: если карточка уже вставлена (source_type=telegram + source_ref),
    повторно не создаём.
    """
    try:
        resp = (
            supabase.table("cards")
            .select("id")
            .eq("source_type", "telegram")
            .eq("source_ref", source_ref)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if rows:
            return int(rows[0]["id"])
    except Exception:
        log.exception("Failed to lookup existing card by source_ref=%r", source_ref)
    return None


def _fetch_unprocessed_posts(limit: int) -> List[Dict[str, Any]]:
    """
    Берём telegram_posts с processed_to_card = false
    + встраиваем данные telegram_channels.
    """
    query = (
        supabase.table("telegram_posts")
        .select(
            "id, channel_id, tg_message_id, message_url, raw_text, raw_meta, published_at, card_id, processed_to_card, "
            "channel:telegram_channels(id, title, default_tags, language)"
        )
        .eq("processed_to_card", False)
        .order("published_at", desc=False)
        .limit(limit)
    )
    resp = query.execute()
    data = resp.data or []
    log.info("Fetched %d unprocessed telegram_posts", len(data))
    return data


def _merge_tags(channel_default_tags: Any, normalized_tags: Any) -> List[str]:
    """
    Объединяем теги из таблицы каналов и из модели, приводим к нижнему регистру и убираем дубли.
    """
    result: List[str] = []

    def _add_many(items: Any):
        nonlocal result
        if not items:
            return
        if isinstance(items, str):
            items = [items]
        if not isinstance(items, list):
            return
        for t in items:
            if not isinstance(t, str):
                continue
            v = t.strip().lower()
            if not v:
                continue
            result.append(v)

    _add_many(channel_default_tags)
    _add_many(normalized_tags)

    seen = set()
    deduped: List[str] = []
    for t in result:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


def _insert_card_from_telegram(
    normalized: Dict[str, Any],
    channel: Dict[str, Any],
    source_ref: str,
) -> int:
    """
    Вставляет карточку в cards и возвращает её id.
    normalized — dict из normalize_telegram_post.
    """
    channel_title = (channel.get("title") or "").strip()
    channel_default_tags = channel.get("default_tags") or []

    tags = _merge_tags(channel_default_tags, normalized.get("tags"))

    # meta.source_name: модель -> title канала -> DEFAULT_SOURCE_NAME
    source_name = (normalized.get("source_name") or "").strip() or channel_title or DEFAULT_SOURCE_NAME

    quality = str(normalized.get("quality") or "ok").strip().lower()
    is_active = True
    if quality != "ok" and not TELEGRAM_ALLOW_FALLBACK_RAW_ACTIVE:
        # не показываем “сырые” карточки в ленте
        is_active = False

    meta = {
        "source_name": source_name,
        "quality": quality,
        "ingest": "telegram",
    }

    language = (normalized.get("language") or "").strip() or (channel.get("language") or "ru")
    try:
        importance_score = float(normalized.get("importance_score", 0.5))
    except Exception:
        importance_score = 0.5
    importance_score = max(0.0, min(1.0, importance_score))

    title = (normalized.get("title") or "").strip()
    body = (normalized.get("body") or "").strip()

    card_payload = {
        "title": title,
        "body": body,
        "tags": tags,
        "importance_score": importance_score,
        "language": language,
        "is_active": is_active,
        "source_type": "telegram",
        "source_ref": source_ref,
        "meta": meta,
    }

    log.info(
        "Inserting card from telegram: active=%s quality=%s title=%r source_name=%r tags=%r source_ref=%r",
        is_active,
        quality,
        title,
        source_name,
        tags,
        source_ref,
    )

    resp = supabase.table("cards").insert(card_payload).execute()
    if not resp.data:
        raise RuntimeError("Supabase insert into cards returned no data")
    card_id = int(resp.data[0]["id"])
    return card_id


def _mark_post_processed(post_id: int, card_id: int) -> None:
    supabase.table("telegram_posts").update(
        {
            "processed_to_card": True,
            "card_id": card_id,
        }
    ).eq("id", post_id).execute()


def process_telegram_posts_batch(limit: int = BATCH_SIZE) -> None:
    """
    Основной пайплайн: telegram_posts -> OpenAI -> cards.

    Ключевые фиксы:
    - идемпотентность по source_ref (message_url или telegram:{channel_id}:{tg_message_id})
    - если у поста уже есть card_id — просто отмечаем processed_to_card=true
    - “сырые” fallback карточки НЕ активируем (по умолчанию)
    """
    posts = _fetch_unprocessed_posts(limit=limit)
    if not posts:
        log.info("No unprocessed telegram_posts found")
        return

    processed = 0
    skipped = 0

    for post in posts:
        post_id = int(post["id"])
        channel = post.get("channel") or {}

        raw_text = (post.get("raw_text") or "").strip()
        if not raw_text:
            log.warning("Post id=%s has empty raw_text, skipping", post_id)
            skipped += 1
            continue

        # простой quality gate до OpenAI
        if len(raw_text) < TELEGRAM_MIN_TEXT_CHARS:
            log.info("Post id=%s too short (%d chars), skipping", post_id, len(raw_text))
            skipped += 1
            # можно помечать processed_to_card=true, но я оставляю в очереди —
            # вдруг позже захочешь другой критерий
            continue

        if len(raw_text) > TELEGRAM_MAX_TEXT_CHARS:
            raw_text = raw_text[:TELEGRAM_MAX_TEXT_CHARS].strip()

        # если по какой-то причине card_id уже есть — просто помечаем как processed
        existing_card_id = post.get("card_id")
        if existing_card_id:
            try:
                _mark_post_processed(post_id, int(existing_card_id))
                processed += 1
                continue
            except Exception:
                log.exception("Failed to mark already-linked post processed id=%s", post_id)

        channel_id = post.get("channel_id")
        tg_message_id = post.get("tg_message_id")
        message_url = (post.get("message_url") or "").strip()

        source_ref = _safe_source_ref(message_url, channel_id, tg_message_id)

        # идемпотентность: если карточка уже есть по этому source_ref — не создаём заново
        try:
            already = _fetch_existing_card_id_by_source_ref(source_ref)
            if already:
                _mark_post_processed(post_id, already)
                processed += 1
                continue
        except Exception:
            log.exception("Idempotency check failed for post id=%s", post_id)

        language = (channel.get("language") or "ru").strip() or "ru"
        channel_title = channel.get("title") or ""

        try:
            normalized = normalize_telegram_post(
                raw_text=raw_text,
                channel_title=channel_title,
                language=language,
            )

            # если OpenAI вернул fallback_raw — по умолчанию вставим, но не активируем
            card_id = _insert_card_from_telegram(
                normalized=normalized,
                channel=channel,
                source_ref=source_ref,
            )

            _mark_post_processed(post_id, card_id)
            processed += 1

        except Exception:
            log.exception("Failed to process telegram_post id=%s", post_id)
            # пост останется непросессed, попробуем в следующем батче

    log.info("Processed=%d skipped=%d in this batch", processed, skipped)


if __name__ == "__main__":
    # CLI-режим: PYTHONPATH=src python -m telegram_ingest.process_telegram_posts
    process_telegram_posts_batch(limit=BATCH_SIZE)

# file: src/telegram_ingest/process_telegram_posts.py
import os
import logging
from typing import Any, Dict, List

from supabase import create_client, Client

from webapp_backend.openai_client import normalize_telegram_post

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
DEFAULT_SOURCE_NAME = os.getenv("DEFAULT_SOURCE_NAME", "EYYE • AI-подборка")

BATCH_SIZE = int(os.getenv("TELEGRAM_PROCESS_BATCH_SIZE", "50"))

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def _fetch_unprocessed_posts(limit: int) -> List[Dict[str, Any]]:
    """
    Берём telegram_posts с processed_to_card = false
    + встраиваем данные telegram_channels.
    """
    query = (
        supabase.table("telegram_posts")
        .select(
            "id, channel_id, tg_message_id, message_url, raw_text, raw_meta, published_at, "
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
    message_url: str,
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
    meta = {"source_name": source_name}

    language = (normalized.get("language") or "").strip() or (channel.get("language") or "ru")

    card_payload = {
        "title": normalized.get("title", "").strip(),
        "body": normalized.get("body", "").strip(),
        "tags": tags,
        "importance_score": float(normalized.get("importance_score", 0.5)),
        "language": language,
        "is_active": True,
        "source_type": "telegram",
        "source_ref": message_url,
        "meta": meta,
    }

    log.info(
        "Inserting card from telegram: title=%r, source_name=%r, tags=%r",
        card_payload["title"],
        source_name,
        tags,
    )

    resp = supabase.table("cards").insert(card_payload).execute()
    if not resp.data:
        raise RuntimeError("Supabase insert into cards returned no data")
    card_id = resp.data[0]["id"]
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
    """
    posts = _fetch_unprocessed_posts(limit=limit)
    if not posts:
        log.info("No unprocessed telegram_posts found")
        return

    processed = 0
    for post in posts:
        post_id = post["id"]
        channel = post.get("channel") or {}
        raw_text = (post.get("raw_text") or "").strip()
        message_url = post.get("message_url") or ""
        language = (channel.get("language") or "ru").strip() or "ru"
        channel_title = channel.get("title") or ""

        if not raw_text:
            log.warning("Post id=%s has empty raw_text, skipping", post_id)
            continue

        try:
            normalized = normalize_telegram_post(
                raw_text=raw_text,
                channel_title=channel_title,
                language=language,
            )
            card_id = _insert_card_from_telegram(
                normalized=normalized,
                channel=channel,
                message_url=message_url,
            )
            _mark_post_processed(post_id, card_id)
            processed += 1
        except Exception:
            log.exception("Failed to process telegram_post id=%s", post_id)
            # пост останется непросессed, попробуем в следующем батче

    log.info("Processed %d telegram_posts in this batch", processed)


if __name__ == "__main__":
    # CLI-режим: PYTHONPATH=src python -m telegram_ingest.process_telegram_posts
    process_telegram_posts_batch(limit=BATCH_SIZE)

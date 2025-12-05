# file: src/telegram_ingest/fetch_telegram_posts.py
import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import timezone
from typing import Dict, Any, List

from telethon import TelegramClient
from dotenv import load_dotenv
from supabase import create_client, Client

# Подтягиваем .env (локально и на сервере)
load_dotenv()

logger = logging.getLogger(__name__)

# ==========
# Пути / импорты внутреннего кода
# ==========

# /root/eyye-tg-bot/src/telegram_ingest/fetch_telegram_posts.py -> /root/eyye-tg-bot/src
CURRENT_DIR = Path(__file__).resolve()
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from webapp_backend.openai_client import (
    normalize_telegram_post,
    is_configured as openai_is_configured,
)
from webapp_backend.cards_service import _insert_cards_into_db

# ==========
# Конфиг Supabase / Telegram
# ==========

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "eyye_session")

# Сколько сообщений максимум тащим за один проход по каналу
TELEGRAM_FETCH_LIMIT = int(os.getenv("TELEGRAM_FETCH_LIMIT", "400"))

# Минимальная длина текста, чтобы пытаться делать из него карточку
TELEGRAM_MIN_TEXT_LENGTH_FOR_CARD = int(
    os.getenv("TELEGRAM_MIN_TEXT_LENGTH_FOR_CARD", "40")
)

# Язык по умолчанию для телеграм-каналов (hint для OpenAI)
TELEGRAM_DEFAULT_LANGUAGE = os.getenv("TELEGRAM_DEFAULT_LANGUAGE", "ru")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_for_channel(
    client: TelegramClient,
    channel_row: Dict[str, Any],
    limit: int | None = None,
) -> None:
    """
    Забираем последние сообщения для одного канала,
    пишем их в telegram_posts и параллельно создаём карточки в cards.
    """
    if limit is None or limit <= 0:
        limit = TELEGRAM_FETCH_LIMIT

    channel_id = channel_row["id"]  # uuid в telegram_channels
    tg_chat_id = channel_row["tg_chat_id"]
    username = channel_row.get("username")
    last_fetched_message_id = channel_row.get("last_fetched_message_id")

    title = channel_row.get("title") or str(username) or str(tg_chat_id)

    # entity: по username (если есть) или по tg_chat_id
    entity = await client.get_entity(username or tg_chat_id)

    kwargs: Dict[str, Any] = {"limit": limit}
    if last_fetched_message_id:
        # берём только новые сообщения
        kwargs["min_id"] = last_fetched_message_id

    messages = await client.get_messages(entity, **kwargs)
    if not messages:
        print(f"[{title}] новых сообщений нет")
        return

    rows: List[Dict[str, Any]] = []
    cards: List[Dict[str, Any]] = []
    max_message_id = last_fetched_message_id or 0

    # идём от старых к новым, чтобы published_at были по возрастанию
    for msg in reversed(messages):
        if not msg.message:
            continue

        tg_message_id = msg.id
        if last_fetched_message_id and tg_message_id <= last_fetched_message_id:
            continue

        max_message_id = max(max_message_id, tg_message_id)

        # t.me/<username>/<message_id>, если username есть
        if username:
            message_url = f"https://t.me/{username}/{tg_message_id}"
        else:
            message_url = None

        published_at = msg.date
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        raw_text = msg.message or ""

        # Сырой пост — в telegram_posts
        rows.append(
            {
                "channel_id": channel_id,
                "tg_message_id": tg_message_id,
                "message_url": message_url,
                "raw_text": raw_text,
                "published_at": published_at.isoformat(),
                "raw_meta": {},
            }
        )

        # Пытаемся сразу же сделать EYYE-карточку через OpenAI
        if (
            openai_is_configured()
            and raw_text
            and len(raw_text.strip()) >= TELEGRAM_MIN_TEXT_LENGTH_FOR_CARD
        ):
            try:
                norm = normalize_telegram_post(
                    raw_text=raw_text,
                    channel_title=title,
                    language=TELEGRAM_DEFAULT_LANGUAGE,
                )
                card: Dict[str, Any] = {
                    "title": norm.get("title"),
                    "body": norm.get("body"),
                    "tags": norm.get("tags") or [],
                    "importance_score": norm.get("importance_score", 0.5),
                    # язык карточки — берём из ответа модели, либо дефолт
                    "language": norm.get("language") or TELEGRAM_DEFAULT_LANGUAGE,
                    # отдаём source_name, чтобы в cards.meta.source_name был корректный бренд
                    "source_name": norm.get("source_name") or title,
                    # чтобы можно было отследить конкретный источник
                    "source_ref": message_url or f"{tg_chat_id}:{tg_message_id}",
                }
                cards.append(card)
            except Exception:
                logger.exception(
                    "Failed to normalize telegram message %s from channel %s",
                    tg_message_id,
                    title,
                )

    # Вставляем сырые посты
    if rows:
        print(f"[{title}] вставляем {len(rows)} сообщений в telegram_posts")
        supabase.table("telegram_posts").insert(rows).execute()

        # фиксируем до какого message_id дошли
        supabase.table("telegram_channels").update(
            {"last_fetched_message_id": max_message_id}
        ).eq("id", channel_id).execute()
    else:
        print(f"[{title}] сообщений для вставки нет")

    # Вставляем карточки в cards
    if cards:
        inserted = _insert_cards_into_db(
            supabase,
            cards,
            # language=None -> язык берётся из каждой карточки
            language=None,
            source_type="telegram",
            fallback_source_name=title,
            source_ref=None,
        )
        print(
            f"[{title}] создано {len(inserted)} карточек из Telegram-постов"
        )
    else:
        print(f"[{title}] нет подходящих постов для карточек")


async def main() -> None:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    # Берём все активные каналы
    channels = (
        supabase.table("telegram_channels")
        .select("*")
        .eq("is_active", True)
        .execute()
        .data
    )

    if not channels:
        print("Активных каналов в telegram_channels не найдено")
        await client.disconnect()
        return

    for ch in channels:
        await fetch_for_channel(client, ch)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

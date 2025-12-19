# file: src/telegram_ingest/fetch_telegram_posts.py
import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import timezone
from typing import Dict, Any, List

from telethon import TelegramClient
from telethon.errors import (
    UsernameInvalidError,
    UsernameNotOccupiedError,
    ChannelPrivateError,
    ChannelInvalidError,
)
from dotenv import load_dotenv
from supabase import create_client, Client

# Подтягиваем .env (локально и на сервере)
load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Пути / импорты внутреннего кода
# ==========

# /root/eyye-tg-bot/src/telegram_ingest/fetch_telegram_posts.py -> /root/eyye-tg-bot/src
CURRENT_DIR = Path(__file__).resolve()
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_for_channel(
    client: TelegramClient,
    channel_row: Dict[str, Any],
    limit: int | None = None,
) -> None:
    """
    Забираем последние сообщения для одного канала и пишем их в telegram_posts.

    ВАЖНО: карточки в cards тут НЕ создаём.
    Нормализация + дедуп + вставка в cards происходит в telegram_ingest.process_telegram_posts.py
    """
    if limit is None or limit <= 0:
        limit = TELEGRAM_FETCH_LIMIT

    channel_id = channel_row["id"]  # uuid в telegram_channels
    tg_chat_id = channel_row["tg_chat_id"]
    username = channel_row.get("username")
    last_fetched_message_id = channel_row.get("last_fetched_message_id")

    title = channel_row.get("title") or str(username) or str(tg_chat_id)

    # --- безопасно резолвим entity ---
    try:
        entity_key = username or tg_chat_id
        logger.info("Fetching for channel %s (@%s, tg_chat_id=%s)", title, username, tg_chat_id)
        entity = await client.get_entity(entity_key)
    except (UsernameInvalidError, UsernameNotOccupiedError) as e:
        logger.warning(
            "Channel %s (@%s) has invalid or unused username. Disabling channel. Error: %s",
            title,
            username,
            e,
        )
        supabase.table("telegram_channels").update({"is_active": False}).eq("id", channel_id).execute()
        return
    except (ChannelPrivateError, ChannelInvalidError) as e:
        logger.warning(
            "Channel %s (@%s) is private or invalid. Disabling channel. Error: %s",
            title,
            username,
            e,
        )
        supabase.table("telegram_channels").update({"is_active": False}).eq("id", channel_id).execute()
        return
    except Exception as e:
        logger.exception(
            "Unexpected error while resolving channel %s (@%s). Skipping this channel. Error: %s",
            title,
            username,
            e,
        )
        return

    kwargs: Dict[str, Any] = {"limit": limit}
    if last_fetched_message_id:
        # Telethon вернёт сообщения с id > min_id
        kwargs["min_id"] = last_fetched_message_id

    messages = await client.get_messages(entity, **kwargs)
    if not messages:
        print(f"[{title}] новых сообщений нет")
        return

    rows: List[Dict[str, Any]] = []
    max_message_id = last_fetched_message_id or 0

    # идём от старых к новым, чтобы published_at были по возрастанию
    for msg in reversed(messages):
        if not msg.message:
            continue

        tg_message_id = msg.id
        if last_fetched_message_id and tg_message_id <= last_fetched_message_id:
            continue

        max_message_id = max(max_message_id, tg_message_id)

        if username:
            message_url = f"https://t.me/{username}/{tg_message_id}"
        else:
            message_url = None

        published_at = msg.date
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        raw_text = msg.message or ""

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

    if rows:
        print(f"[{title}] вставляем {len(rows)} сообщений в telegram_posts")
        supabase.table("telegram_posts").insert(rows).execute()

        supabase.table("telegram_channels").update(
            {"last_fetched_message_id": max_message_id}
        ).eq("id", channel_id).execute()
    else:
        print(f"[{title}] сообщений для вставки нет")


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

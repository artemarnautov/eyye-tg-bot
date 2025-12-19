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

# грузим env НАДЁЖНО (и для systemd, и для ручного запуска)
load_dotenv("/root/eyye-tg-bot/.env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "eyye_session")

TELEGRAM_FETCH_LIMIT = int(os.getenv("TELEGRAM_FETCH_LIMIT", "400"))

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_for_channel(client: TelegramClient, channel_row: Dict[str, Any], limit: int | None = None) -> None:
    if limit is None or limit <= 0:
        limit = TELEGRAM_FETCH_LIMIT

    channel_id = channel_row["id"]
    tg_chat_id = channel_row.get("tg_chat_id")
    username = channel_row.get("username")
    last_fetched_message_id = channel_row.get("last_fetched_message_id")
    title = channel_row.get("title") or str(username) or str(tg_chat_id)

    try:
        entity_key = username or tg_chat_id
        logger.info("Fetching raw posts for %s (@%s, tg_chat_id=%s)", title, username, tg_chat_id)
        entity = await client.get_entity(entity_key)
    except (UsernameInvalidError, UsernameNotOccupiedError, ChannelPrivateError, ChannelInvalidError) as e:
        logger.warning("Channel %s (@%s) invalid/private. Disabling. Error: %s", title, username, e)
        supabase.table("telegram_channels").update({"is_active": False}).eq("id", channel_id).execute()
        return
    except Exception as e:
        logger.exception("Failed to resolve channel %s (@%s): %s", title, username, e)
        return

    kwargs: Dict[str, Any] = {"limit": limit}
    if last_fetched_message_id:
        kwargs["min_id"] = last_fetched_message_id

    messages = await client.get_messages(entity, **kwargs)
    if not messages:
        logger.info("[%s] no new messages", title)
        return

    rows: List[Dict[str, Any]] = []
    max_message_id = last_fetched_message_id or 0

    for msg in reversed(messages):
        if not msg.message:
            continue

        tg_message_id = msg.id
        if last_fetched_message_id and tg_message_id <= last_fetched_message_id:
            continue

        max_message_id = max(max_message_id, tg_message_id)

        message_url = f"https://t.me/{username}/{tg_message_id}" if username else None

        published_at = msg.date
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=timezone.utc)

        rows.append(
            {
                "channel_id": channel_id,
                "tg_message_id": tg_message_id,
                "message_url": message_url,
                "raw_text": msg.message or "",
                "published_at": published_at.isoformat(),
                "raw_meta": {},
            }
        )

    if rows:
        # upsert по уникальному индексу (channel_id, tg_message_id)
        supabase.table("telegram_posts").upsert(rows, on_conflict="channel_id,tg_message_id").execute()
        supabase.table("telegram_channels").update({"last_fetched_message_id": max_message_id}).eq("id", channel_id).execute()
        logger.info("[%s] upserted %d raw telegram_posts, last_fetched_message_id=%s", title, len(rows), max_message_id)


async def main() -> None:
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    channels = (
        supabase.table("telegram_channels")
        .select("*")
        .eq("is_active", True)
        .execute()
        .data
    )

    if not channels:
        logger.info("No active telegram_channels")
        await client.disconnect()
        return

    for ch in channels:
        await fetch_for_channel(client, ch)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

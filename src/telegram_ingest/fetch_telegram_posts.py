# file: src/telegram_ingest/fetch_telegram_posts.py
import os
import asyncio
from datetime import timezone
from typing import Dict, Any, List

from telethon import TelegramClient
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "eyye_session")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


async def fetch_for_channel(client: TelegramClient, channel_row: Dict[str, Any], limit: int = 200):
    """
    Забираем последние сообщения для одного канала и пишем их в telegram_posts.
    """
    channel_id = channel_row["id"]          # uuid в telegram_channels
    tg_chat_id = channel_row["tg_chat_id"]
    username = channel_row.get("username")
    last_fetched_message_id = channel_row.get("last_fetched_message_id")

    # entity: по username (если есть) или по tg_chat_id
    entity = await client.get_entity(username or tg_chat_id)

    kwargs = {"limit": limit}
    if last_fetched_message_id:
        # берем только новые сообщения
        kwargs["min_id"] = last_fetched_message_id

    messages = await client.get_messages(entity, **kwargs)
    if not messages:
        print(f"[{channel_row['title']}] новых сообщений нет")
        return

    rows: List[Dict[str, Any]] = []
    max_message_id = last_fetched_message_id or 0

    # идем от старых к новым, чтобы published_at были по возрастанию
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

        rows.append({
            "channel_id": channel_id,
            "tg_message_id": tg_message_id,
            "message_url": message_url,
            "raw_text": msg.message,
            "published_at": published_at.isoformat(),
            "raw_meta": {},
        })

    if rows:
        print(f"[{channel_row['title']}] вставляем {len(rows)} сообщений")
        supabase.table("telegram_posts").insert(rows).execute()

        # фиксируем до какого message_id дошли
        supabase.table("telegram_channels").update(
            {"last_fetched_message_id": max_message_id}
        ).eq("id", channel_id).execute()
    else:
        print(f"[{channel_row['title']}] сообщений для вставки нет")


async def main():
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

    for ch in channels:
        await fetch_for_channel(client, ch, limit=200)

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

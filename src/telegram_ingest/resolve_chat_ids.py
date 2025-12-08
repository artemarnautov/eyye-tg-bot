# file: src/telegram_ingest/resolve_chat_ids.py
import os
import asyncio
import logging
from typing import Any, Dict, List

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError, FloodWaitError
from supabase import create_client, Client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "eyye_session")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_channels_without_chat_id(limit: int = 500) -> List[Dict[str, Any]]:
    """
    Берём активные каналы, у которых tg_chat_id IS NULL.
    """
    resp = (
        supabase.table("telegram_channels")
        .select("id, username, title, tg_chat_id, is_active")
        .is_("tg_chat_id", None)
        .eq("is_active", True)
        .limit(limit)
        .execute()
    )
    data = resp.data or []
    log.info("Found %d channels without tg_chat_id", len(data))
    return data


async def resolve_username(client: TelegramClient, username: str) -> int | None:
    """
    Возвращает chat_id по username, либо None.
    """
    if not username:
        return None

    uname = username.lstrip("@").strip()
    if not uname:
        return None

    try:
        entity = await client.get_entity(uname)
    except UsernameInvalidError:
        log.warning("Invalid username: %s", username)
        return None
    except UsernameNotOccupiedError:
        log.warning("Username not occupied: %s", username)
        return None
    except FloodWaitError as e:
        log.error("FloodWaitError for %s: wait %s sec", username, e.seconds)
        # для MVP можно просто бросить исключение
        raise
    except Exception:
        log.exception("Failed to resolve username %s", username)
        return None

    chat_id = getattr(entity, "id", None)
    if chat_id is None:
        log.warning("Entity for %s has no id (type=%s)", username, type(entity))
    return chat_id


def update_channel_chat_id(row_id: str, tg_chat_id: int) -> None:
    resp = (
        supabase.table("telegram_channels")
        .update({"tg_chat_id": tg_chat_id})
        .eq("id", row_id)
        .execute()
    )
    if resp.data:
        log.info("Updated channel %s with tg_chat_id=%s", row_id, tg_chat_id)
    else:
        log.warning("No rows updated for id=%s", row_id)


async def main() -> None:
    channels = fetch_channels_without_chat_id()

    if not channels:
        log.info("No channels to resolve — nothing to do.")
        return

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()  # при первом запуске спросит номер и код

    try:
        for ch in channels:
            row_id = ch["id"]
            username = ch.get("username")
            title = ch.get("title") or username

            log.info("Resolving %s (@%s)...", title, username)

            chat_id = await resolve_username(client, username)
            if chat_id is None:
                log.warning("Skip %s (@%s) — cannot resolve chat_id", title, username)
                continue

            update_channel_chat_id(row_id, chat_id)
    finally:
        await client.disconnect()

    log.info("Done resolving tg_chat_id for channels.")


if __name__ == "__main__":
    asyncio.run(main())

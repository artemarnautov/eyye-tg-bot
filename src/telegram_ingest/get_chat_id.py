# file: src/telegram_ingest/get_chat_id.py
import os
import asyncio

from telethon import TelegramClient
from dotenv import load_dotenv

# Грузим переменные из .env (локально и на сервере, если файл рядом)
load_dotenv()

API_ID = int(os.environ["TELEGRAM_API_ID"])
API_HASH = os.environ["TELEGRAM_API_HASH"]
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "eyye_session")


async def main():
    """
    Вспомогательный скрипт:
    - при первом запуске авторизует Telethon по номеру телефона;
    - спрашивает @username канала;
    - печатает его tg_chat_id и title.
    """
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()  # при первом запуске спросит номер и код

    username = input("Введите @username канала (без @): ").strip()
    if username.startswith("@"):
        username = username[1:]

    entity = await client.get_entity(username)

    print("===================================")
    print(f"Канал: @{username}")
    print(f"id (tg_chat_id): {entity.id}")
    print(f"title: {getattr(entity, 'title', '')}")
    print("===================================")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

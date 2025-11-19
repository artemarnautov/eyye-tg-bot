# file: src/bot.py

import os
import logging

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

import sentry_sdk
from supabase import create_client, Client


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# Глобальный клиент Supabase (инициализируем в init_supabase_if_needed)
supabase: Client | None = None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — приветствие и базовое описание бота.
    Параллельно сохраняем/обновляем пользователя в Supabase.
    """
    user = update.effective_user
    first_name = user.first_name if user is not None else "друг"

    # Пытаемся сохранить пользователя в Supabase
    try:
        await save_user_to_supabase(update)
    except Exception as e:
        logger.error("Не удалось сохранить пользователя в Supabase", exc_info=e)

    text = (
        f"Привет, {first_name}! 👋\n\n"
        "Это MVP бота EYYE.\n"
        "Сейчас я умею только отвечать на команды:\n"
        "/start — приветствие\n"
        "/help — список команд\n"
        "/ping — проверка, что бот жив\n\n"
        "Дальше будем добавлять персонализированную новостную ленту. 📰"
    )

    if update.message:
        await update.message.reply_text(text)
    else:
        logger.warning("Получено событие /start без message")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /help — показывает список доступных команд.
    """
    text = (
        "Доступные команды:\n"
        "/start — приветствие и описание бота\n"
        "/help — список команд\n"
        "/ping — проверка, что бот жив\n"
    )
    if update.message:
        await update.message.reply_text(text)


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /ping — простой healthcheck. Удобно проверить, что бот отвечает.
    """
    user_id = update.effective_user.id if update.effective_user else "unknown"
    logger.info("Получена команда /ping от user_id=%s", user_id)

    if update.message:
        await update.message.reply_text("pong 🏓")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Глобальный обработчик ошибок.
    Логируем всё, чтобы понимать, что пошло не так.
    """
    logger.error("Произошла ошибка при обработке апдейта", exc_info=context.error)

    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "Упс, произошла внутренняя ошибка. Мы уже смотрим, что случилось. 😔"
        )


async def save_user_to_supabase(update: Update) -> None:
    """
    Сохраняем информацию о пользователе в таблицу telegram_users в Supabase.
    Храним только id и username.
    Если Supabase не настроен — просто выходим.
    """
    global supabase

    if supabase is None:
        logger.info("Supabase не настроен, пропускаем сохранение пользователя")
        return

    user = update.effective_user
    if user is None:
        logger.warning("Нет effective_user в апдейте, не можем сохранить пользователя")
        return

    data = {
        "id": user.id,
        "username": user.username,  # username может быть None — в БД тогда будет NULL
    }

    logger.info("Сохраняем/обновляем пользователя в Supabase: %s", data)

    # upsert — вставит новую запись или обновит существующую по первичному ключу (id)
    response = supabase.table("telegram_users").upsert(data).execute()
    logger.info("Ответ Supabase при сохранении пользователя: %s", response)


def init_sentry_if_needed() -> None:
    """
    Подключаем Sentry, если задан SENTRY_DSN в .env.
    Если переменная не задана — просто ничего не делаем.
    """
    dsn = os.getenv("SENTRY_DSN")
    if dsn:
        sentry_sdk.init(
            dsn=dsn,
            traces_sample_rate=1.0,
        )
        logger.info("Sentry инициализирован")
    else:
        logger.info("Sentry не настроен (SENTRY_DSN не задан)")


def init_supabase_if_needed() -> None:
    """
    Инициализируем клиент Supabase, если заданы SUPABASE_URL и SUPABASE_KEY.
    Если чего-то не хватает — просто логируем и работаем без Supabase.
    """
    global supabase

    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        logger.info("SUPABASE_URL или SUPABASE_KEY не заданы — Supabase отключен")
        supabase = None
        return

    supabase_client = create_client(url, key)
    supabase = supabase_client
    logger.info("Supabase клиент инициализирован")


def get_bot_token() -> str:
    """
    Читаем BOT_TOKEN из окружения.
    Если токен не найден — выводим понятную ошибку и выходим.
    """
    token = os.getenv("BOT_TOKEN")
    if not token:
        print(
            "Ошибка: переменная окружения BOT_TOKEN не установлена.\n"
            "Убедись, что в корне проекта есть файл .env с строкой:\n"
            "BOT_TOKEN=твой_телеграм_токен"
        )
        raise SystemExit(1)
    return token


def build_application() -> Application:
    """
    Создаём и настраиваем экземпляр Application.
    Отдельная функция, чтобы дальше было проще расширять конфиг.
    """
    bot_token = get_bot_token()

    application = ApplicationBuilder().token(bot_token).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping))

    application.add_error_handler(error_handler)

    return application


def main() -> None:
    """
    Точка входа в приложение.
    Вызывается, когда запускаем: python -m src.bot
    """
    load_dotenv()

    init_sentry_if_needed()
    init_supabase_if_needed()

    logger.info("Запускаем EYYE Telegram Bot")

    application = build_application()

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()


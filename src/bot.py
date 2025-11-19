# file: src/bot.py

import os
import logging

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ApplicationBuilder,
)

import sentry_sdk


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — приветствие и базовое описание бота.
    """
    user = update.effective_user
    first_name = user.first_name if user is not None else "друг"

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
    logger.info("Получена команда /ping от user_id=%s", update.effective_user.id if update.effective_user else "unknown")
    if update.message:
        await update.message.reply_text("pong 🏓")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Глобальный обработчик ошибок.
    Логируем всё, чтобы понимать, что пошло не так.
    """
    logger.error("Произошла ошибка при обработке апдейта", exc_info=context.error)

    # Отправим минимум информации пользователю, если это было сообщение
    if isinstance(update, Update) and update.message:
        await update.message.reply_text(
            "Упс, произошла внутренняя ошибка. Мы уже смотрим, что случилось. 😔"
        )


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

    logger.info("Запускаем EYYE Telegram Bot")

    application = build_application()

    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()


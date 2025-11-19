# file: src/bot.py

import os
import logging

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

import sentry_sdk


# Включаем базовый логгер, чтобы видеть сообщения в journald / консоли
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик команды /start.
    Просто здоровается с пользователем и объясняет, что это MVP.
    """
    user = update.effective_user
    first_name = user.first_name if user is not None else "друг"

    text = (
        f"Привет, {first_name}! 👋\n\n"
        "Это MVP бота EYYE.\n"
        "Сейчас я умею только отвечать на команду /start.\n"
        "Дальше будем добавлять персонализированную новостную ленту. 📰"
    )

    if update.message:
        await update.message.reply_text(text)
    else:
        logger.warning("Получено событие /start без message")


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


def main() -> None:
    """
    Точка входа в приложение.
    Вызывается, когда запускаем: python -m src.bot
    """
    # Загружаем переменные из .env
    load_dotenv()

    # Инициализируем Sentry (если есть DSN)
    init_sentry_if_needed()

    # Получаем токен бота
    bot_token = get_bot_token()

    logger.info("Запускаем EYYE Telegram Bot")

    # Создаем приложение python-telegram-bot
    application = Application.builder().token(bot_token).build()

    # Регистрируем команды
    application.add_handler(CommandHandler("start", start))

    # Запускаем polling (бот будет получать апдейты)
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()

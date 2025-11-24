# file: src/bot.py
import logging
import os
from typing import Optional

from dotenv import load_dotenv
from supabase import Client, create_client
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# ==========================
# Инициализация окружения
# ==========================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set in environment variables")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========================
# Логирование
# ==========================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ==========================
# Работа с Supabase
# ==========================

async def save_user_to_supabase(telegram_id: int, username: Optional[str]) -> None:
    """
    Сохраняем / обновляем пользователя в таблице telegram_users.
    Если Supabase не настроен, просто пишем в лог и выходим.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip save_user_to_supabase")
        return

    data = {
        "id": telegram_id,
        "username": username,
    }

    try:
        response = supabase.table("telegram_users").upsert(
            data,
            on_conflict="id",
        ).execute()
        logger.info("Upsert telegram user %s: %s", telegram_id, response)
    except Exception as e:
        # Логируем, но не падаем
        logger.exception("Error saving user to Supabase: %s", e)


async def load_user_from_supabase(telegram_id: int) -> Optional[dict]:
    """
    Читаем пользователя из таблицы telegram_users по id.
    Возвращаем dict или None.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip load_user_from_supabase")
        return None

    try:
        result = (
            supabase.table("telegram_users")
            .select("*")
            .eq("id", telegram_id)
            .single()
            .execute()
        )
        data = getattr(result, "data", None)
        if isinstance(data, list):
            return data[0] if data else None
        return data
    except Exception as e:
        # Логируем и возвращаем None — наверху покажем только данные из Telegram
        logger.exception("Error loading user from Supabase: %s", e)
        return None


# ==========================
# Хендлеры команд
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — сохраняем пользователя в Supabase и показываем приветствие.
    """
    user = update.effective_user
    if user:
        await save_user_to_supabase(user.id, user.username)

    text_lines = [
        "Привет! Это EYYE — твой персональный новостной ассистент.",
        "",
        "Пока что бот умеет немногое:",
        "/ping — проверить, что бот жив",
        "/me — показать, что бот знает о твоём аккаунте",
        "/help — показать справку",
    ]

    if update.message:
        await update.message.reply_text("\n".join(text_lines))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /help — список команд.
    """
    text_lines = [
        "Доступные команды:",
        "/start — перезапустить бота",
        "/ping — проверить, что бот жив",
        "/me — показать, что бот знает о тебе в базе и в Telegram",
        "/help — эта справка",
    ]

    if update.message:
        await update.message.reply_text("\n".join(text_lines))


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /ping — простая проверка, что бот жив.
    """
    if update.message:
        await update.message.reply_text("pong")


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /me — показать:
    - данные из Telegram,
    - если получится, данные из Supabase.
    """
    user = update.effective_user
    if not user:
        if update.message:
            await update.message.reply_text("Не получилось определить твой Telegram-профиль.")
        return

    # На всякий случай ещё раз сохраняем пользователя
    await save_user_to_supabase(user.id, user.username)

    # Базовая информация из Telegram
    tg_lines = [
        "Данные из Telegram:",
        f"id: {user.id}",
        f"username: {user.username}",
        f"first_name: {user.first_name}",
        f"last_name: {user.last_name}",
        "",
    ]

    # Если Supabase не настроен — просто говорим об этом
    if not supabase:
        tg_lines.append("Supabase сейчас не настроен, поэтому показываю только данные из Telegram.")
        if update.message:
            await update.message.reply_text("\n".join(tg_lines))
        return

    # Пытаемся прочитать запись из Supabase
    row = await load_user_from_supabase(user.id)

    if not row:
        tg_lines.append(
            "Supabase сейчас отвечает с ошибкой или запись ещё не создана.\n"
            "Показываю только данные из Telegram."
        )
        if update.message:
            await update.message.reply_text("\n".join(tg_lines))
        return

    # Если запись есть, добавляем её в вывод
    sb_lines = [
        "Информация о тебе в базе EYYE (Supabase):",
        f"id: {row.get('id')}",
        f"username: {row.get('username')}",
        f"created_at: {row.get('created_at')}",
    ]

    all_lines = tg_lines + sb_lines

    if update.message:
        await update.message.reply_text("\n".join(all_lines))


# ==========================
# Глобальный обработчик ошибок
# ==========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Логируем любые необработанные исключения и стараемся аккуратно ответить пользователю.
    """
    logger.exception("Exception while handling update: %s", context.error)

    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Что-то пошло не так, но мы уже смотрим в логи.",
            )
    except Exception:
        logger.exception("Failed to send error message to user")


# ==========================
# Сборка и запуск приложения
# ==========================

def build_application() -> Application:
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("me", me))

    application.add_error_handler(error_handler)

    return application


def main() -> None:
    app = build_application()
    app.run_polling()


if __name__ == "__main__":
    main()

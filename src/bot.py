# file: src/bot.py
import logging
import os
import json
from typing import Optional, Any, Dict, List

from dotenv import load_dotenv
from supabase import Client, create_client

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    WebAppInfo,
)
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

BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
WEBAPP_BASE_URL = os.getenv("WEBAPP_BASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN or TELEGRAM_BOT_TOKEN is not set in environment variables")

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
# Supabase helpers
# ==========================

async def save_user_to_supabase(telegram_id: int, username: Optional[str]) -> None:
    """
    Простая upsert-запись в таблицу telegram_users.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip save_user_to_supabase")
        return

    data = {
        "id": telegram_id,
        "username": username,
    }

    try:
        resp = (
            supabase.table("telegram_users")
            .upsert(data, on_conflict="id")
            .execute()
        )
        logger.info("Upsert telegram user %s: %s", telegram_id, resp)
    except Exception:
        logger.exception("Error saving user to Supabase")


async def load_user_profile(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    user_profiles по user_id — для /me и /raw_profile.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip load_user_profile")
        return None

    try:
        resp = (
            supabase.table("user_profiles")
            .select("*")
            .eq("user_id", telegram_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Error loading user profile from Supabase")
        return None

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return None
    if isinstance(data, list):
        return data[0]
    if isinstance(data, dict):
        return data
    return None


async def delete_user_profile(telegram_id: int) -> bool:
    """
    Удаляем профиль + веса тем пользователя (user_topic_weights).
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip delete_user_profile")
        return False

    ok = True
    try:
        resp_prof = (
            supabase.table("user_profiles")
            .delete()
            .eq("user_id", telegram_id)
            .execute()
        )
        logger.info("Deleted user_profiles for %s: %s", telegram_id, resp_prof)
    except Exception:
        ok = False
        logger.exception("Error deleting user_profiles")

    try:
        resp_weights = (
            supabase.table("user_topic_weights")
            .delete()
            .eq("user_id", telegram_id)
            .execute()
        )
        logger.info("Deleted user_topic_weights for %s: %s", telegram_id, resp_weights)
    except Exception:
        ok = False
        logger.exception("Error deleting user_topic_weights")

    return ok


# ==========================
# Кнопка входа в WebApp
# ==========================

async def send_webapp_entry_point(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
) -> None:
    """
    Отправляет inline-кнопку для входа в WebApp EYYE.

    Если WEBAPP_BASE_URL не задан — показываем заглушку.
    Если URL https:// — используем WebAppInfo, чтобы Telegram передавал initData.
    Если URL http:// — URL-кнопка для локального/тестового режима.
    """
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not chat or not user:
        return

    if not WEBAPP_BASE_URL:
        if message:
            await message.reply_text(
                "WebApp EYYE пока не подключён. "
                "Как только он будет готов, здесь появится кнопка для открытия ленты."
            )
        return

    base_url = WEBAPP_BASE_URL.rstrip("/")
    # tg_id для backend’а, чтобы привязать профиль к Telegram-пользователю
    webapp_url = f"{base_url}/?tg_id={user.id}"

    use_webapp_button = webapp_url.startswith("https://")

    if use_webapp_button:
        button = InlineKeyboardButton(
            text="Открыть EYYE-ленту",
            web_app=WebAppInfo(url=webapp_url),
        )
    else:
        button = InlineKeyboardButton(
            text="Открыть EYYE-ленту",
            url=webapp_url,
        )

    keyboard = InlineKeyboardMarkup([[button]])

    text_lines = [
        "Привет! 👋",
        "",
        "Это EYYE — персональная новостная лента, которая выглядит как Telegram-канал.",
        "",
        "Нажми кнопку ниже, чтобы открыть WebApp:",
    ]

    await context.bot.send_message(
        chat_id=chat.id,
        text="\n".join(text_lines),
        reply_markup=keyboard,
    )


# ==========================
# Команды
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — минимальный онбординг: сохраняем пользователя и даём кнопку WebApp.
    Вся дальнейшая магия (город → темы → лента) происходит внутри WebApp.
    """
    user = update.effective_user

    if user:
        await save_user_to_supabase(user.id, user.username)

    await send_webapp_entry_point(update, context)


async def webapp_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /webapp — просто ещё раз отправить кнопку WebApp.
    """
    await send_webapp_entry_point(update, context)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /help — простая справка.
    """
    lines = [
        "Команды EYYE:",
        "",
        "/start — открыть персональную EYYE-ленту (WebApp)",
        "/webapp — ещё раз показать кнопку входа в WebApp",
        "/ping — проверить, что бот жив",
        "/me — показать, что известно о тебе в базе",
        "/raw_profile — показать сохранённый профиль (город, дополнительные поля)",
        "/reset_profile — удалить профиль (город + выбранные темы)",
        "/help — эта справка",
        "",
        "Весь основной опыт (выбор города, тем и чтение ленты) теперь внутри WebApp.",
    ]
    if update.message:
        await update.message.reply_text("\n".join(lines))


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /ping — проверка, что бот жив.
    """
    if update.message:
        await update.message.reply_text("pong")


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /me — Telegram-данные + краткая инфа из Supabase.
    """
    user = update.effective_user
    if not user:
        if update.message:
            await update.message.reply_text("Не получилось определить твой Telegram-профиль.")
        return

    await save_user_to_supabase(user.id, user.username)

    tg_lines: List[str] = [
        "Данные из Telegram:",
        f"id: {user.id}",
        f"username: {user.username}",
        f"first_name: {user.first_name}",
        f"last_name: {user.last_name}",
        "",
    ]

    if not supabase:
        tg_lines.append("Supabase сейчас не настроен, поэтому показываю только данные из Telegram.")
        if update.message:
            await update.message.reply_text("\n".join(tg_lines))
        return

    profile = await load_user_profile(user.id)
    if not profile:
        tg_lines.append("Профиль в Supabase пока не создан. Заполни его через WebApp.")
        if update.message:
            await update.message.reply_text("\n".join(tg_lines))
        return

    prof_lines: List[str] = [
        "Профиль в Supabase (user_profiles):",
        f"user_id: {profile.get('user_id')}",
        f"location_city: {profile.get('location_city')}",
        f"location_country: {profile.get('location_country')}",
    ]

    if "raw_interests" in profile and profile.get("raw_interests"):
        prof_lines.append("")
        prof_lines.append("raw_interests (обрезано):")
        raw = str(profile.get("raw_interests") or "")
        if len(raw) > 400:
            raw = raw[:397] + "..."
        prof_lines.append(raw)

    if update.message:
        await update.message.reply_text("\n".join(tg_lines + [""] + prof_lines))


async def raw_profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /raw_profile — показать сырые данные профиля (user_profiles JSON, обрезано).
    Удобно для отладки WebApp-онбординга.
    """
    user = update.effective_user
    if not user or not update.message:
        return

    if not supabase:
        await update.message.reply_text("Supabase не настроен, профиль недоступен.")
        return

    profile = await load_user_profile(user.id)
    if not profile:
        await update.message.reply_text(
            "Профиль не найден. Заполни город и темы в WebApp через /start."
        )
        return

    structured = profile.get("structured_profile")
    raw_interests = profile.get("raw_interests") or ""
    location_city = profile.get("location_city")
    location_country = profile.get("location_country")

    lines: List[str] = []
    lines.append("user_profiles (обрезано):")
    lines.append(f"user_id: {profile.get('user_id')}")
    lines.append(f"location_city: {location_city}")
    lines.append(f"location_country: {location_country}")
    lines.append("")

    if raw_interests:
        lines.append("raw_interests:")
        snippet = raw_interests
        if len(snippet) > 800:
            snippet = snippet[:797] + "..."
        lines.append(snippet)
        lines.append("")

    if structured is not None:
        if isinstance(structured, str):
            structured_str = structured
        else:
            try:
                structured_str = json.dumps(structured, ensure_ascii=False, indent=2)
            except Exception:
                structured_str = str(structured)
        lines.append("structured_profile (обрезано):")
        if len(structured_str) > 1600:
            structured_str = structured_str[:1597] + "..."
        lines.append(structured_str)
    else:
        lines.append("structured_profile: отсутствует (может быть добавлен позже).")

    await update.message.reply_text("\n".join(lines))


async def reset_profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reset_profile — удалить профиль и выбранные темы для текущего пользователя.
    """
    user = update.effective_user
    if not user or not update.message:
        return

    if not supabase:
        await update.message.reply_text("Supabase не настроен, сброс профиля невозможен.")
        return

    ok = await delete_user_profile(user.id)
    if ok:
        await update.message.reply_text(
            "Я удалил твой профиль (город и выбранные темы).\n"
            "Можешь снова пройти онбординг в WebApp через /start."
        )
    else:
        await update.message.reply_text(
            "Не получилось удалить профиль. Попробуй ещё раз позже."
        )


# ==========================
# Глобальный обработчик ошибок
# ==========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Логируем любые неотловленные ошибки и аккуратно сообщаем пользователю.
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
    application.add_handler(CommandHandler("webapp", webapp_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("me", me))
    application.add_handler(CommandHandler("raw_profile", raw_profile_command))
    application.add_handler(CommandHandler("reset_profile", reset_profile_command))

    application.add_error_handler(error_handler)

    return application


def main() -> None:
    app = build_application()
    app.run_polling()


if __name__ == "__main__":
    main()

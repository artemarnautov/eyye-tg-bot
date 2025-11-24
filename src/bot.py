# file: src/bot.py
import logging
import os
from typing import Optional, Any, Dict, List

from dotenv import load_dotenv
from supabase import Client, create_client
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
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
# Работа с Supabase: telegram_users
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
        response = (
            supabase.table("telegram_users")
            .upsert(data, on_conflict="id")
            .execute()
        )
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
# Работа с Supabase: user_profiles
# ==========================

async def load_user_profile(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Читаем профиль пользователя из таблицы user_profiles по user_id.
    Возвращаем dict или None.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip load_user_profile")
        return None

    try:
        result = (
            supabase.table("user_profiles")
            .select("*")
            .eq("user_id", telegram_id)
            .single()
            .execute()
        )
        data = getattr(result, "data", None)
        if isinstance(data, list):
            return data[0] if data else None
        return data
    except Exception as e:
        logger.exception("Error loading user profile from Supabase: %s", e)
        return None


async def upsert_user_profile(
    telegram_id: int,
    raw_interests: str,
    location_city: Optional[str] = None,
    location_country: Optional[str] = None,
) -> bool:
    """
    Создаём или обновляем профиль пользователя в таблице user_profiles.
    Пока location_* не парсим и обычно не заполняем.
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip upsert_user_profile")
        return False

    data: Dict[str, Any] = {
        "user_id": telegram_id,
        "raw_interests": raw_interests,
    }
    if location_city is not None:
        data["location_city"] = location_city
    if location_country is not None:
        data["location_country"] = location_country

    try:
        response = (
            supabase.table("user_profiles")
            .upsert(data, on_conflict="user_id")
            .execute()
        )
        logger.info("Upsert user profile %s: %s", telegram_id, response)
        return True
    except Exception as e:
        logger.exception("Error saving user profile to Supabase: %s", e)
        return False


# ==========================
# Хендлеры команд
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — сохраняем пользователя в Supabase.
    Если Supabase настроен и профиля ещё нет — запускаем онбординг по интересам.
    """
    user = update.effective_user

    if user:
        await save_user_to_supabase(user.id, user.username)

    # Если нет сообщения (стрёмный апдейт) — просто выходим
    if not update.message:
        return

    # Если Supabase не настроен — ведём себя как раньше, без онбординга
    if not supabase or not user:
        text_lines = [
            "Привет! Это EYYE — твой персональный новостной ассистент.",
            "",
            "Пока что бот умеет немногое:",
            "/ping — проверить, что бот жив",
            "/me — показать, что бот знает о твоём аккаунте",
            "/help — показать справку",
        ]
        await update.message.reply_text("\n".join(text_lines))
        return

    # Проверяем, есть ли уже профиль интересов
    profile = await load_user_profile(user.id)

    if profile:
        # Профиль уже есть — приветствуем и даём подсказки
        context.user_data["awaiting_profile"] = False
        context.user_data["profile_buffer"] = []

        text_lines = [
            "Снова привет 👋",
            "",
            "Я уже помню твои интересы и город.",
            "",
            "Команды:",
            "/me — показать, что я о тебе знаю",
            "/help — показать справку",
            "/ping — проверить, что бот жив",
        ]
        await update.message.reply_text("\n".join(text_lines))
        return

    # Профиля ещё нет — запускаем онбординг по свободному тексту
    context.user_data["awaiting_profile"] = True
    context.user_data["profile_buffer"] = []

    text_lines = [
        "Привет 👋",
        "",
        "Я — EYYE, твой персональный новостной ассистент.",
        "Чтобы настроить ленту под тебя, расскажи в свободной форме:",
        "",
        "• что тебе интересно читать (темы, форматы, люди);",
        "• в каком городе/стране ты живёшь или учишься;",
        "• что точно не хочется видеть (например, политика, военные новости).",
        "",
        "Можешь написать одним или несколькими сообщениями.",
        "Когда всё опишешь — просто отправь команду /done.",
        "",
        "— Жду твоё первое сообщение 🙂",
    ]
    await update.message.reply_text("\n".join(text_lines))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /help — список команд.
    """
    text_lines = [
        "Доступные команды:",
        "/start — перезапустить бота и (при необходимости) пройти онбординг",
        "/ping — проверить, что бот жив",
        "/me — показать, что бот знает о тебе в базе и в Telegram",
        "/done — закончить описание интересов во время онбординга",
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
    - если получится, данные из Supabase по пользователю,
    - профиль интересов из user_profiles (если есть).
    """
    user = update.effective_user
    if not user:
        if update.message:
            await update.message.reply_text("Не получилось определить твой Telegram-профиль.")
        return

    # На всякий случай ещё раз сохраняем пользователя
    await save_user_to_supabase(user.id, user.username)

    # Базовая информация из Telegram
    tg_lines: List[str] = [
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

    # Пытаемся прочитать запись из telegram_users
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
    sb_lines: List[str] = [
        "Информация о тебе в базе EYYE (Supabase / telegram_users):",
        f"id: {row.get('id')}",
        f"username: {row.get('username')}",
        f"created_at: {row.get('created_at')}",
        "",
    ]

    # Профиль интересов (user_profiles)
    profile = await load_user_profile(user.id)
    profile_lines: List[str] = []

    if profile:
        profile_lines.append("Профиль интересов (user_profiles):")
        raw = profile.get("raw_interests") or ""
        profile_lines.append("raw_interests:")
        profile_lines.append(raw)
        profile_lines.append("")
        loc_city = profile.get("location_city")
        loc_country = profile.get("location_country")
        if loc_city or loc_country:
            profile_lines.append("Локация (если заполнена):")
            if loc_city:
                profile_lines.append(f"- город: {loc_city}")
            if loc_country:
                profile_lines.append(f"- страна: {loc_country}")
            profile_lines.append("")
    else:
        profile_lines.append("Профиль интересов ещё не заполнен.")
        profile_lines.append("Напиши /start, чтобы пройти онбординг или обновить данные.")
        profile_lines.append("")

    all_lines = tg_lines + sb_lines + profile_lines

    if update.message:
        await update.message.reply_text("\n".join(all_lines))


# ==========================
# Онбординг: обработка текста и /done
# ==========================

async def onboarding_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатываем обычные текстовые сообщения.
    Если мы в состоянии онбординга (awaiting_profile=True) —
    добавляем текст в буфер профиля.
    Если нет — просто даём подсказку про /help.
    """
    if not update.message:
        return

    user = update.effective_user
    if not user:
        return

    text = (update.message.text or "").strip()
    if not text:
        return

    # Если сейчас не ждём описание интересов — мягкая подсказка
    if not context.user_data.get("awaiting_profile"):
        await update.message.reply_text(
            "Я пока понимаю только команды. Напиши /help, чтобы увидеть список."
        )
        return

    # Мы в режиме онбординга — записываем текст в буфер
    buffer: List[str] = context.user_data.get("profile_buffer", [])
    buffer.append(text)
    context.user_data["profile_buffer"] = buffer

    logger.info(
        "Onboarding text from user %s: %s (buffer size now %d)",
        user.id,
        text,
        len(buffer),
    )

    await update.message.reply_text(
        "Записал 👍\n\n"
        "Можешь добавить ещё одно-два сообщения с интересами или деталями.\n"
        "Когда всё опишешь — просто отправь команду /done."
    )


async def finish_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /done — завершение онбординга:
    склеиваем все собранные сообщения и сохраняем в user_profiles.
    """
    if not update.message:
        return

    user = update.effective_user
    if not user:
        await update.message.reply_text("Не получилось определить твой Telegram-профиль.")
        return

    # Если мы вообще не в режиме онбординга
    if not context.user_data.get("awaiting_profile"):
        await update.message.reply_text(
            "Сейчас я не собираю описание интересов.\n"
            "Если хочешь обновить профиль, напиши /start."
        )
        return

    buffer: List[str] = context.user_data.get("profile_buffer", [])
    raw_interests = "\n\n".join(buffer).strip()

    if not raw_interests:
        await update.message.reply_text(
            "Похоже, ты ещё ничего не написал 🙈\n"
            "Опиши, пожалуйста, в одном-двух сообщениях свои интересы и город, "
            "а потом снова отправь /done."
        )
        return

    # Сохраняем профиль в Supabase
    ok = await upsert_user_profile(user.id, raw_interests)

    if not ok:
        await update.message.reply_text(
            "Не получилось сохранить профиль. Попробуй, пожалуйста, ещё раз чуть позже."
        )
        return

    # Сбрасываем состояние онбординга
    context.user_data["awaiting_profile"] = False
    context.user_data["profile_buffer"] = []

    await update.message.reply_text(
        "Отлично, я запомнил твои интересы и город 🙌\n\n"
        "На основе этого я буду подбирать для тебя персональную ленту."
    )


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

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("me", me))
    application.add_handler(CommandHandler("done", finish_onboarding))

    # Текстовые сообщения (без команд) — для онбординга
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            onboarding_message,
        )
    )

    application.add_error_handler(error_handler)

    return application


def main() -> None:
    app = build_application()
    app.run_polling()


if __name__ == "__main__":
    main()

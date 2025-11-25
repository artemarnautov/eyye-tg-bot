import logging
import os
import asyncio
import json
import urllib.request
import urllib.error
from typing import Optional, Any, Dict, List, cast
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import Client, create_client
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
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

# Читаем токен бота: сначала BOT_TOKEN, потом TELEGRAM_BOT_TOKEN (на всякий случай)
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Модель по умолчанию — gpt-4.1-mini (можно переопределить в .env)
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Базовый URL для OpenAI + endpoint Chat Completions
OPENAI_API_BASE = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_CHAT_COMPLETIONS_URL = OPENAI_API_BASE.rstrip("/") + "/chat/completions"

# Таймаут HTTP-запроса к OpenAI (секунды)
OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))

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
# Константы для тем
# ==========================

TOPIC_CHOOSE_BUTTON_TEXT = "Выбрать темы"
START_READING_BUTTON_TEXT = "Начать читать"
EXIT_TOPICS_BUTTON_TEXT = "⬅️ Назад"
BACK_TO_MAIN_TOPICS_BUTTON_TEXT = "⬅️ Назад к общим темам"

MAIN_TOPICS: List[str] = [
    "Бизнес и экономика",
    "Финансы и крипто",
    "Технологии и гаджеты",
    "Наука",
    "История",
    "Политика",
    "Общество и культура",
    "Шоу-бизнес и музыка",
    "Кино и сериалы",
    "Игры и киберспорт",
    "Спорт",
    "Жизнь и лайфстайл (путешествия, еда, мода)",
    "Здоровье и саморазвитие",
    "Образование и карьера (универы, стажировки, студенческая жизнь)",
    "Город и локальные новости",
]

SPORT_SUBTOPICS: List[str] = [
    "Футбол",
    "Баскетбол",
    "Теннис",
    "Хоккей",
    "Бег и марафоны",
    "Боевые виды спорта",
    "Формула-1 и автоспорт",
]


def strip_checkmark(text: str) -> str:
    """
    Убираем префикс '✅ ' у текста кнопки, если он есть.
    """
    if text.startswith("✅"):
        return text.lstrip("✅").strip()
    return text


def build_choose_topics_entry_keyboard() -> ReplyKeyboardMarkup:
    """
    Клавиатура, которая появляется сразу после /start:
    показывает только одну кнопку "Выбрать темы".
    """
    return ReplyKeyboardMarkup(
        [[TOPIC_CHOOSE_BUTTON_TEXT]],
        resize_keyboard=True,
    )


def build_main_topics_keyboard(selected_topics: List[str]) -> ReplyKeyboardMarkup:
    """
    Клавиатура с основными темами.
    Выбранные темы помечаем '✅ '.
    Внизу: большая кнопка "Начать читать" и под ней "⬅️ Назад".
    """
    selected = set(selected_topics)

    def label(topic: str) -> str:
        return f"✅ {topic}" if topic in selected else topic

    keyboard: List[List[str]] = [
        [label(MAIN_TOPICS[0]), label(MAIN_TOPICS[1])],
        [label(MAIN_TOPICS[2]), label(MAIN_TOPICS[3])],
        [label(MAIN_TOPICS[4]), label(MAIN_TOPICS[5])],
        [label(MAIN_TOPICS[6]), label(MAIN_TOPICS[7])],
        [label(MAIN_TOPICS[8]), label(MAIN_TOPICS[9])],
        [label(MAIN_TOPICS[10]), label(MAIN_TOPICS[11])],
        [label(MAIN_TOPICS[12]), label(MAIN_TOPICS[13])],
        [label(MAIN_TOPICS[14])],
        [START_READING_BUTTON_TEXT],
        [EXIT_TOPICS_BUTTON_TEXT],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def build_sport_topics_keyboard(selected_topics: List[str]) -> ReplyKeyboardMarkup:
    """
    Клавиатура с подкатегориями спорта.
    Выбранные помечаем '✅ '.
    Внизу: "Начать читать", затем "⬅️ Назад к общим темам" и "⬅️ Назад".
    """
    selected = set(selected_topics)

    def label(topic: str) -> str:
        return f"✅ {topic}" if topic in selected else topic

    keyboard: List[List[str]] = [
        [label(SPORT_SUBTOPICS[0]), label(SPORT_SUBTOPICS[1])],
        [label(SPORT_SUBTOPICS[2]), label(SPORT_SUBTOPICS[3])],
        [label(SPORT_SUBTOPICS[4]), label(SPORT_SUBTOPICS[5])],
        [label(SPORT_SUBTOPICS[6])],
        [START_READING_BUTTON_TEXT],
        [BACK_TO_MAIN_TOPICS_BUTTON_TEXT],
        [EXIT_TOPICS_BUTTON_TEXT],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def update_topics_keyboard_markup(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    topics_mode: Optional[str],
    selected_topics: List[str],
) -> None:
    """
    Обновляем только разметку клавиатуры (без новых сообщений),
    чтобы показать выбранные темы чекбоксами.
    """
    if topics_mode == "main":
        keyboard = build_main_topics_keyboard(selected_topics)
    elif topics_mode == "sports":
        keyboard = build_sport_topics_keyboard(selected_topics)
    else:
        return

    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=keyboard,
        )
    except Exception as e:
        logger.error("Failed to update topics keyboard: %s", e)


# ==========================
# Работа с Supabase: telegram_users
# ==========================

async def save_user_to_supabase(telegram_id: int, username: Optional[str]) -> None:
    """
    upsert в таблицу telegram_users.
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
        logger.exception("Error saving user to Supabase: %s", e)


async def load_user_from_supabase(telegram_id: int) -> Optional[dict]:
    """
    Читаем пользователя из таблицы telegram_users по id.
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
        logger.exception("Error loading user from Supabase: %s", e)
        return None


# ==========================
# Работа с Supabase: user_profiles
# ==========================

async def load_user_profile(telegram_id: int) -> Optional[Dict[str, Any]]:
    """
    Профиль пользователя из user_profiles.
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
    upsert в user_profiles (raw_interests + опционально location_*).
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


async def upsert_user_profile_structured(
    telegram_id: int,
    structured_profile: Dict[str, Any],
    raw_interests: Optional[str] = None,
) -> bool:
    """
    upsert structured_profile в user_profiles.
    (На будущее, сейчас напрямую не зовём.)
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip upsert_user_profile_structured")
        return False

    data: Dict[str, Any] = {
        "user_id": telegram_id,
        "structured_profile": structured_profile,
    }

    loc_city = structured_profile.get("location_city") or structured_profile.get("city")
    loc_country = structured_profile.get("location_country") or structured_profile.get("country")

    if loc_city:
        data["location_city"] = loc_city
    if loc_country:
        data["location_country"] = loc_country
    if raw_interests is not None:
        data["raw_interests"] = raw_interests

    try:
        response = (
            supabase.table("user_profiles")
            .upsert(data, on_conflict="user_id")
            .execute()
        )
        logger.info("Upsert structured_profile for %s: %s", telegram_id, response)
        return True
    except Exception as e:
        logger.exception("Error saving structured_profile to Supabase: %s", e)
        return False


# ==========================
# OpenAI: structured_profile
# ==========================

def _build_fallback_profile_from_raw(raw_interests: str) -> Dict[str, Any]:
    """
    Очень простой fallback-профиль, если OpenAI не ответил.
    Строим темы по строкам raw_interests, которые совпадают с MAIN_TOPICS / SPORT_SUBTOPICS.
    """
    lines = [l.strip() for l in (raw_interests or "").splitlines() if l.strip()]

    topics: List[Dict[str, Any]] = []

    def map_category(name: str) -> Optional[str]:
        if name == "Бизнес и экономика":
            return "business"
        if name == "Финансы и крипто":
            return "finance"
        if name == "Технологии и гаджеты":
            return "tech"
        if name == "Наука":
            return "science"
        if name == "История":
            return "history"
        if name == "Политика":
            return "politics"
        if name in ("Спорт", *SPORT_SUBTOPICS):
            return "sports"
        if name == "Образование и карьера (универы, стажировки, студенческая жизнь)":
            return "education"
        if name == "Жизнь и лайфстайл (путешествия, еда, мода)":
            return "lifestyle"
        return None

    for line in lines:
        if line.lower().startswith("выбранные темы"):
            continue

        if line in MAIN_TOPICS or line in SPORT_SUBTOPICS:
            category = map_category(line)
            topics.append(
                {
                    "name": line.lower(),
                    "weight": 1.0,
                    "category": category,
                    "detail": None,
                }
            )

    tags: List[str] = []
    for t in topics:
        cat = t.get("category")
        if cat and cat not in tags:
            tags.append(cat)

    return {
        "location_city": None,
        "location_country": None,
        "topics": topics,
        "negative_topics": [],
        "interests_as_tags": tags,
        "user_meta": {
            "age_group": None,
            "student_status": None,
        },
    }


def _normalize_profile_dict(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализация профиля: дефолты и чистка.
    """
    profile = dict(profile)

    profile.setdefault("location_city", None)
    profile.setdefault("location_country", None)
    profile.setdefault("topics", [])
    profile.setdefault("negative_topics", [])
    profile.setdefault("interests_as_tags", [])
    profile.setdefault("user_meta", {})

    topics = profile.get("topics")
    if not isinstance(topics, list):
        topics = []
    normalized_topics: List[Dict[str, Any]] = []
    for t in topics:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name", "")).strip()
        if not name:
            continue
        weight = t.get("weight", 1.0)
        try:
            weight = float(weight)
        except (TypeError, ValueError):
            weight = 1.0
        category = t.get("category")
        detail = t.get("detail")
        normalized_topics.append(
            {
                "name": name,
                "weight": weight,
                "category": category,
                "detail": detail,
            }
        )
    profile["topics"] = normalized_topics

    neg = profile.get("negative_topics")
    if not isinstance(neg, list):
        neg = []
    profile["negative_topics"] = [str(x).strip() for x in neg if str(x).strip()]

    tags = profile.get("interests_as_tags")
    if not isinstance(tags, list):
        tags = []
    profile["interests_as_tags"] = [str(x).strip() for x in tags if str(x).strip()]

    user_meta = profile.get("user_meta")
    if not isinstance(user_meta, dict):
        user_meta = {}
    profile["user_meta"] = user_meta

    return profile


def call_openai_chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Обёртка вокруг OpenAI Chat Completions.
    Принимает payload со старыми полями (input, max_output_tokens и т.п.),
    под капотом бьёт в /v1/chat/completions.
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skipping OpenAI call")
        return {}

    url = OPENAI_CHAT_COMPLETIONS_URL
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    model = payload.get("model") or OPENAI_MODEL or "gpt-4.1-mini"

    # 1) если передали messages — используем их;
    # 2) если нет, смотрим input (список сообщений или строка).
    messages = payload.get("messages")
    if not messages:
        input_field = payload.get("input")
        if isinstance(input_field, list):
            messages = input_field
        else:
            messages = [{"role": "user", "content": str(input_field)}]

    max_tokens = payload.get("max_tokens")
    if max_tokens is None:
        max_tokens = payload.get("max_output_tokens", 512)
    try:
        max_tokens_int = int(max_tokens)
    except (TypeError, ValueError):
        max_tokens_int = 512

    temperature = payload.get("temperature", 0.2)
    try:
        temperature_float = float(temperature)
    except (TypeError, ValueError):
        temperature_float = 0.2

    body: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens_int,
        "temperature": temperature_float,
    }

    if "response_format" in payload:
        body["response_format"] = payload["response_format"]

    data = json.dumps(body).encode("utf-8")

    started_at = datetime.now(timezone.utc)
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("OpenAI chat.completions call OK (%.2fs)", elapsed)
        return json.loads(raw)
    except urllib.error.HTTPError as e:
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = "<no body>"
        logger.error(
            "OpenAI HTTPError in chat.completions (%.2fs), code=%s, body=%s",
            elapsed,
            e.code,
            error_body[:1000],
        )
        return {}
    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.exception("Error calling OpenAI chat.completions (%.2fs): %s", elapsed, e)
        return {}


def _call_openai_structured_profile_sync(raw_interests: str) -> Dict[str, Any]:
    """
    Строим structured_profile через gpt-4.1-mini в JSON-режиме.
    Если что-то пошло не так — fallback из raw_interests.
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skipping structured_profile build")
        return _normalize_profile_dict(_build_fallback_profile_from_raw(raw_interests))

    system_prompt = """
Ты помогаешь новостному рекомендательному сервису EYYE.
По свободному описанию интересов и города пользователя ты должен вернуть
СТРОГО JSON-объект с полями:

{
  "location_city": string | null,
  "location_country": string | null,
  "topics": [
    {
      "name": string,
      "weight": number,
      "category": string | null,
      "detail": string | null
    },
    ...
  ],
  "negative_topics": [string, ...],
  "interests_as_tags": [string, ...],
  "user_meta": {
    "age_group": string | null,
    "student_status": string | null
  }
}

Требования:
- Никакого текста вне JSON.
- Если информации нет — используй null или пустые массивы.
- weight от 0.0 до 1.0.
- category — общий род ("business", "sports", "culture", "tech", "education" и т.п.) или null.
- interests_as_tags — короткие теги латиницей ("startups", "premier_league", "uk_universities").
"""

    payload: Dict[str, Any] = {
        "model": OPENAI_MODEL or "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": raw_interests},
        ],
        "max_output_tokens": 800,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    resp_json = call_openai_chat(payload)
    if not resp_json:
        logger.warning(
            "OpenAI did not return response JSON for structured_profile. Using fallback from raw_interests."
        )
        fallback = _build_fallback_profile_from_raw(raw_interests)
        return _normalize_profile_dict(fallback)

    try:
        choices = resp_json.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("No choices in OpenAI response")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Empty content in OpenAI response")

        logger.debug(
            "OpenAI structured_profile raw content (first 200 chars): %s",
            content[:200].replace("\n", " "),
        )

        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("Parsed JSON is not an object")

        return _normalize_profile_dict(parsed)
    except Exception:
        logger.exception("Failed to parse OpenAI structured_profile response. Using fallback.")
        fallback = _build_fallback_profile_from_raw(raw_interests)
        return _normalize_profile_dict(fallback)


def build_and_save_structured_profile(user_id: int, raw_interests: str) -> None:
    """
    Строит structured_profile (через OpenAI или fallback) и сохраняет в Supabase.
    """
    text_len = len(raw_interests or "")
    logger.info(
        "build_and_save_structured_profile: start for user_id=%s, raw_interests_len=%s",
        user_id,
        text_len,
    )

    try:
        profile = _call_openai_structured_profile_sync(raw_interests)
    except Exception:
        logger.exception(
            "build_and_save_structured_profile: unexpected error in _call_openai_structured_profile_sync "
            "for user_id=%s",
            user_id,
        )
        return

    if not profile or not isinstance(profile, dict):
        logger.warning(
            "build_and_save_structured_profile: got empty or invalid structured_profile for user_id=%s",
            user_id,
        )
        return

    if not supabase:
        logger.warning(
            "build_and_save_structured_profile: supabase client is not configured, skip saving for user_id=%s",
            user_id,
        )
        return

    update_data = {
        "location_city": profile.get("location_city"),
        "location_country": profile.get("location_country"),
        "structured_profile": profile,
    }

    try:
        table = supabase.table("user_profiles")

        resp = table.update(update_data).eq("user_id", user_id).execute()
        data_list = getattr(resp, "data", None)

        logger.info(
            "Update structured_profile for user_id=%s: data=%s count=%s",
            user_id,
            data_list,
            getattr(resp, "count", None),
        )

        if not data_list:
            insert_data = {
                "user_id": user_id,
                "raw_interests": raw_interests or "",
                "location_city": profile.get("location_city"),
                "location_country": profile.get("location_country"),
                "structured_profile": profile,
            }
            resp_ins = table.insert(insert_data).execute()
            logger.info(
                "Insert user_profile with structured_profile for user_id=%s: data=%s count=%s",
                user_id,
                getattr(resp_ins, "data", None),
                getattr(resp_ins, "count", None),
            )

    except Exception:
        logger.exception(
            "Unexpected error while saving structured_profile for user_id=%s",
            user_id,
        )


# ==========================
# Команды
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — онбординг или приветствие, если профиль уже есть.
    """
    user = update.effective_user

    if user:
        await save_user_to_supabase(user.id, user.username)

    if not update.message:
        return

    if not supabase or not user:
        text_lines = [
            "Привет! Это EYYE — твой персональный новостной ассистент.",
            "",
            "Пока что бот умеет немногое:",
            "/ping — проверить, что бот жив",
            "/me — показать, что бот знает о твоём аккаунте",
            "/feed — черновой список тем, по которым я буду искать новости (когда будет профиль)",
            "/help — показать справку",
        ]
        await update.message.reply_text("\n".join(text_lines))
        return

    profile = await load_user_profile(user.id)

    if profile:
        context.user_data["awaiting_profile"] = False
        context.user_data["profile_buffer"] = []
        context.user_data["selected_topics"] = []
        context.user_data["topics_mode"] = None
        context.user_data["topics_keyboard_message_id"] = None
        context.user_data["topics_keyboard_chat_id"] = None

        text_lines = [
            "Снова привет 👋",
            "",
            "Я уже помню твои интересы и город.",
            "",
            "Команды:",
            "/me — показать, что я о тебе знаю",
            "/feed — по каким темам буду искать новости",
            "/help — показать справку",
            "/ping — проверить, что бот жив",
        ]
        await update.message.reply_text(
            "\n".join(text_lines),
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    # Профиля ещё нет — запускаем онбординг
    context.user_data["awaiting_profile"] = True
    context.user_data["profile_buffer"] = []
    context.user_data["selected_topics"] = []
    context.user_data["topics_mode"] = None
    context.user_data["topics_keyboard_message_id"] = None
    context.user_data["topics_keyboard_chat_id"] = None

    text_lines = [
        "Привет 👋",
        "",
        "Я — EYYE, твой персональный новостной ассистент.",
        "Чтобы настроить ленту под тебя, можно сделать так:",
        "",
        "1) Написать в свободной форме, что тебе интересно читать,",
        "   где ты живёшь/учишься и что не хочется видеть.",
        "",
        "2) Или нажать кнопку «Выбрать темы» ниже и выбрать из списка общих тем.",
        "",
        "Можешь комбинировать оба подхода: и выбирать темы, и дописывать детали текстом.",
        "Когда всё опишешь — просто отправь команду /done.",
        "",
        "— Жду твоё первое сообщение 🙂",
    ]
    await update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=build_choose_topics_entry_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text_lines = [
        "Доступные команды:",
        "/start — перезапустить бота и (при необходимости) пройти онбординг",
        "/ping — проверить, что бот жив",
        "/me — показать, что бот знает о тебе в базе и в Telegram",
        "/feed — черновой вывод, по каким темам я буду искать новости",
        "/done — закончить описание интересов во время онбординга",
        "/help — эта справка",
    ]
    if update.message:
        await update.message.reply_text("\n".join(text_lines))


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text("pong")


async def me(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /me — Telegram-данные + Supabase + structured_profile (если есть).
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

    row = await load_user_from_supabase(user.id)

    if not row:
        tg_lines.append(
            "Supabase сейчас отвечает с ошибкой или запись ещё не создана.\n"
            "Показываю только данные из Telegram."
        )
        if update.message:
            await update.message.reply_text("\n".join(tg_lines))
        return

    sb_lines: List[str] = [
        "Информация о тебе в базе EYYE (Supabase / telegram_users):",
        f"id: {row.get('id')}",
        f"username: {row.get('username')}",
        f"created_at: {row.get('created_at')}",
        "",
    ]

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

        structured = profile.get("structured_profile")
        if structured is None:
            profile_lines.append("structured_profile: ещё не посчитан или пуст.")
        else:
            if isinstance(structured, str):
                try:
                    structured_data = json.loads(structured)
                except json.JSONDecodeError:
                    structured_data = None
            else:
                structured_data = structured

            if not isinstance(structured_data, dict):
                profile_lines.append("structured_profile: есть, но не удалось распарсить JSON.")
            else:
                profile_lines.append("structured_profile:")
                sp_city = structured_data.get("location_city") or "—"
                sp_country = structured_data.get("location_country") or "—"
                profile_lines.append(f"- city: {sp_city}")
                profile_lines.append(f"- country: {sp_country}")

                topics = structured_data.get("topics") or []
                if topics:
                    profile_lines.append("- topics:")
                    for topic in topics:
                        name = topic.get("name") or "unknown"
                        weight = topic.get("weight")
                        if isinstance(weight, (int, float)):
                            weight_str = f"{weight:.2f}"
                        else:
                            weight_str = "?"
                        profile_lines.append(f"  • {name} ({weight_str})")
                else:
                    profile_lines.append("- topics: []")

                negative = structured_data.get("negative_topics") or []
                if negative:
                    profile_lines.append("- negative_topics:")
                    for nt in negative:
                        profile_lines.append(f"  • {nt}")
                else:
                    profile_lines.append("- negative_topics: []")
    else:
        profile_lines.append("Профиль интересов ещё не заполнен.")
        profile_lines.append("Напиши /start, чтобы пройти онбординг или обновить данные.")
        profile_lines.append("")

    all_lines = tg_lines + sb_lines + profile_lines

    if update.message:
        await update.message.reply_text("\n".join(all_lines))


async def feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /feed — читаем structured_profile.
    Если его ещё нет — быстрый fallback по raw_interests + параллельно строим нормальный профиль.
    """
    user = update.effective_user
    message = update.effective_message

    if user is None or message is None:
        return

    if supabase is None:
        await message.reply_text("Внутренняя ошибка: база профилей не настроена.")
        return

    try:
        resp = (
            supabase.table("user_profiles")
            .select("structured_profile, raw_interests")
            .eq("user_id", user.id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load profile from Supabase for user_id=%s", user.id)
        await message.reply_text("Не получилось получить твой профиль интересов. Попробуй ещё раз позже.")
        return

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        await message.reply_text(
            "Я пока не знаю твоих интересов. Пройди, пожалуйста, онбординг через /start, "
            "а потом попробуй /feed ещё раз."
        )
        return

    row = data[0]
    structured = row.get("structured_profile")
    raw_interests = row.get("raw_interests") or ""

    if structured is not None:
        if isinstance(structured, str):
            try:
                structured = json.loads(structured)
            except Exception:
                logger.exception("Failed to parse structured_profile JSON for user_id=%s", user.id)
                await message.reply_text(
                    "Твой структурированный профиль сейчас в странном формате. "
                    "Попробуй пройти онбординг заново позже."
                )
                return

        if not isinstance(structured, dict):
            await message.reply_text(
                "Твой профиль интересов сейчас в непонятном формате. "
                "Попробуй пройти онбординг заново позже."
            )
            return

        profile_dict = structured
        using_fallback = False
    else:
        if not raw_interests:
            await message.reply_text(
                "Похоже, у меня пока нет ни структурированного профиля, ни исходного описания интересов 😔\n"
                "Напиши /start, чтобы пройти онбординг."
            )
            return

        profile_dict = _normalize_profile_dict(_build_fallback_profile_from_raw(raw_interests))
        using_fallback = True

        if OPENAI_API_KEY:
            application: Application = cast(Application, context.application)
            try:
                application.create_task(
                    asyncio.to_thread(build_and_save_structured_profile, user.id, raw_interests)
                )
                logger.info(
                    "feed: scheduled build_and_save_structured_profile for user_id=%s (fallback mode)",
                    user.id,
                )
            except Exception:
                logger.exception(
                    "feed: failed to schedule build_and_save_structured_profile for user_id=%s",
                    user.id,
                )

    topics = profile_dict.get("topics") or []
    negative_topics = profile_dict.get("negative_topics") or []
    tags = profile_dict.get("interests_as_tags") or []

    lines: List[str] = []

    topic_names: List[str] = []
    for t in topics:
        if isinstance(t, dict):
            name = t.get("name")
            if name:
                topic_names.append(str(name))
    topic_names = topic_names[:8]

    if topic_names:
        lines.append("Я буду искать новости по темам: " + ", ".join(topic_names) + ".")

    if tags:
        tags_str = ", ".join(str(x) for x in tags[:10])
        lines.append("Теги интересов: " + tags_str + ".")

    if negative_topics:
        neg_str = ", ".join(str(x) for x in negative_topics[:8])
        lines.append("Буду стараться избегать тем: " + neg_str + ".")

    if not lines:
        lines.append(
            "У меня пока нет достаточно структурированных данных о твоих интересах. "
            "Как только профиль обновится, я смогу подбирать под тебя новости."
        )

    if using_fallback:
        lines.append(
            "\nСейчас я ориентируюсь на быстрый черновой профиль по твоим выборам. "
            "Параллельно строю более точный профиль с помощью ИИ."
        )

    await message.reply_text("\n".join(lines))


# ==========================
# Онбординг: текст + кнопки
# ==========================

async def onboarding_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Любые текстовые сообщения во время онбординга:
    либо выбор тем, либо свободный текст.
    """
    if not update.message:
        return

    user = update.effective_user
    if not user:
        return

    text_raw = (update.message.text or "").strip()
    if not text_raw:
        return

    if not context.user_data.get("awaiting_profile"):
        await update.message.reply_text(
            "Я пока понимаю только команды. Напиши /help, чтобы увидеть список."
        )
        return

    if text_raw == TOPIC_CHOOSE_BUTTON_TEXT:
        context.user_data["topics_mode"] = "main"
        selected_topics: List[str] = context.user_data.get("selected_topics", [])
        keyboard = build_main_topics_keyboard(selected_topics)
        sent = await update.message.reply_text(
            "Вот общие темы. Нажимай на те, что тебе интересны.\n"
            "Можно выбрать несколько. В любой момент жми «⬅️ Назад», чтобы вернуться к свободному вводу.",
            reply_markup=keyboard,
        )
        context.user_data["topics_keyboard_message_id"] = sent.message_id
        context.user_data["topics_keyboard_chat_id"] = sent.chat_id
        return

    if text_raw == START_READING_BUTTON_TEXT:
        await finish_onboarding(update, context)
        return

    if text_raw == EXIT_TOPICS_BUTTON_TEXT:
        context.user_data["topics_mode"] = None
        context.user_data["topics_keyboard_message_id"] = None
        context.user_data["topics_keyboard_chat_id"] = None
        keyboard = ReplyKeyboardRemove()
        await update.message.reply_text(
            "Убрал клавиатуру тем. Можешь продолжить писать своими словами 🙂",
            reply_markup=keyboard,
        )
        return

    if text_raw == BACK_TO_MAIN_TOPICS_BUTTON_TEXT:
        context.user_data["topics_mode"] = "main"
        selected_topics = context.user_data.get("selected_topics", [])
        keyboard = build_main_topics_keyboard(selected_topics)
        sent = await update.message.reply_text(
            "Вернул список общих тем. Можно выбирать дальше.",
            reply_markup=keyboard,
        )
        context.user_data["topics_keyboard_message_id"] = sent.message_id
        context.user_data["topics_keyboard_chat_id"] = sent.chat_id
        return

    text = strip_checkmark(text_raw)

    topics_mode: Optional[str] = context.user_data.get("topics_mode")
    selected_topics: List[str] = context.user_data.get("selected_topics", [])
    keyboard_message_id = context.user_data.get("topics_keyboard_message_id")
    keyboard_chat_id = context.user_data.get("topics_keyboard_chat_id")

    # Подтемы спорта
    if topics_mode == "sports" and text in SPORT_SUBTOPICS:
        selected = set(selected_topics)
        if text in selected:
            selected.remove(text)
        else:
            selected.add(text)
        context.user_data["selected_topics"] = list(selected)

        if keyboard_message_id and keyboard_chat_id:
            await update_topics_keyboard_markup(
                context,
                keyboard_chat_id,
                keyboard_message_id,
                topics_mode,
                context.user_data["selected_topics"],
            )
        return

    # Основные темы
    if topics_mode == "main":
        if text == "Спорт":
            context.user_data["topics_mode"] = "sports"
            selected_topics = context.user_data.get("selected_topics", [])
            keyboard = build_sport_topics_keyboard(selected_topics)
            sent = await update.message.reply_text(
                "Выбери вид спорта, который тебе интересен.\n"
                "Можно несколько. Кнопка «⬅️ Назад к общим темам» вернёт предыдущий список.",
                reply_markup=keyboard,
            )
            context.user_data["topics_keyboard_message_id"] = sent.message_id
            context.user_data["topics_keyboard_chat_id"] = sent.chat_id
            return

        if text in MAIN_TOPICS:
            selected = set(selected_topics)
            if text in selected:
                selected.remove(text)
            else:
                selected.add(text)
            context.user_data["selected_topics"] = list(selected)

            if keyboard_message_id and keyboard_chat_id:
                await update_topics_keyboard_markup(
                    context,
                    keyboard_chat_id,
                    keyboard_message_id,
                    topics_mode,
                    context.user_data["selected_topics"],
                )
            return

    # Свободный текст
    buffer: List[str] = context.user_data.get("profile_buffer", [])
    buffer.append(text_raw)
    context.user_data["profile_buffer"] = buffer

    logger.info(
        "Onboarding free-text from user %s: %s (buffer size now %d)",
        user.id,
        text_raw,
        len(buffer),
    )

    await update.message.reply_text(
        "Записал 👍\n\n"
        "Можешь добавить ещё сообщения с интересами или деталями.\n"
        "Когда всё опишешь — просто отправь команду /done или нажми «Начать читать»."
    )


async def finish_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /done — конец онбординга: сохраняем raw_interests и в фоне строим structured_profile.
    """
    if not update.message:
        return

    user = update.effective_user
    if not user:
        await update.message.reply_text("Не получилось определить твой Telegram-профиль.")
        return

    if not context.user_data.get("awaiting_profile"):
        await update.message.reply_text(
            "Сейчас я не собираю описание интересов.\n"
            "Если хочешь обновить профиль, напиши /start."
        )
        return

    buffer: List[str] = context.user_data.get("profile_buffer", [])
    selected_topics: List[str] = context.user_data.get("selected_topics", [])

    parts: List[str] = []
    if buffer:
        parts.append("\n\n".join(buffer).strip())

    if selected_topics:
        unique_topics = sorted(set(selected_topics))
        topics_block = "Выбранные темы:\n" + "\n".join(unique_topics)
        parts.append(topics_block)

    raw_interests = "\n\n".join(parts).strip()

    if not raw_interests:
        await update.message.reply_text(
            "Похоже, ты ещё ничего не написал и не выбрал 🙈\n"
            "Опиши, пожалуйста, в одном-двух сообщениях свои интересы и город "
            "или выбери что-то из тем, а потом снова отправь /done или нажми «Начать читать»."
        )
        return

    ok = await upsert_user_profile(user.id, raw_interests)

    if not ok:
        await update.message.reply_text(
            "Не получилось сохранить профиль. Попробуй, пожалуйста, ещё раз чуть позже."
        )
        return

    context.user_data["awaiting_profile"] = False
    context.user_data["profile_buffer"] = []
    context.user_data["selected_topics"] = []
    context.user_data["topics_mode"] = None
    context.user_data["topics_keyboard_message_id"] = None
    context.user_data["topics_keyboard_chat_id"] = None

    await update.message.reply_text(
        "Отлично, я запомнил твои интересы и выбранные темы 🙌\n\n"
        "Дальше я в фоне попробую аккуратно структурировать профиль с помощью ИИ, "
        "чтобы позже точнее подбирать тебе новости. Посмотреть профиль можно командой /me.",
        reply_markup=ReplyKeyboardRemove(),
    )

    if not supabase:
        logger.warning("Supabase is not configured, skip building structured_profile")
        return
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip building structured_profile")
        return

    application: Application = cast(Application, context.application)
    try:
        application.create_task(
            asyncio.to_thread(build_and_save_structured_profile, user.id, raw_interests)
        )
        logger.info(
            "finish_onboarding: scheduled build_and_save_structured_profile for user_id=%s",
            user.id,
        )
    except Exception:
        logger.exception("finish_onboarding: failed to schedule build_and_save_structured_profile")


# ==========================
# Глобальный обработчик ошибок
# ==========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    application.add_handler(CommandHandler("feed", feed))
    application.add_handler(CommandHandler("done", finish_onboarding))

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



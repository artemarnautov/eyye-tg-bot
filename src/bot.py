# file: src/bot.py
import logging
import os
import asyncio
import json
import urllib.request
import urllib.error
import time
import re
from typing import Optional, Any, Dict, List, Tuple, cast
from datetime import datetime, timezone, timedelta

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

# Простейший rate-limit для генерации ленты (в секундах)
FEED_OPENAI_COOLDOWN_SECONDS = int(os.getenv("FEED_OPENAI_COOLDOWN_SECONDS", "60"))

# === Новые константы для фида ===
FEED_CARDS_LIMIT = 15          # сколько карточек отправляем за один показ ленты
FEED_MAX_CARD_AGE_HOURS = 48   # насколько свежие карточки считаем актуальными
DEFAULT_FEED_TAGS = ["world_news", "business", "tech", "uk_students"]

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


# ==========================
# Вспомогательные функции
# ==========================

def strip_checkmark(text: str) -> str:
    """
    Убираем префикс '✅ ' у текста кнопки, если он есть.
    """
    if text.startswith("✅"):
        return text.lstrip("✅").strip()
    return text


def _truncate(text: str, max_len: int = 1500) -> str:
    """
    Обрезаем длинную строку для отправки в Telegram.
    """
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


# ==========================
# Клавиатуры
# ==========================

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

    Важно:
    - Не используем .single(), чтобы не ловить PGRST116,
      когда профиль ещё не создан (0 строк).
    - Возвращаем либо dict с профилем, либо None.
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
    except Exception as e:
        logger.exception("Error loading user profile from Supabase: %s", e)
        return None

    # В supabase-py результат обычно лежит в .data, иногда в .model
    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)

    if not data:
        # Нормальная ситуация: пользователь ещё не проходил онбординг
        logger.info("No user_profile row yet for user_id=%s", telegram_id)
        return None

    if isinstance(data, list):
        return data[0]

    if isinstance(data, dict):
        return data

    logger.warning(
        "Unexpected response format from user_profiles for user_id=%s: %r",
        telegram_id,
        data,
    )
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


async def delete_user_profile(telegram_id: int) -> bool:
    """
    Удаление профиля пользователя (для /reset_profile).
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip delete_user_profile")
        return False

    try:
        resp = (
            supabase.table("user_profiles")
            .delete()
            .eq("user_id", telegram_id)
            .execute()
        )
        logger.info("Deleted user_profile for %s: %s", telegram_id, resp)
        return True
    except Exception:
        logger.exception("Error deleting user profile from Supabase for %s", telegram_id)
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

        first_choice = choices[0] or {}
        message = first_choice.get("message") or {}
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
# Лента: карточки из таблицы cards
# ==========================

# Память rate-limit для генерации ленты (в памяти процесса)
_last_feed_openai_call: Dict[int, datetime] = {}


def _is_allowed_feed_openai_call(user_id: int) -> bool:
    """
    Проверяем, не слишком ли часто мы дергаем OpenAI для генерации НОВЫХ карточек.
    """
    if FEED_OPENAI_COOLDOWN_SECONDS <= 0:
        return True

    now = datetime.now(timezone.utc)
    last = _last_feed_openai_call.get(user_id)
    if not last:
        _last_feed_openai_call[user_id] = now
        return True

    delta = (now - last).total_seconds()
    if delta >= FEED_OPENAI_COOLDOWN_SECONDS:
        _last_feed_openai_call[user_id] = now
        return True

    return False


def get_user_topic_weights(user_id: int) -> Dict[str, float]:
    """
    Читаем таблицу user_topic_weights и возвращаем {tag: weight}.
    Если Supabase не настроен или запрос упал — возвращаем пустой словарь.
    """
    if not supabase:
        return {}

    try:
        resp = (
            supabase.table("user_topic_weights")
            .select("tag, weight")
            .eq("user_id", user_id)
            .execute()
        )
    except Exception:
        logger.exception("Error loading user_topic_weights for user_id=%s", user_id)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return {}

    result: Dict[str, float] = {}
    for row in data:
        tag = row.get("tag")
        if not tag:
            continue
        try:
            w = float(row.get("weight", 0.0))
        except (TypeError, ValueError):
            w = 0.0
        if w != 0.0:
            result[str(tag)] = w
    return result


def _extract_interest_tags_from_profile(profile_dict: Dict[str, Any]) -> List[str]:
    """
    Берём interests_as_tags из structured_profile / fallback-профиля.
    """
    tags = profile_dict.get("interests_as_tags") or []
    if not isinstance(tags, list):
        tags = []
    normalized: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s:
            normalized.append(s)
    # Убираем дубликаты, сохраняя порядок
    return list(dict.fromkeys(normalized))


def fetch_candidate_cards(tags: List[str], limit: int) -> List[Dict[str, Any]]:
    """
    Берём кандидатов из таблицы cards.
    - Если есть теги — берём карточки, у которых tags пересекаются с нашими тегами.
    - Если тегов нет — просто свежие карточки.
    """
    if not supabase:
        logger.warning("Supabase is not configured, fetch_candidate_cards -> []")
        return []

    try:
        query = supabase.table("cards").select("*").eq("is_active", True)

        if tags:
            # overlaps(tags, tags_array) -> оператор && в Postgres
            query = query.overlaps("tags", tags)

        resp = query.order("created_at", desc=True).limit(limit).execute()
    except Exception:
        logger.exception("Error fetching candidate cards from Supabase")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    return data or []


def _score_cards_for_user(
    cards: List[Dict[str, Any]],
    base_tags: List[str],
    topic_weights: Dict[str, float],
) -> List[Dict[str, Any]]:
    """
    Присваиваем скор каждой карточке: важность + совпадение по тегам + динамические веса + свежесть.
    """
    now = datetime.now(timezone.utc)
    base_tag_set = set(base_tags)

    scored: List[Tuple[float, Dict[str, Any]]] = []

    for card in cards:
        card_tags = card.get("tags") or []
        if not isinstance(card_tags, list):
            card_tags = []

        try:
            importance = float(card.get("importance_score") or 1.0)
        except (TypeError, ValueError):
            importance = 1.0

        # бонус за совпадение с базовыми тегами из профиля
        profile_bonus = 0.0
        for t in card_tags:
            if t in base_tag_set:
                profile_bonus += 0.3

        # бонус по динамическим весам
        dyn_bonus = 0.0
        for t in card_tags:
            dyn_bonus += topic_weights.get(t, 0.0)

        # бонус за свежесть
        recency_bonus = 0.0
        created_at = card.get("created_at")
        if isinstance(created_at, str):
            try:
                dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                age_hours = (now - dt).total_seconds() / 3600.0
                if age_hours < FEED_MAX_CARD_AGE_HOURS:
                    recency_bonus = (FEED_MAX_CARD_AGE_HOURS - age_hours) / FEED_MAX_CARD_AGE_HOURS
            except Exception:
                pass

        score = importance + profile_bonus + dyn_bonus + recency_bonus
        scored.append((score, card))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for score, c in scored]
# ==========================
# Разбор "кривого" JSON от OpenAI для карточек
# ==========================

# Ищем объекты вида { "id": "...", ... } в тексте,
# даже если общий JSON наверху поломан.
CARD_OBJECT_RE = re.compile(
    r'\{\s*"id"\s*:\s*"(?P<id>[^"]+)"(?P<body>.*?)\}',
    re.DOTALL,
)


def _parse_openai_cards_from_text(content: str) -> List[Dict[str, Any]]:
    """
    Пытаемся вытащить карточки из "кривого" JSON-текста.
    Ищем отдельные объекты с полями id/title/summary/topic/tag/importance.
    Если ничего не нашли — возвращаем пустой список.
    """
    if not content:
        return []

    cards: List[Dict[str, Any]] = []

    def _extract_str(block: str, field: str) -> Optional[str]:
        # "field": "значение"
        m = re.search(rf'"{field}"\s*:\s*"([^"]*)"', block)
        if m:
            return m.group(1).strip() or None
        return None

    def _extract_float(block: str, field: str, default: float = 1.0) -> float:
        # "field": 0.87
        m = re.search(rf'"{field}"\s*:\s*([0-9]+(\.[0-9]+)?)', block)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return default
        return default

    for idx, m in enumerate(CARD_OBJECT_RE.finditer(content), start=1):
        block = m.group(0)

        card_id = m.group("id") or f"item_{idx}"

        title = _extract_str(block, "title") or "Новость для тебя"
        summary = _extract_str(block, "summary") or ""
        topic = _extract_str(block, "topic")
        tag = _extract_str(block, "tag")
        importance = _extract_float(block, "importance", 1.0)

        # если вообще нет содержания — пропускаем
        if not title and not summary:
            continue

        cards.append(
            {
                "id": card_id,
                "title": title,
                "summary": summary,
                "topic": topic,
                "tag": tag,
                "importance": importance,
            }
        )

    return cards


def _generate_cards_for_tags_via_openai_sync(
    tags: List[str],
    language: str,
    count: int,
) -> List[Dict[str, Any]]:
    """
    Синхронная генерация новых карточек через OpenAI в формате JSON.
    Вызывается в отдельном потоке.
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip OpenAI card generation")
        return []

    if not tags:
        tags = DEFAULT_FEED_TAGS

    system_prompt = (
        "Ты – движок новостной ленты EYYE.\n"
        "Твоя задача – сгенерировать короткие новостные карточки в одном стиле.\n"
        "Каждая карточка: заголовок и 2–4 абзаца текста.\n"
        "Пиши на языке, указанном в параметрах (ru или en).\n"
        "Отвечай строго валидным JSON без лишнего текста."
    )

    user_payload = {
        "language": language,
        "count": count,
        "tags": tags,
        "requirements": [
            "Карточки должны быть интересными и понятными.",
            "Не выдумывай факты про конкретных людей, лучше обобщай тенденции.",
            "Избегай кликбейта, но делай заголовки цепляющими.",
        ],
        "output_format": {
            "cards": [
                {
                    "title": "string",
                    "body": "string",
                    "tags": ["string"],
                    "category": "string",
                    "importance_score": 1.0,
                }
            ]
        },
    }

    payload: Dict[str, Any] = {
        "model": OPENAI_MODEL or "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        "max_output_tokens": 1200,
        "temperature": 0.7,
        "response_format": {"type": "json_object"},
    }

    started = time.monotonic()
    resp_json = call_openai_chat(payload)
    elapsed = time.monotonic() - started
    logger.info("OpenAI card generation call finished in %.2fs", elapsed)

    if not resp_json:
        return []

    try:
        choices = resp_json.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("No choices in OpenAI response")

        message = choices[0].get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Empty content in OpenAI card generation response")

           # ...
    message = choices[0].get("message") or {}
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise ValueError("Empty content in OpenAI cards response")

    logger.debug(
        "OpenAI cards raw content (first 200 chars): %s",
        content[:200].replace("\n", " "),
    )

    # Пытаемся сначала строгий JSON
    try:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("Parsed card JSON is not an object")

        items = parsed.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("No 'items' list in card JSON")

    except json.JSONDecodeError:
        # Кривой JSON — пробуем вытащить карточки вручную
        logger.exception(
            "Failed to parse OpenAI card generation response as JSON. "
            "Trying to salvage items from raw text."
        )
        items = _parse_openai_cards_from_text(content)
        if not items:
            logger.error("Salvage parser did not find any valid card items.")
            return []
        else:
            logger.warning(
                "Salvage parser recovered %d card items from broken JSON.",
                len(items),
            )
    # дальше оставляем всё как было: нормализуем items и т.д.

        result: List[Dict[str, Any]] = []
        for c in raw_cards:
            if not isinstance(c, dict):
                continue
            title = str(c.get("title", "")).strip()
            body = str(c.get("body", "")).strip()
            if not title or not body:
                continue

            card_tags = c.get("tags") or tags
            if not isinstance(card_tags, list):
                card_tags = tags

            category = c.get("category") or None
            try:
                importance = float(c.get("importance_score", 1.0))
            except (TypeError, ValueError):
                importance = 1.0

            result.append(
                {
                    "source_type": "llm",
                    "source_ref": None,
                    "title": title,
                    "body": body,
                    "tags": [str(t).strip() for t in card_tags if t],
                    "category": category,
                    "language": language,
                    "importance_score": importance,
                    "meta": {
                        "generated_for_tags": tags,
                    },
                }
            )

        return result
    except Exception:
        logger.exception("Failed to parse OpenAI card generation response")
        return []


def _insert_cards_into_db(cards: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Вставка карточек в таблицу cards. Возвращаем то, что вернул Supabase.
    """
    if not cards:
        return []
    if not supabase:
        logger.warning("Supabase is not configured, skip inserting cards into DB")
        return []

    try:
        resp = supabase.table("cards").insert(cards).execute()
    except Exception:
        logger.exception("Error inserting cards into DB")
        return []

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    data = data or []
    logger.info("Inserted %d cards into DB", len(data))
    return data


def _get_or_generate_personalized_cards_sync(
    user_id: int,
    profile_dict: Dict[str, Any],
    allow_openai_generation: bool,
    language: str = "ru",
) -> List[Dict[str, Any]]:
    """
    Синхронная логика:
    1) Берём теги интересов пользователя.
    2) Берём динамические веса.
    3) Берём кандидатов из cards.
    4) Если карточек мало и не заблокирован rate-limit — генерируем новые и кладём в БД.
    5) Считаем скор и возвращаем TOP-N.
    """
    if not supabase:
        logger.warning("Supabase is not configured, cannot build personalized cards")
        return []

    base_tags = _extract_interest_tags_from_profile(profile_dict)
    if not base_tags:
        base_tags = DEFAULT_FEED_TAGS

    topic_weights = get_user_topic_weights(user_id)

    candidates = fetch_candidate_cards(base_tags, limit=FEED_CARDS_LIMIT * 3)

    if allow_openai_generation and len(candidates) < FEED_CARDS_LIMIT:
        need = max(FEED_CARDS_LIMIT * 2 - len(candidates), FEED_CARDS_LIMIT)
        logger.info(
            "Not enough cards in DB for user_id=%s (have %d). Generating ~%d new cards via OpenAI.",
            user_id,
            len(candidates),
            need,
        )
        new_cards = _generate_cards_for_tags_via_openai_sync(base_tags, language, need)
        inserted = _insert_cards_into_db(new_cards)
        candidates.extend(inserted)

    if not candidates:
        return []

    ranked = _score_cards_for_user(candidates, base_tags, topic_weights)
    return ranked[:FEED_CARDS_LIMIT]


async def _send_personalized_feed_from_profile(
    chat_id: int,
    user_id: int,
    profile_dict: Dict[str, Any],
    context: ContextTypes.DEFAULT_TYPE,
    reason: str = "default",
) -> None:
    """
    Отправка ленты карточек:
    - берём/генерируем персональные карточки через таблицу cards;
    - отправляем каждую отдельным сообщением.
    """
    logger.info(
        "Sending personalized feed (cards) for user_id=%s (reason=%s)",
        user_id,
        reason,
    )

    allow_openai = _is_allowed_feed_openai_call(user_id)

    cards = await asyncio.to_thread(
        _get_or_generate_personalized_cards_sync,
        user_id,
        profile_dict,
        allow_openai,
        "ru",
    )

    if not cards:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "Пока не смог собрать для тебя ленту. "
                    "Попробуй ещё раз чуть позже — я уже готовлю контент."
                ),
            )
        except Exception:
            logger.exception("Failed to send 'no cards' message to user_id=%s", user_id)
        return

    for card in cards:
        title = (card.get("title") or "").strip()
        body = (card.get("body") or "").strip()

        parts: List[str] = []
        if title:
            parts.append(f"📰 <b>{_truncate(title, 200)}</b>")
        if body:
            parts.append("")
            parts.append(_truncate(body, 2000))

        text = "\n".join(parts).strip()
        if not text:
            continue

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
            )
        except Exception:
            logger.exception(
                "Failed to send card id=%s to user_id=%s", card.get("id"), user_id
            )


async def _load_effective_profile(
    user_id: int,
) -> Tuple[Optional[Dict[str, Any]], bool, Optional[str]]:
    """
    Загружаем эффективный профиль пользователя:
    - если есть structured_profile — используем его;
    - иначе строим fallback из raw_interests.
    Возвращаем (profile_dict, using_fallback, raw_interests_or_none).
    """
    if not supabase:
        logger.warning("_load_effective_profile: Supabase is not configured")
        return None, False, None

    try:
        resp = (
            supabase.table("user_profiles")
            .select("structured_profile, raw_interests")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("_load_effective_profile: failed to query Supabase for user_id=%s", user_id)
        return None, False, None

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return None, False, None

    row = data[0]
    structured = row.get("structured_profile")
    raw_interests = row.get("raw_interests") or ""

    if structured is not None:
        if isinstance(structured, str):
            try:
                structured_obj = json.loads(structured)
            except Exception:
                logger.exception(
                    "_load_effective_profile: failed to parse structured_profile JSON for user_id=%s",
                    user_id,
                )
                structured_obj = None
        else:
            structured_obj = structured

        if not isinstance(structured_obj, dict):
            logger.warning(
                "_load_effective_profile: structured_profile has unexpected type for user_id=%s",
                user_id,
            )
            profile_dict = None
        else:
            profile_dict = _normalize_profile_dict(structured_obj)
            return profile_dict, False, raw_interests

    # сюда попадаем, если structured_profile нет или он странный
    if not raw_interests:
        return None, True, None

    fallback_profile = _normalize_profile_dict(_build_fallback_profile_from_raw(raw_interests))

    # параллельно пробуем построить нормальный профиль, если есть OpenAI
    if OPENAI_API_KEY:
        try:
            app = cast(Application, Application._get_instance())
            app.create_task(
                asyncio.to_thread(build_and_save_structured_profile, user_id, raw_interests)
            )
            logger.info(
                "_load_effective_profile: scheduled build_and_save_structured_profile for user_id=%s",
                user_id,
            )
        except Exception:
            logger.exception(
                "_load_effective_profile: failed to schedule build_and_save_structured_profile for user_id=%s",
                user_id,
            )

    return fallback_profile, True, raw_interests


# ==========================
# Команды
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start — онбординг или мгновенная выдача ленты, если профиль уже есть.
    """
    user = update.effective_user

    if user:
        await save_user_to_supabase(user.id, user.username)

    if not update.message:
        return

    if not supabase or not user:
        # Режим без базы — просто справка
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

    profile = await load_user_profile(user.id)

    if profile:
        # Профиль уже есть — сразу показываем ленту
        context.user_data["awaiting_profile"] = False
        context.user_data["profile_buffer"] = []
        context.user_data["selected_topics"] = []
        context.user_data["topics_mode"] = None
        context.user_data["topics_keyboard_message_id"] = None
        context.user_data["topics_keyboard_chat_id"] = None

        await update.message.reply_text(
            "Снова привет 👋\n\n"
            "Я уже помню твои интересы. Обновляю под тебя ленту прямо сейчас.",
            reply_markup=ReplyKeyboardRemove(),
        )

        effective_profile, using_fallback, _ = await _load_effective_profile(user.id)
        if not effective_profile:
            await update.message.reply_text(
                "Пока не смог собрать твой профиль интересов. "
                "Попробуй обновить его через /start чуть позже."
            )
            return

        await _send_personalized_feed_from_profile(
            chat_id=update.effective_chat.id,
            user_id=user.id,
            profile_dict=effective_profile,
            context=context,
            reason="start_existing_profile",
        )
        if using_fallback:
            await update.message.reply_text(
                "Пока использую черновой профиль, в фоне строю более точный вариант с помощью ИИ."
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
        "Когда всё опишешь — просто отправь команду /done или нажми «Начать читать».",
        "",
        "— Жду твоё первое сообщение 🙂",
    ]
    await update.message.reply_text(
        "\n".join(text_lines),
        reply_markup=build_choose_topics_entry_keyboard(),
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /help — простая справка.
    """
    text_lines = [
        "Доступные команды:",
        "/start — перезапустить бота и (при необходимости) пройти онбординг",
        "/ping — проверить, что бот жив",
        "/me — показать, что бот знает о тебе",
        "/feed — черновой вывод тем/тегов профиля (для отладки)",
        "/raw_profile — показать сохранённые raw_interests и structured_profile (обрезано)",
        "/done — закончить описание интересов во время онбординга",
        "/help — эта справка",
        "/reset_profile — удалить профиль и пройти онбординг заново (для тестов)",
    ]
    if update.message:
        await update.message.reply_text("\n".join(text_lines))


async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /ping — проверка, что бот жив.
    """
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
        profile_lines.append(_truncate(raw, 800))
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
                profile_lines.append("structured_profile (кратко):")
                sp_city = structured_data.get("location_city") or "—"
                sp_country = structured_data.get("location_country") or "—"
                profile_lines.append(f"- city: {sp_city}")
                profile_lines.append(f"- country: {sp_country}")

                topics = structured_data.get("topics") or []
                if topics:
                    profile_lines.append("- topics:")
                    for topic in topics[:10]:
                        if not isinstance(topic, dict):
                            continue
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
                    for nt in negative[:10]:
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


async def raw_profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /raw_profile — показать сырые данные профиля (raw_interests + structured_profile JSON, обрезанные).
    Удобно для отладки.
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
            "Профиль не найден. Пройди онбординг через /start, чтобы я запомнил твои интересы."
        )
        return

    raw = profile.get("raw_interests") or ""
    structured = profile.get("structured_profile")

    lines: List[str] = []
    lines.append("raw_interests (обрезано):")
    lines.append(_truncate(raw, 1200))
    lines.append("")

    if structured is None:
        lines.append("structured_profile: ещё не посчитан или пуст.")
    else:
        if isinstance(structured, str):
            structured_str = structured
        else:
            try:
                structured_str = json.dumps(structured, ensure_ascii=False, indent=2)
            except Exception:
                structured_str = str(structured)
        lines.append("structured_profile (обрезано):")
        lines.append(_truncate(structured_str, 1800))

    await update.message.reply_text("\n".join(lines))


async def reset_profile_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /reset_profile — удалить профиль и начать онбординг заново.
    Пока без ограничения по ролям (удобно для разработки).
    """
    user = update.effective_user
    if not user or not update.message:
        return

    if not supabase:
        await update.message.reply_text("Supabase не настроен, сброс профиля невозможен.")
        return

    ok = await delete_user_profile(user.id)
    if ok:
        # Сброс локального состояния онбординга
        context.user_data.clear()
        await update.message.reply_text(
            "Я удалил твой профиль интересов. "
            "Чтобы настроить всё заново, отправь /start.",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await update.message.reply_text(
            "Не получилось удалить профиль. Попробуй чуть позже."
        )


async def feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /feed — отладочная команда: читаем structured_profile и показываем список тем/тегов.
    Это НЕ основная точка входа в ленту.
    """
    user = update.effective_user
    message = update.effective_message

    if user is None or message is None:
        return

    if supabase is None:
        await message.reply_text("Внутренняя ошибка: база профилей не настроена.")
        return

    profile_dict, using_fallback, _ = await _load_effective_profile(user.id)
    if not profile_dict:
        await message.reply_text(
            "Я пока не знаю твоих интересов. Пройди, пожалуйста, онбординг через /start."
        )
        return

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
    topic_names = topic_names[:12]

    if topic_names:
        lines.append("Основные темы, по которым я ориентируюсь:")
        lines.append(", ".join(topic_names) + ".")
        lines.append("")

    if tags:
        tags_str = ", ".join(str(x) for x in tags[:15])
        lines.append("Теги интересов:")
        lines.append(tags_str + ".")
        lines.append("")

    if negative_topics:
        neg_str = ", ".join(str(x) for x in negative_topics[:10])
        lines.append("Темы, которых стоит избегать:")
        lines.append(neg_str + ".")
        lines.append("")

    if using_fallback:
        lines.append(
            "Сейчас использую черновой профиль по твоим выборам. "
            "В фоне строю более точный профиль с помощью ИИ."
        )

    if not lines:
        lines.append(
            "У меня пока нет достаточно структурированных данных о твоих интересах. "
            "Как только профиль обновится, я смогу подбирать под тебя новости."
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
            "Сейчас я уже не собираю профиль. Напиши /start, чтобы обновить свои интересы."
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
    После этого сразу показываем первую версию ленты (на основе fallback-профиля).
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

    # Сбрасываем локальные флаги онбординга
    context.user_data["awaiting_profile"] = False
    context.user_data["profile_buffer"] = []
    context.user_data["selected_topics"] = []
    context.user_data["topics_mode"] = None
    context.user_data["topics_keyboard_message_id"] = None
    context.user_data["topics_keyboard_chat_id"] = None

    await update.message.reply_text(
        "Отлично, я запомнил твои интересы и выбранные темы 🙌\n\n"
        "Собираю для тебя первую версию ленты.",
        reply_markup=ReplyKeyboardRemove(),
    )

    # В фоне строим полноценный structured_profile (если есть Supabase + OpenAI)
    if supabase and OPENAI_API_KEY:
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

    # Для мгновенной отдачи ленты используем локальный fallback-профиль.
    fallback_profile = _normalize_profile_dict(_build_fallback_profile_from_raw(raw_interests))

    await _send_personalized_feed_from_profile(
        chat_id=update.effective_chat.id,
        user_id=user.id,
        profile_dict=fallback_profile,
        context=context,
        reason="finish_onboarding",
    )

    if OPENAI_API_KEY:
        await update.message.reply_text(
            "Пока это черновая версия ленты по твоим выборам. "
            "В фоне я донастрою профиль с помощью ИИ и следующие подборки будут точнее."
        )


# ==========================
# Глобальный обработчик ошибок
# ==========================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Логируем любые неотловленные ошибки и пытаемся аккуратно сообщить пользователю.
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
    """
    Регистрируем все хендлеры и собираем Application.
    """
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping))
    application.add_handler(CommandHandler("me", me))
    application.add_handler(CommandHandler("feed", feed))
    application.add_handler(CommandHandler("raw_profile", raw_profile_command))
    application.add_handler(CommandHandler("reset_profile", reset_profile_command))
    application.add_handler(CommandHandler("done", finish_onboarding))

    # Любой текст во время онбординга
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            onboarding_message,
        )
    )

    application.add_error_handler(error_handler)

    return application


def main() -> None:
    """
    Точка входа — запускаем polling.
    """
    app = build_application()
    app.run_polling()


if __name__ == "__main__":
    main()

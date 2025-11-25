# file: src/bot.py
import logging
import os
import asyncio
import json
import urllib.request
import urllib.error
from typing import Optional, Any, Dict, List
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

BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# модель берём из окружения, по умолчанию gpt-5-mini
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")

# базовый URL для OpenAI (сейчас используем конкретный endpoint /v1/responses)
OPENAI_API_BASE = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

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
        # логируем, но не падаем
        logger.error("Failed to update topics keyboard: %s", e)


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


async def upsert_user_profile_structured(
    telegram_id: int,
    structured_profile: Dict[str, Any],
    raw_interests: Optional[str] = None,
) -> bool:
    """
    Создаём или обновляем structured_profile в user_profiles.
    Заодно при наличии обновляем location_city/location_country и, при желании, raw_interests.
    (Сейчас не используется напрямую, но оставляем на будущее.)
    """
    if not supabase:
        logger.warning("Supabase client is not configured, skip upsert_user_profile_structured")
        return False

    data: Dict[str, Any] = {
        "user_id": telegram_id,
        "structured_profile": structured_profile,
    }

    # Если модель выделила локацию — синхронизируем
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
# OpenAI: построение structured_profile
# ==========================
# ==========================
# OpenAI: построение structured_profile
# ==========================

# JSON Schema для профиля пользователя EYYE.
# Эта схема используется в Responses API (text.format.type = "json_schema"),
# чтобы модель генерировала строго структурированный объект.
PROFILE_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "location_city": {"type": ["string", "null"]},
        "location_country": {"type": ["string", "null"]},
        "topics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "weight": {"type": "number"},
                    "category": {"type": ["string", "null"]},
                    "detail": {"type": ["string", "null"]},
                },
                "required": ["name", "weight"],
                "additionalProperties": False,
            },
        },
        "negative_topics": {
            "type": "array",
            "items": {"type": "string"},
        },
        "interests_as_tags": {
            "type": "array",
            "items": {"type": "string"},
        },
        "user_meta": {
            "type": "object",
            "properties": {
                "age_group": {"type": ["string", "null"]},
                "student_status": {"type": ["string", "null"]},
            },
            "required": ["age_group", "student_status"],
            "additionalProperties": False,
        },
    },
    "required": [
        "location_city",
        "location_country",
        "topics",
        "negative_topics",
        "interests_as_tags",
        "user_meta",
    ],
    "additionalProperties": False,
}


def _extract_array_for_key_from_partial_json(content: str, key: str) -> Optional[Any]:
    """
    Пытаемся вытащить JSON-массив для ключа "key" из частично сломанного JSON-текста.
    Пример: ... "topics": [ { ... }, { ... } ], ...
    Алгоритм:
    - находим `"key"`
    - ищем первую '[' после него
    - считаем вложенные '[' / ']'
    - когда счётчик вернулся к 0 — вырезаем подстроку и пытаемся json.loads(...)
    """
    try:
        key_pos = content.find(f'"{key}"')
        if key_pos == -1:
            return None

        bracket_start = content.find("[", key_pos)
        if bracket_start == -1:
            return None

        depth = 0
        end_idx = None
        for i, ch in enumerate(content[bracket_start:], start=bracket_start):
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break

        if end_idx is None:
            return None

        array_str = content[bracket_start : end_idx + 1]
        return json.loads(array_str)
    except Exception:
        return None


def _build_fallback_profile_from_raw(raw_interests: str) -> Dict[str, Any]:
    """
    Очень простой fallback-профиль на случай, если OpenAI дважды вернул мусор.
    Строим темы по тем строкам raw_interests, которые совпадают с MAIN_TOPICS / SPORT_SUBTOPICS.
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


def _salvage_profile_from_broken_content(
    raw_interests: str,
    broken_content: Optional[str],
) -> Dict[str, Any]:
    """
    Третий уровень защиты:
    - пробуем вытащить topics / negative_topics / interests_as_tags из битого JSON OpenAI;
    - если не получилось вообще ничего — строим fallback из raw_interests.
    """
    topics: List[Any] = []
    negative: List[Any] = []
    tags: List[Any] = []

    if broken_content:
        topics_candidate = _extract_array_for_key_from_partial_json(broken_content, "topics")
        if isinstance(topics_candidate, list):
            topics = topics_candidate

        neg_candidate = _extract_array_for_key_from_partial_json(broken_content, "negative_topics")
        if isinstance(neg_candidate, list):
            negative = neg_candidate

        tags_candidate = _extract_array_for_key_from_partial_json(broken_content, "interests_as_tags")
        if isinstance(tags_candidate, list):
            tags = tags_candidate

    if not topics and not negative and not tags:
        logger.warning("Could not salvage anything from broken_content, using raw_interests fallback")
        return _build_fallback_profile_from_raw(raw_interests)

    logger.warning(
        "Salvaged structured_profile from broken_content: topics=%d, negative_topics=%d, tags=%d",
        len(topics),
        len(negative),
        len(tags),
    )

    return {
        "location_city": None,
        "location_country": None,
        "topics": topics,
        "negative_topics": negative,
        "interests_as_tags": tags,
        "user_meta": {
            "age_group": None,
            "student_status": None,
        },
    }


def _normalize_profile_dict(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Унифицированная нормализация профиля:
    - дефолты полей,
    - нормализация списка topics / negative_topics / interests_as_tags / user_meta.
    """
    profile = dict(profile)  # на всякий случай копия

    profile.setdefault("location_city", None)
    profile.setdefault("location_country", None)
    profile.setdefault("topics", [])
    profile.setdefault("negative_topics", [])
    profile.setdefault("interests_as_tags", [])
    profile.setdefault("user_meta", {})

    # topics
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

    # negative_topics
    neg = profile.get("negative_topics")
    if not isinstance(neg, list):
        neg = []
    profile["negative_topics"] = [str(x).strip() for x in neg if str(x).strip()]

    # interests_as_tags
    tags = profile.get("interests_as_tags")
    if not isinstance(tags, list):
        tags = []
    profile["interests_as_tags"] = [str(x).strip() for x in tags if str(x).strip()]

    # user_meta
    user_meta = profile.get("user_meta")
    if not isinstance(user_meta, dict):
        user_meta = {}
    profile["user_meta"] = user_meta

    return profile


def _extract_parsed_profile_from_response(resp_json: Dict[str, Any]) -> (Optional[Dict[str, Any]], Optional[str]):
    """
    Достаём из ответа Responses API:
    - parsed (dict) — если модель вернула structured output,
    - broken_text (str) — если есть только текстовый ответ (похожий на JSON, но может быть битым).
    """
    parsed: Optional[Dict[str, Any]] = None
    broken_text: Optional[str] = None

    try:
        output = resp_json.get("output")
        if isinstance(output, list):
            for item in output:
                if not isinstance(item, dict):
                    continue
                if item.get("type") != "message":
                    continue
                content_list = item.get("content")
                if not isinstance(content_list, list):
                    continue
                for block in content_list:
                    if not isinstance(block, dict):
                        continue

                    # 1) structured JSON — parsed/json в блоке
                    parsed_candidate = block.get("parsed") or block.get("json")
                    if isinstance(parsed_candidate, dict):
                        parsed = parsed_candidate
                        break

                    # 2) текстовый блок — потенциально битый JSON
                    block_type = block.get("type")
                    if block_type in ("output_text", "input_text", "text"):
                        text_val = block.get("text")
                        if isinstance(text_val, str) and broken_text is None:
                            broken_text = text_val

                if parsed is not None:
                    break

        # запасной вариант — если structured нет, а текст лежит наверху
        if parsed is None and broken_text is None:
            top_text = resp_json.get("output_text")
            if isinstance(top_text, str):
                broken_text = top_text

    except Exception:
        logger.exception("Failed to extract structured output from OpenAI response")

    return parsed, broken_text


def _request_profile_with_schema(raw_interests: str) -> (Optional[Dict[str, Any]], Optional[str]):
    """
    Первый (основной) запрос к OpenAI:
    - используем json_schema + strict=true;
    - пытаемся получить parsed из structured output;
    - если не получается — возвращаем (None, broken_text), где broken_text — текстовый ответ.
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skipping structured_profile build")
        return None, None

    model = OPENAI_MODEL or "gpt-5-mini"

    system_prompt = """
Ты помогаешь новостному рекомендательному сервису EYYE.
По свободному описанию интересов и города пользователя ты должен вернуть
структурированный профиль с полями:

- location_city / location_country — город и страна (если понятно, иначе null).
- topics — список объектов { name, weight, category, detail }:
  - name — короткое название темы ("стартапы", "премьер-лига", "аниме").
  - weight — важность от 0.0 до 1.0.
  - category — общий род ("business", "sports", "culture", "tech", "education" и т.п.) или null.
  - detail — 1–2 слова уточнения ("UK football", "US startups") или null.
- negative_topics — массив строк с темами, которые пользователь не хочет видеть.
- interests_as_tags — нормализованные теги латиницей ("startups", "premier_league", "uk_universities").
- user_meta:
  - age_group — примерный возраст ("18-24", "25-34" и т.п.) или null.
  - student_status — "school_student", "university_student", "postgraduate_student", "not_student" или null.

Старайся заполнять как можно аккуратнее, но если информации мало — используй null и пустые массивы.
"""

    payload: Dict[str, Any] = {
        "model": model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": raw_interests},
        ],
        "max_output_tokens": 800,
        "text": {
            "format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "eyye_user_profile",
                    "strict": True,
                    "schema": PROFILE_JSON_SCHEMA,
                },
            }
        },
    }

    url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/responses")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = "<no body>"
        logger.error("OpenAI HTTPError (primary): %s | body=%s", e, error_body[:2000])
        return None, None
    except Exception:
        logger.exception("Error calling OpenAI (primary)")
        return None, None

    try:
        resp_json = json.loads(body.decode("utf-8"))
    except Exception:
        logger.exception("Failed to parse OpenAI response JSON (primary): %r", body[:1000])
        return None, None

    return _extract_parsed_profile_from_response(resp_json)


def _request_profile_retry_with_schema(
    raw_interests: str,
    broken_content: str,
) -> (Optional[Dict[str, Any]], Optional[str]):
    """
    Второй (единственный ретрай) запрос:
    - объясняем, что прошлый JSON был битым;
    - даём исходные интересы и прошлый ответ;
    - снова просим корректный JSON по той же схеме.
    """
    if not OPENAI_API_KEY:
        return None, None

    model = OPENAI_MODEL or "gpt-5-mini"

    system_prompt = """
Ты помощник сервиса EYYE. Ранее ты вернул некорректный JSON-профиль пользователя.
Сейчас тебе нужно СНОВА построить профиль по строгой схеме.

Игнорируй все ошибки прошлого ответа и просто верни новый корректный профиль.
"""

    user_prompt = (
        "Вот исходное описание интересов пользователя:\n\n"
        f"{raw_interests}\n\n"
        "Вот твой предыдущий ответ (битый JSON, который нужно игнорировать):\n\n"
        f"{broken_content}\n\n"
        "Построй, пожалуйста, НОВЫЙ корректный профиль по согласованной схеме."
    )

    payload: Dict[str, Any] = {
        "model": model,
        "input": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": 800,
        "text": {
            "format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "eyye_user_profile_retry",
                    "strict": True,
                    "schema": PROFILE_JSON_SCHEMA,
                },
            }
        },
    }

    url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/responses")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    data_bytes = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
    except urllib.error.HTTPError as e:
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = "<no body>"
        logger.error("OpenAI HTTPError (retry): %s | body=%s", e, error_body[:2000])
        return None, None
    except Exception:
        logger.exception("Error calling OpenAI (retry)")
        return None, None

    try:
        resp_json = json.loads(body.decode("utf-8"))
    except Exception:
        logger.exception("Failed to parse OpenAI response JSON (retry): %r", body[:1000])
        return None, None

    return _extract_parsed_profile_from_response(resp_json)


def _call_openai_structured_profile_sync(raw_interests: str) -> Optional[Dict[str, Any]]:
    """
    Главная функция построения structured_profile через OpenAI.

    Уровни надёжности:
    1) Основной запрос с json_schema + strict → пытаемся получить parsed.
    2) Если не получилось, но есть текстовый ответ → один ретрай с "repair"-промптом.
    3) Если и после ретрая нет parsed, но есть хотя бы какой-то текст —
       вытаскиваем максимум из битого ответа или строим fallback из raw_interests.
    4) Только если вообще нечего спасать — возвращаем None.
    """
    # 1️⃣ Первый запрос
    parsed, broken_content = _request_profile_with_schema(raw_interests)

    if isinstance(parsed, dict):
        return _normalize_profile_dict(parsed)

    # 2️⃣ Ретрай, если есть текстовый ответ
    if broken_content:
        logger.warning("Structured profile not found in primary response, retrying with repair prompt")
        parsed_retry, broken_retry = _request_profile_retry_with_schema(raw_interests, broken_content)
        if isinstance(parsed_retry, dict):
            return _normalize_profile_dict(parsed_retry)

        # 3️⃣ Спасаем максимум из битого ответа (retry или первичного)
        salvage_source = broken_retry or broken_content
        if salvage_source:
            logger.warning("Retry did not return structured profile, salvaging from broken content")
            salvaged = _salvage_profile_from_broken_content(raw_interests, salvage_source)
            return _normalize_profile_dict(salvaged)

    # 4️⃣ Вообще нечего спасать: нет parsed и нет текста
    logger.warning("OpenAI did not return any usable content for structured_profile")
    return None


def build_and_save_structured_profile(user_id: int, raw_interests: str) -> None:
    """
    Строит structured_profile через OpenAI и сохраняет в Supabase.

    ВАЖНО:
    - raw_interests мы здесь НЕ перезатираем, чтобы не ловить NOT NULL ошибки.
    - Обновляем только location_* и structured_profile.
    """
    text_len = len(raw_interests or "")
    logger.info(
        "build_and_save_structured_profile: start for user_id=%s, raw_interests_len=%s",
        user_id,
        text_len,
    )

    profile = _call_openai_structured_profile_sync(raw_interests)
    if not profile:
        logger.warning(
            "build_and_save_structured_profile: OpenAI returned empty structured_profile for user_id=%s",
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
        logger.info(
            "Update structured_profile for user_id=%s: data=%s count=%s",
            user_id,
            getattr(resp, "data", None),
            getattr(resp, "count", None),
        )

        data_list = getattr(resp, "data", None)
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

    if not update.message:
        return

    # Если Supabase не настроен — ведёмся как раньше, без онбординга по профилю
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

    # Проверяем, есть ли уже профиль интересов
    profile = await load_user_profile(user.id)

    if profile:
        # Профиль уже есть — приветствуем и даём подсказки
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

    # Профиля ещё нет — запускаем онбординг по свободному тексту + кнопкам тем
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
    """
    /help — список команд.
    """
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
    - профиль интересов из user_profiles (если есть, включая structured_profile).
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

        # structured_profile (jsonb)
        structured = profile.get("structured_profile")
        if structured is None:
            profile_lines.append("structured_profile: ещё не посчитан или пуст.")
        else:
            # Supabase может вернуть dict или строку
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
    Черновая команда /feed:
    - читает structured_profile из Supabase,
    - выводит пользователю, по каким темам мы будем искать новости.
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
            .select("structured_profile")
            .eq("user_id", user.id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load structured_profile from Supabase for user_id=%s", user.id)
        await message.reply_text("Не получилось получить ваш профиль интересов. Попробуйте ещё раз позже.")
        return

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        await message.reply_text(
            "Я пока не знаю ваших интересов. Пройди, пожалуйста, онбординг через /start, "
            "а потом попробуй /feed ещё раз."
        )
        return

    row = data[0]
    structured = row.get("structured_profile")

    if structured is None:
        await message.reply_text(
            "Твой профиль ещё строится. Подожди пару секунд и попробуй /feed снова."
        )
        return

    # Supabase может вернуть либо dict, либо JSON-строку
    if isinstance(structured, str):
        try:
            structured = json.loads(structured)
        except Exception:
            logger.exception("Failed to parse structured_profile JSON for user_id=%s", user.id)
            await message.reply_text(
                "Ваш структурированный профиль сейчас в странном формате. "
                "Попробуй пройти онбординг заново позже."
            )
            return

    if not isinstance(structured, dict):
        await message.reply_text(
            "Ваш профиль интересов сейчас в непонятном формате. "
            "Попробуй пройти онбординг заново позже."
        )
        return

    topics = structured.get("topics") or []
    negative_topics = structured.get("negative_topics") or []
    tags = structured.get("interests_as_tags") or []

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

    await message.reply_text("\n".join(lines))


# ==========================
# Онбординг: обработка текста и кнопок тем
# ==========================

async def onboarding_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обрабатываем обычные текстовые сообщения.
    Если мы в состоянии онбординга (awaiting_profile=True) —
    либо обрабатываем выбор тем, либо записываем свободный текст.
    Если нет — просто даём подсказку про /help.
    """
    if not update.message:
        return

    user = update.effective_user
    if not user:
        return

    text_raw = (update.message.text or "").strip()
    if not text_raw:
        return

    # Если сейчас НЕ ждём описание интересов — мягкая подсказка
    if not context.user_data.get("awaiting_profile"):
        await update.message.reply_text(
            "Я пока понимаю только команды. Напиши /help, чтобы увидеть список."
        )
        return

    # Специальные кнопки, которые НЕ зависят от префикса "✅"
    if text_raw == TOPIC_CHOOSE_BUTTON_TEXT:
        # Пользователь вошёл в режим выбора общих тем
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
        # "Начать читать" действует так же, как /done
        await finish_onboarding(update, context)
        return

    if text_raw == EXIT_TOPICS_BUTTON_TEXT:
        # Убираем клавиатуру и выходим из режима выбора тем
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
        # Возврат из подменю спорта к общим темам
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

    # Нормализуем текст (убираем "✅ ")
    text = strip_checkmark(text_raw)

    topics_mode: Optional[str] = context.user_data.get("topics_mode")
    selected_topics: List[str] = context.user_data.get("selected_topics", [])
    keyboard_message_id = context.user_data.get("topics_keyboard_message_id")
    keyboard_chat_id = context.user_data.get("topics_keyboard_chat_id")

    # --- Выбор подтем спорта ---
    if topics_mode == "sports" and text in SPORT_SUBTOPICS:
        selected = set(selected_topics)
        if text in selected:
            selected.remove(text)
        else:
            selected.add(text)
        context.user_data["selected_topics"] = list(selected)

        # Обновляем клавиатуру без новых сообщений
        if keyboard_message_id and keyboard_chat_id:
            await update_topics_keyboard_markup(
                context,
                keyboard_chat_id,
                keyboard_message_id,
                topics_mode,
                context.user_data["selected_topics"],
            )
        return

    # --- Выбор основных тем ---
    if topics_mode == "main":
        # Отдельно обрабатываем "Спорт" — открываем подменю
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

            # Обновляем клавиатуру без текста от бота
            if keyboard_message_id and keyboard_chat_id:
                await update_topics_keyboard_markup(
                    context,
                    keyboard_chat_id,
                    keyboard_message_id,
                    topics_mode,
                    context.user_data["selected_topics"],
                )
            return

    # --- Всё остальное считаем свободным текстом интересов ---
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
    /done — завершение онбординга:
    склеиваем все собранные сообщения и выбранные темы и сохраняем в user_profiles.
    Параллельно (в фоне) строим structured_profile через OpenAI, если доступно.
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

    # Если ни текста, ни выбранных тем — просим что-нибудь выбрать/написать
    if not raw_interests:
        await update.message.reply_text(
            "Похоже, ты ещё ничего не написал и не выбрал 🙈\n"
            "Опиши, пожалуйста, в одном-двух сообщениях свои интересы и город "
            "или выбери что-то из тем, а потом снова отправь /done или нажми «Начать читать»."
        )
        return

    # Сохраняем профиль в Supabase
    ok = await upsert_user_profile(user.id, raw_interests)

    if not ok:
        await update.message.reply_text(
            "Не получилось сохранить профиль. Попробуй, пожалуйста, ещё раз чуть позже."
        )
        return

    # Сбрасываем состояние онбординга и убираем клавиатуру
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

    # В фоне строим structured_profile (если есть Supabase и OPENAI_API_KEY)
    if not supabase:
        logger.warning("Supabase is not configured, skip building structured_profile")
        return
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip building structured_profile")
        return

    application: Application = context.application  # type: ignore[assignment]
    try:
        application.create_task(build_and_save_structured_profile(user.id, raw_interests))
        logger.info(
            "finish_onboarding: scheduled build_and_save_structured_profile for user_id=%s",
            user.id,
        )
    except Exception:
        logger.exception(
            "finish_onboarding: failed to schedule build_and_save_structured_profile for user_id=%s",
            user.id,
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
    application.add_handler(CommandHandler("feed", feed))
    application.add_handler(CommandHandler("done", finish_onboarding))

    # Текстовые сообщения (без команд) — для онбординга и выбора тем
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

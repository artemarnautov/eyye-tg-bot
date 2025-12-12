# file: src/webapp_backend/openai_client.py
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# ==========
# Конфиг OpenAI
# ==========
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_WIKIPEDIA_MODEL = os.getenv("OPENAI_WIKIPEDIA_MODEL", OPENAI_MODEL)

OPENAI_API_BASE = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_CHAT_COMPLETIONS_URL = OPENAI_API_BASE.rstrip("/") + "/chat/completions"

OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))
RAW_LOG_MAX_LEN = 4000

DEFAULT_FEED_TAGS = ["world_news", "business", "tech", "uk_students"]

ALLOWED_TAGS_CANONICAL = [
    "world_news",
    "business",
    "finance",
    "tech",
    "science",
    "history",
    "politics",
    "society",
    "entertainment",
    "gaming",
    "sports",
    "lifestyle",
    "education",
    "city",
    "uk_students",
]
ALLOWED_TAGS_SET = set(ALLOWED_TAGS_CANONICAL)

# лёгкие алиасы (чтобы модельные “crypto/ai/movies” не вылетали в пустоту)
TAG_ALIASES = {
    "crypto": "finance",
    "cryptocurrency": "finance",
    "ai": "tech",
    "startup": "business",
    "movies": "entertainment",
    "movie": "entertainment",
    "cinema": "entertainment",
    "games": "gaming",
    "sport": "sports",
    "education_career": "education",
}


def is_configured() -> bool:
    return bool(OPENAI_API_KEY)


def _clamp01(x: float) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.5
    return max(0.0, min(1.0, v))


def _clean_text(s: Any, max_len: int) -> str:
    text = str(s or "").strip()
    if not text:
        return ""
    # убираем лишние пробелы
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_len].strip()


def _normalize_tag_list(tags: Any, fallback: List[str] | None = None) -> List[str]:
    """
    1) приводим к list[str]
    2) lower + алиасы
    3) фильтр по allowlist
    4) дедуп
    """
    fallback = fallback or []
    if not tags:
        tags_list: List[str] = []
    elif isinstance(tags, str):
        tags_list = [tags]
    elif isinstance(tags, (list, tuple)):
        tags_list = [str(t) for t in tags]
    else:
        tags_list = []

    out: List[str] = []
    for t in tags_list:
        v = str(t or "").strip().lower()
        if not v:
            continue
        v = TAG_ALIASES.get(v, v)
        if v in ALLOWED_TAGS_SET:
            out.append(v)

    # дедуп
    seen = set()
    deduped: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            deduped.append(t)

    if not deduped:
        # fallback тоже фильтруем по allowlist
        fb = []
        for t in fallback:
            v = str(t or "").strip().lower()
            v = TAG_ALIASES.get(v, v)
            if v in ALLOWED_TAGS_SET:
                fb.append(v)
        # дедуп fallback
        seen2 = set()
        deduped2 = []
        for t in fb:
            if t not in seen2:
                seen2.add(t)
                deduped2.append(t)
        return deduped2

    return deduped


def call_openai_chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skipping OpenAI call")
        return {}

    url = OPENAI_CHAT_COMPLETIONS_URL
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    model = payload.get("model") or OPENAI_MODEL or "gpt-4.1-mini"

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

    temperature = payload.get("temperature", 0.4)
    try:
        temperature_float = float(temperature)
    except (TypeError, ValueError):
        temperature_float = 0.4

    body: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens_int,
        "temperature": temperature_float,
    }

    if "response_format" in payload:
        body["response_format"] = payload["response_format"]

    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    started_at = datetime.now(timezone.utc)
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("OpenAI chat.completions call OK (%.2fs)", elapsed)
        logger.debug(
            "OpenAI raw response (first %d chars): %s",
            RAW_LOG_MAX_LEN,
            raw[:RAW_LOG_MAX_LEN],
        )
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


def _extract_message_content(resp_json: Dict[str, Any]) -> str:
    if not resp_json:
        return ""

    choices = resp_json.get("choices")
    if not isinstance(choices, list) or not choices:
        logger.error("No choices in OpenAI response")
        return ""

    message = choices[0].get("message") or {}
    content = message.get("content")

    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, dict):
                    value = text.get("value")
                    if isinstance(value, str):
                        parts.append(value)
                elif isinstance(text, str):
                    parts.append(text)
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join(parts)

    if isinstance(content, str):
        return content

    return str(content or "")


def _try_loose_json_parse(content: str) -> Dict[str, Any] | None:
    if not content:
        return None

    text = content.strip()
    first = text.find("{")
    last = text.rfind("}")
    if first == -1 or last == -1 or last <= first:
        return None

    candidate = text[first : last + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


# ==========
# Генерация карточек “с нуля”
# ==========

def generate_cards_for_tags(tags: List[str], language: str, count: int) -> List[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip OpenAI card generation")
        return []

    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    # фильтруем входные теги
    tags = _normalize_tag_list(tags, fallback=DEFAULT_FEED_TAGS)
    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    system_prompt = (
        "Ты – движок новостной ленты EYYE.\n"
        "Сгенерируй короткие новостные карточки.\n"
        "Каждая карточка: заголовок + 2–4 абзаца текста.\n"
        "Пиши на языке параметра (ru или en).\n"
        "Отвечай строго валидным JSON без лишнего текста.\n\n"
        "Теги можно использовать ТОЛЬКО из этого списка:\n"
        + ", ".join(ALLOWED_TAGS_CANONICAL)
        + "\n\n"
        "JSON-структура:\n"
        "{\n"
        '  \"cards\": [\n'
        "    {\n"
        '      \"title\": \"...\",\n'
        '      \"body\": \"...\",\n'
        '      \"tags\": [\"world_news\"],\n'
        '      \"importance_score\": 0.7,\n'
        '      \"language\": \"ru\"\n'
        "    }\n"
        "  ]\n"
        "}\n"
    )

    user_payload = {
        "language": language,
        "count": count,
        "tags": tags,
        "requirements": [
            "Карточки должны быть интересными и понятными.",
            "Не выдумывай точные факты про конкретных реальных людей.",
            "Избегай кликбейта, но делай заголовки цепляющими.",
            "НЕ делай одинаковые заголовки у разных карточек.",
        ],
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

    content_str = _extract_message_content(resp_json).strip()
    if not content_str:
        logger.error("Empty content in OpenAI cards response")
        return []

    raw_cards: List[Dict[str, Any]] = []
    try:
        parsed = json.loads(content_str)
        if isinstance(parsed, dict):
            raw_cards = parsed.get("cards") or parsed.get("items") or []
        elif isinstance(parsed, list):
            raw_cards = parsed
    except Exception:
        parsed_loose = _try_loose_json_parse(content_str)
        if parsed_loose is not None:
            raw_cards = parsed_loose.get("cards") or parsed_loose.get("items") or []

    if not raw_cards:
        return []

    # пост-валидация + дедуп заголовков
    seen_titles = set()
    result: List[Dict[str, Any]] = []

    def norm_title(t: str) -> str:
        t = (t or "").strip().lower()
        t = re.sub(r"[\s\.\,\!\?\:\;\-–—]+", " ", t)
        return " ".join(t.split())

    for c in raw_cards:
        if not isinstance(c, dict):
            continue

        title = _clean_text(c.get("title"), 160)
        body = _clean_text(c.get("body") or c.get("summary"), 2600)
        if not title or not body:
            continue

        nt = norm_title(title)
        if nt and nt in seen_titles:
            continue

        tags_out = _normalize_tag_list(c.get("tags"), fallback=tags)
        importance = _clamp01(c.get("importance_score", c.get("importance", 0.6)))

        result.append(
            {
                "title": title,
                "body": body,
                "tags": tags_out,
                "importance_score": importance,
                "language": language,
                "quality": "ok",
            }
        )
        if nt:
            seen_titles.add(nt)

    return result


# ==========
# Нормализация Telegram-постов → EYYE-карточка
# ==========

def normalize_telegram_post(raw_text: str, channel_title: str, language: str = "ru") -> Dict[str, Any]:
    """
    Возвращает dict:
      {title, body, tags, importance_score, language, source_name, quality}
    quality:
      - "ok" (если нормализация удалась)
      - "fallback_raw" (если OpenAI недоступен/сломался)
    """
    first_line = (raw_text or "").strip().split("\n")[0] or "Новость"
    fallback = {
        "title": _clean_text(first_line, 200) or "Новость",
        "body": _clean_text(raw_text, 2000) or _clean_text(first_line, 200),
        "tags": [],
        "importance_score": 0.5,
        "language": language,
        "source_name": None,
        "quality": "fallback_raw",
    }

    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip normalize_telegram_post")
        return fallback

    system_prompt = (
        "Ты модуль нормализации новостной ленты EYYE.\n"
        "Верни ОДНУ аккуратную новостную карточку в формате JSON.\n\n"
        "Правила:\n"
        "1) НЕ придумывай новости.\n"
        "2) title — одно краткое предложение без кликбейта.\n"
        "3) body — 2–4 абзаца по 1–3 предложения, без эмодзи.\n"
        "4) tags — 1–6 тегов только из списка:\n"
        "   world_news, business, finance, tech, science, history, politics, society,\n"
        "   entertainment, gaming, sports, lifestyle, education, city, uk_students.\n"
        "5) importance_score — 0..1\n"
        "6) language — 'ru'/'en'...\n"
        "7) source_name — только если явно есть название издания в тексте.\n"
        "8) НЕ упоминай Telegram.\n"
        "Верни СТРОГО один JSON-объект."
    )

    user_prompt = (
        f"Язык оригинала (hint): {language}\n"
        f"Название Telegram-канала: {channel_title}\n\n"
        "Сырой текст поста:\n"
        "-------------------\n"
        f"{(raw_text or '').strip()}\n"
        "-------------------\n\n"
        "Верни JSON:\n"
        "{\n"
        '  \"title\": \"...\",\n'
        '  \"body\": \"...\",\n'
        '  \"tags\": [\"world_news\"],\n'
        '  \"importance_score\": 0.7,\n'
        '  \"language\": \"ru\",\n'
        '  \"source_name\": \"...\"\n'
        "}"
    )

    payload: Dict[str, Any] = {
        "model": OPENAI_MODEL or "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": 700,
        "temperature": 0.3,
        "response_format": {"type": "json_object"},
    }

    started = time.monotonic()
    resp_json = call_openai_chat(payload)
    elapsed = time.monotonic() - started
    logger.info(
        "OpenAI normalize_telegram_post call finished in %.2fs (channel_title=%r)",
        elapsed,
        channel_title,
    )

    if not resp_json:
        return fallback

    content_str = _extract_message_content(resp_json).strip()
    if not content_str:
        return fallback

    parsed: Dict[str, Any] | None = None
    try:
        obj = json.loads(content_str)
        if isinstance(obj, dict):
            parsed = obj
    except Exception:
        parsed = _try_loose_json_parse(content_str)

    if not isinstance(parsed, dict):
        return fallback

    title = _clean_text(parsed.get("title"), 200) or fallback["title"]
    body = _clean_text(parsed.get("body"), 2600) or fallback["body"]

    tags = _normalize_tag_list(parsed.get("tags"), fallback=[])
    importance_score = _clamp01(parsed.get("importance_score", parsed.get("importance", 0.5)))

    lang_value = str(parsed.get("language") or "").strip() or language
    source_name = parsed.get("source_name")
    if isinstance(source_name, str):
        source_name = source_name.strip() or None
    else:
        source_name = None

    return {
        "title": title,
        "body": body,
        "tags": tags,
        "importance_score": importance_score,
        "language": lang_value,
        "source_name": source_name,
        "quality": "ok",
    }


# ==========
# Нормализация Wikipedia extract → EYYE-карточка
# ==========

def normalize_wikipedia_article(
    *,
    title_hint: str,
    raw_text: str,
    language: str,
    why_now: str,
) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        first = (title_hint or "").strip() or "Статья"
        body = _clean_text(raw_text, 1400) or first
        return {
            "title": first[:200],
            "body": body,
            "tags": [],
            "importance_score": 0.6,
            "language": language,
            "source_name": None,
            "why_now": why_now,
            "quality": "fallback_raw",
        }

    system_prompt = (
        "Ты нормализуешь выдержку из Wikipedia в формат карточки EYYE.\n"
        "Важно:\n"
        "- НЕ выдумывай факты.\n"
        "- Пиши как короткая новостная заметка: нейтрально, компактно.\n"
        "- why_now: используй СТРОГО переданный hint, ничего не добавляй от себя.\n"
        "- tags: только из списка:\n"
        "  world_news, business, finance, tech, science, history, politics, society,\n"
        "  entertainment, gaming, sports, lifestyle, education, city, uk_students.\n"
        "Верни валидный JSON-объект."
    )

    user_prompt = (
        f"language: {language}\n"
        f"title_hint: {title_hint}\n"
        f"why_now_hint: {why_now}\n\n"
        "text:\n"
        "-------------------\n"
        f"{(raw_text or '').strip()}\n"
        "-------------------\n\n"
        "JSON:\n"
        "{\n"
        '  \"title\": \"...\",\n'
        '  \"body\": \"...\",\n'
        '  \"tags\": [\"world_news\"],\n'
        '  \"importance_score\": 0.7,\n'
        '  \"language\": \"ru\",\n'
        '  \"source_name\": null,\n'
        '  \"why_now\": \"...\"\n'
        "}"
    )

    payload: Dict[str, Any] = {
        "model": OPENAI_WIKIPEDIA_MODEL or OPENAI_MODEL or "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": 420,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }

    resp_json = call_openai_chat(payload)
    if not resp_json:
        return {
            "title": _clean_text(title_hint, 200) or "Статья",
            "body": _clean_text(raw_text, 1400),
            "tags": [],
            "importance_score": 0.6,
            "language": language,
            "source_name": None,
            "why_now": why_now,
            "quality": "fallback_raw",
        }

    content_str = _extract_message_content(resp_json).strip()
    if not content_str:
        return {
            "title": _clean_text(title_hint, 200) or "Статья",
            "body": _clean_text(raw_text, 1400),
            "tags": [],
            "importance_score": 0.6,
            "language": language,
            "source_name": None,
            "why_now": why_now,
            "quality": "fallback_raw",
        }

    parsed = None
    try:
        parsed = json.loads(content_str)
    except Exception:
        parsed = _try_loose_json_parse(content_str)

    if not isinstance(parsed, dict):
        parsed = {}

    out_title = _clean_text(parsed.get("title"), 220) or _clean_text(title_hint, 220) or "Статья"
    out_body = _clean_text(parsed.get("body"), 2600) or _clean_text(raw_text, 1400)

    out_tags = _normalize_tag_list(parsed.get("tags"), fallback=[])
    out_importance = _clamp01(parsed.get("importance_score", 0.6))

    out_lang = str(parsed.get("language") or "").strip() or language

    out_source = parsed.get("source_name")
    if isinstance(out_source, str):
        out_source = out_source.strip() or None
    else:
        out_source = None

    out_why = _clean_text(parsed.get("why_now"), 220) or _clean_text(why_now, 220)

    return {
        "title": out_title,
        "body": out_body,
        "tags": out_tags,
        "importance_score": out_importance,
        "language": out_lang,
        "source_name": out_source,
        "why_now": out_why,
        "quality": "ok",
    }

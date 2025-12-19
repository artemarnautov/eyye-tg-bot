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

# ==========
# ДИНАМИЧЕСКИЙ конфиг (важно: env читается во время вызова)
# ==========

def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()

def _openai_api_key() -> str:
    return _env_str("OPENAI_API_KEY", "")

def _openai_model() -> str:
    return _env_str("OPENAI_MODEL", "gpt-4.1-mini") or "gpt-4.1-mini"

def _openai_wikipedia_model() -> str:
    return _env_str("OPENAI_WIKIPEDIA_MODEL", _openai_model()) or _openai_model()

def _openai_api_base() -> str:
    return _env_str("OPENAI_BASE_URL", "https://api.openai.com/v1")

def _openai_chat_completions_url() -> str:
    return _openai_api_base().rstrip("/") + "/chat/completions"

def _openai_timeout_seconds() -> float:
    try:
        return float(_env_str("OPENAI_TIMEOUT_SECONDS", "30"))
    except Exception:
        return 30.0

def _output_language() -> str:
    # Всегда русский по умолчанию
    lang = _env_str("EYYE_OUTPUT_LANGUAGE", "ru").lower()
    return "ru" if lang not in ("ru", "en") else lang

def is_configured() -> bool:
    return bool(_openai_api_key())


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
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text[:max_len].strip()


def _normalize_tag_list(tags: Any, fallback: List[str] | None = None) -> List[str]:
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

    seen = set()
    deduped: List[str] = []
    for t in out:
        if t not in seen:
            seen.add(t)
            deduped.append(t)

    if not deduped:
        fb = []
        for t in fallback:
            v = str(t or "").strip().lower()
            v = TAG_ALIASES.get(v, v)
            if v in ALLOWED_TAGS_SET:
                fb.append(v)
        seen2 = set()
        deduped2 = []
        for t in fb:
            if t not in seen2:
                seen2.add(t)
                deduped2.append(t)
        return deduped2

    return deduped


def call_openai_chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    api_key = _openai_api_key()
    if not api_key:
        logger.warning("OPENAI_API_KEY is not set, skipping OpenAI call")
        return {}

    url = _openai_chat_completions_url()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    model = (payload.get("model") or _openai_model()).strip() or _openai_model()

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
        with urllib.request.urlopen(req, timeout=_openai_timeout_seconds()) as resp:
            raw = resp.read().decode("utf-8")
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("OpenAI chat.completions call OK (%.2fs)", elapsed)
        logger.debug("OpenAI raw response (first %d chars): %s", RAW_LOG_MAX_LEN, raw[:RAW_LOG_MAX_LEN])
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
# Генерация карточек “с нуля” (всегда RU)
# ==========

def generate_cards_for_tags(tags: List[str], language: str, count: int) -> List[Dict[str, Any]]:
    if not is_configured():
        logger.warning("OPENAI_API_KEY is not set, skip OpenAI card generation")
        return []

    # принудительно выводим на русском
    language = _output_language()

    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    tags = _normalize_tag_list(tags, fallback=DEFAULT_FEED_TAGS)
    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    system_prompt = (
        "Ты – движок новостной ленты EYYE.\n"
        "Сгенерируй короткие новостные карточки.\n"
        "Каждая карточка: заголовок + 2–4 абзаца текста.\n"
        "ВАЖНО: пиши ТОЛЬКО на русском языке.\n"
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
        "output_language": "ru",
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
        "model": _openai_model(),
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
                "language": "ru",
                "quality": "ok",
            }
        )
        if nt:
            seen_titles.add(nt)

    return result


# ==========
# Нормализация Telegram → карточка (вывод всегда RU)
# ==========

def normalize_telegram_post(raw_text: str, channel_title: str, language: str = "ru") -> Dict[str, Any]:
    input_lang_hint = (language or "ru").strip().lower()
    out_lang = _output_language()  # будет ru

    first_line = (raw_text or "").strip().split("\n")[0] or "Новость"
    fallback = {
        "title": _clean_text(first_line, 200) or "Новость",
        "body": _clean_text(raw_text, 2000) or _clean_text(first_line, 200),
        "tags": [],
        "importance_score": 0.5,
        "language": out_lang,
        "source_name": None,
        "quality": "fallback_raw",
        "input_language_hint": input_lang_hint,
    }

    if not is_configured():
        logger.warning("OPENAI_API_KEY is not set, skip normalize_telegram_post")
        return fallback

    system_prompt = (
        "Ты модуль нормализации новостной ленты EYYE.\n"
        "Верни ОДНУ аккуратную новостную карточку в формате JSON.\n\n"
        "КРИТИЧНО:\n"
        "- Пиши итоговую карточку ТОЛЬКО на русском языке.\n"
        "- Если исходный текст на другом языке, перескажи/переведи смысл на русский без добавления фактов.\n\n"
        "Правила:\n"
        "1) НЕ придумывай новости.\n"
        "2) title — одно краткое предложение без кликбейта.\n"
        "3) body — 2–4 абзаца по 1–3 предложения, без эмодзи.\n"
        "4) tags — 1–6 тегов только из списка:\n"
        "   world_news, business, finance, tech, science, history, politics, society,\n"
        "   entertainment, gaming, sports, lifestyle, education, city, uk_students.\n"
        "5) importance_score — 0..1\n"
        "6) language — всегда 'ru'\n"
        "7) source_name — только если явно есть название издания в тексте.\n"
        "8) НЕ упоминай Telegram.\n"
        "Верни СТРОГО один JSON-объект."
    )

    user_prompt = (
        f"Язык оригинала (hint): {input_lang_hint}\n"
        f"Язык вывода: ru\n"
        f"Название источника (Telegram-канала): {channel_title}\n\n"
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
        "model": _openai_model(),
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
    logger.info("OpenAI normalize_telegram_post call finished in %.2fs (channel_title=%r)", elapsed, channel_title)

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
        "language": "ru",
        "source_name": source_name,
        "quality": "ok",
        "input_language_hint": input_lang_hint,
    }


# ==========
# Нормализация Wikipedia → карточка (вывод всегда RU)
# ==========

def normalize_wikipedia_article(*, title_hint: str, raw_text: str, language: str, why_now: str) -> Dict[str, Any]:
    input_lang_hint = (language or "ru").strip().lower()
    out_lang = _output_language()

    if not is_configured():
        first = (title_hint or "").strip() or "Статья"
        body = _clean_text(raw_text, 1400) or first
        return {
            "title": first[:200],
            "body": body,
            "tags": [],
            "importance_score": 0.6,
            "language": out_lang,
            "source_name": None,
            "why_now": _clean_text(why_now, 220),
            "quality": "fallback_raw",
            "input_language_hint": input_lang_hint,
        }

    system_prompt = (
        "Ты нормализуешь выдержку из Wikipedia в формат карточки EYYE.\n"
        "КРИТИЧНО:\n"
        "- Пиши итоговую карточку ТОЛЬКО на русском языке.\n"
        "- Если исходный текст/why_now на другом языке, переведи смысл на русский, не добавляя фактов.\n"
        "Важно:\n"
        "- НЕ выдумывай факты.\n"
        "- Пиши как короткая новостная заметка: нейтрально, компактно.\n"
        "- tags: только из списка:\n"
        "  world_news, business, finance, tech, science, history, politics, society,\n"
        "  entertainment, gaming, sports, lifestyle, education, city, uk_students.\n"
        "- language: всегда 'ru'\n"
        "Верни валидный JSON-объект."
    )

    user_prompt = (
        f"input_language_hint: {input_lang_hint}\n"
        f"output_language: ru\n"
        f"title_hint: {title_hint}\n"
        f"why_now_hint (translate to ru, keep meaning): {why_now}\n\n"
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
        "model": _openai_wikipedia_model(),
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
            "language": out_lang,
            "source_name": None,
            "why_now": _clean_text(why_now, 220),
            "quality": "fallback_raw",
            "input_language_hint": input_lang_hint,
        }

    content_str = _extract_message_content(resp_json).strip()
    if not content_str:
        return {
            "title": _clean_text(title_hint, 200) or "Статья",
            "body": _clean_text(raw_text, 1400),
            "tags": [],
            "importance_score": 0.6,
            "language": out_lang,
            "source_name": None,
            "why_now": _clean_text(why_now, 220),
            "quality": "fallback_raw",
            "input_language_hint": input_lang_hint,
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

    out_why = _clean_text(parsed.get("why_now"), 220) or _clean_text(why_now, 220)

    out_source = parsed.get("source_name")
    if isinstance(out_source, str):
        out_source = out_source.strip() or None
    else:
        out_source = None

    return {
        "title": out_title,
        "body": out_body,
        "tags": out_tags,
        "importance_score": out_importance,
        "language": "ru",
        "source_name": out_source,
        "why_now": out_why,
        "quality": "ok",
        "input_language_hint": input_lang_hint,
    }

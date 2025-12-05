# file: src/webapp_backend/openai_client.py
import json
import logging
import os
import re
import time
from typing import Any, Dict, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ==========
# Конфиг OpenAI
# ==========

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

# Можно переопределить базовый URL (для прокси / совместимых API)
OPENAI_API_BASE = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_CHAT_COMPLETIONS_URL = OPENAI_API_BASE.rstrip("/") + "/chat/completions"

OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))

RAW_LOG_MAX_LEN = 4000


def is_configured() -> bool:
    """
    Есть ли вообще ключ для OpenAI.
    """
    return bool(OPENAI_API_KEY)


# ==========
# Низкоуровневый вызов chat.completions
# ==========

import urllib.request
import urllib.error


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


# ==========
# Парсеры "кривого" JSON карточек
# ==========

# Объекты, где точно есть title — этого достаточно для salvage
CARD_BLOCK_RE = re.compile(
    r"\{[^{}]*\"title\"\s*:\s*\"[^\"]+\"[^{}]*\}",
    re.DOTALL,
)


def _extract_str(block: str, field: str) -> str | None:
    m = re.search(rf'"{re.escape(field)}"\s*:\s*"([^"]*)"', block)
    if m:
        value = (m.group(1) or "").strip()
        return value or None
    return None


def _extract_float(block: str, field: str, default: float = 1.0) -> float:
    m = re.search(rf'"{re.escape(field)}"\s*:\s*([0-9]+(\.[0-9]+)?)', block)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return default
    return default


def _extract_tags_from_block(block: str, fallback_tags: List[str]) -> List[str]:
    # Пытаемся вытащить tags: ["...","..."]
    m = re.search(r'"tags"\s*:\s*\[([^\]]*)\]', block)
    tags: List[str] = []
    if m:
        inner = m.group(1)
        for part in inner.split(","):
            part = part.strip()
            if not part:
                continue
            # убираем кавычки по краям
            if part.startswith('"') and part.endswith('"'):
                part = part[1:-1]
            if part:
                tags.append(part.strip())

    if not tags:
        # fallback: одиночный tag
        tag = _extract_str(block, "tag")
        if tag:
            tags.append(tag)

    if not tags:
        tags = list(fallback_tags)

    return tags


def _parse_openai_cards_from_text(content: str, fallback_tags: List[str]) -> List[Dict[str, Any]]:
    """
    Пытаемся вытащить карточки из "кривого" JSON-текста.
    Ищем отдельные объекты с полями title/body/summary/tag/importance/tags.
    Если ничего не нашли — возвращаем пустой список.
    """
    if not content:
        return []

    cards: List[Dict[str, Any]] = []

    for idx, m in enumerate(CARD_BLOCK_RE.finditer(content), start=1):
        block = m.group(0)

        title = _extract_str(block, "title") or f"Новость #{idx}"
        body = (
            _extract_str(block, "body")
            or _extract_str(block, "summary")
            or ""
        )

        if not title and not body:
            continue

        importance = _extract_float(block, "importance_score", 1.0)
        if importance == 1.0:
            importance = _extract_float(block, "importance", 1.0)

        tags = _extract_tags_from_block(block, fallback_tags)

        cards.append(
            {
                "title": title,
                "body": body or title,
                "tags": tags,
                "importance_score": importance,
            }
        )

    return cards


def _try_loose_json_parse(content: str) -> Dict[str, Any] | None:
    """
    Пытаемся немного "починить" JSON:
    - обрезаем мусор с краёв
    - пробуем вытащить только объект, начинающийся с { и заканчивающийся на }.
    Возвращаем dict или None.
    """
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


def _extract_message_content(resp_json: Dict[str, Any]) -> str:
    """
    Вытаскиваем message.content из chat.completions-ответа,
    поддерживая как строку, так и список блоков.
    """
    if not resp_json:
        return ""

    choices = resp_json.get("choices")
    if not isinstance(choices, list) or not choices:
        logger.error("No choices in OpenAI response")
        return ""

    message = choices[0].get("message") or {}
    content = message.get("content")

    # Поддерживаем как старый формат (строка), так и новый (список блоков)
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
        content_str = "\n".join(parts)
    elif isinstance(content, str):
        content_str = content
    else:
        content_str = str(content or "")

    return content_str


# ==========
# Высокоуровневая генерация карточек
# ==========

DEFAULT_FEED_TAGS = ["world_news", "business", "tech", "uk_students"]


def generate_cards_for_tags(
    tags: List[str],
    language: str,
    count: int,
) -> List[Dict[str, Any]]:
    """
    Синхронная генерация новых карточек через OpenAI.
    Результат: список словарей формата, подходящего для вставки в таблицу cards:
    {
      "title": str,
      "body": str,
      "tags": [str, ...],
      "importance_score": float,
    }
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip OpenAI card generation")
        return []

    if not tags:
        tags = list(DEFAULT_FEED_TAGS)

    system_prompt = (
        "Ты – движок новостной ленты EYYE.\n"
        "Твоя задача – сгенерировать короткие новостные карточки в одном стиле.\n"
        "Каждая карточка: цепляющий заголовок и 2–4 абзаца текста.\n"
        "Пиши на языке, указанном в параметрах (ru или en).\n"
        "Отвечай строго валидным JSON без лишнего текста.\n"
        "Структура JSON:\n"
        "{\n"
        '  \"cards\": [\n'
        "    {\n"
        '      \"title\": \"Заголовок\",\n'
        '      \"body\": \"Текст карточки (2–4 абзаца)\",\n'
        '      \"tags\": [\"один_из_тегов_из_списка\"],\n'
        '      \"importance_score\": 1.0\n'
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
            "Не выдумывай точные факты про конкретных реальных людей, лучше описывай общие тренды.",
            "Избегай кликбейта, но делай заголовки цепляющими.",
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
        # Даже если модель забьёт на это, это не сломает совместимость
        "response_format": {"type": "json_object"},
    }

    started = time.monotonic()
    resp_json = call_openai_chat(payload)
    elapsed = time.monotonic() - started
    logger.info("OpenAI card generation call finished in %.2fs", elapsed)

    if not resp_json:
        return []

    content_str = _extract_message_content(resp_json)
    if not content_str.strip():
        logger.error("Empty content in OpenAI cards response")
        return []

    logger.debug(
        "OpenAI cards raw content (first %d chars): %s",
        200,
        content_str[:200].replace("\n", " "),
    )

    raw_cards: List[Dict[str, Any]] = []

    # 1) Пробуем честный json.loads(content_str)
    try:
        parsed = json.loads(content_str)
        if isinstance(parsed, dict):
            raw_cards = parsed.get("cards") or parsed.get("items") or []
        elif isinstance(parsed, list):
            raw_cards = parsed
        else:
            logger.warning("Parsed card JSON is not dict/list, got %s", type(parsed))
            raw_cards = []
    except json.JSONDecodeError:
        logger.warning(
            "Failed to json.loads OpenAI content directly, trying loose JSON parse",
            exc_info=True,
        )
        parsed_loose = _try_loose_json_parse(content_str)
        if parsed_loose is not None:
            raw_cards = parsed_loose.get("cards") or parsed_loose.get("items") or []
        else:
            raw_cards = []

    # 2) Если до сих пор пусто — задействуем salvage по тексту
    if not raw_cards:
        logger.error(
            "Card list is empty after JSON parsing, trying salvage parser on raw text"
        )
        raw_cards = _parse_openai_cards_from_text(content_str, tags)
        if not raw_cards:
            logger.error("Salvage parser did not find any valid card items.")
            return []
        logger.warning(
            "Salvage parser recovered %d card items from broken JSON.",
            len(raw_cards),
        )

    result: List[Dict[str, Any]] = []
    for c in raw_cards:
        if not isinstance(c, dict):
            continue

        title = str(c.get("title", "")).strip()
        body = str(c.get("body") or c.get("summary") or "").strip()
        if not title or not body:
            # если salvage уже подставил body/title, сюда не попадём,
            # но для честного JSON лучше фильтровать
            continue

        card_tags = c.get("tags") or tags
        if not isinstance(card_tags, list):
            card_tags = [str(card_tags)] if card_tags else tags

        try:
            importance = float(c.get("importance_score", c.get("importance", 1.0)))
        except (TypeError, ValueError):
            importance = 1.0

        result.append(
            {
                "title": title,
                "body": body,
                "tags": [str(t).strip() for t in card_tags if t],
                "importance_score": importance,
            }
        )

    return result


# ==========
# Нормализация Telegram-постов → EYYE-карточка
# ==========

def _normalize_tag_list(tags: Any) -> List[str]:
    """
    Нормализует поле tags к списку строк в нижнем регистре.
    """
    if not tags:
        return []
    if isinstance(tags, str):
        tags = [tags]
    if not isinstance(tags, list):
        return []

    result: List[str] = []
    for t in tags:
        if not isinstance(t, str):
            continue
        v = t.strip().lower()
        if v:
            result.append(v)

    # убираем дубликаты, сохраняя порядок
    seen = set()
    deduped: List[str] = []
    for t in result:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


def normalize_telegram_post(
    raw_text: str,
    channel_title: str,
    language: str = "ru",
) -> Dict[str, Any]:
    """
    Нормализация сырого Telegram-поста в EYYE-карточку с помощью OpenAI.

    Возвращает dict:
      {
        "title": str,
        "body": str,
        "tags": [str, ...],
        "importance_score": float,
        "language": str,
        "source_name": Optional[str],
      }
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skip normalize_telegram_post")
        # Минимальный фоллбек: тупо заворачиваем сырой текст
        first_line = (raw_text or "").strip().split("\n")[0] or "Новость"
        return {
            "title": first_line[:200],
            "body": (raw_text or "").strip()[:2000] or first_line[:200],
            "tags": [],
            "importance_score": 0.5,
            "language": language,
            "source_name": None,
        }

    system_prompt = (
        "Ты модуль нормализации новостной ленты EYYE.\n"
        "На вход ты получаешь сырой пост из Telegram-канала и название канала.\n"
        "Твоя задача — вернуть ОДНУ аккуратную новостную карточку в формате JSON.\n\n"
        "Правила:\n"
        "1) НЕ придумывай новости, работай только с предоставленным текстом.\n"
        "2) title — одно краткое предложение, передающее суть новости без кликбейта.\n"
        "3) body — 2–4 абзаца по 1–3 предложения, без воды, без эмодзи, без обращений к читателю.\n"
        "4) tags — список из 1–6 тематических тегов.\n"
        "   Используй ТОЛЬКО следующий список тегов (строго так, латиницей, нижний регистр):\n"
        "   world_news, business, finance, tech, science, history, politics, society,\n"
        "   entertainment, gaming, sports, lifestyle, education, city, uk_students.\n"
        "   Выбирай теги, которые лучше всего описывают новость; если ничего не подходит,\n"
        "   используй world_news.\n"
        "5) importance_score — число от 0 до 1 (0.1 — мелкая заметка, 0.9 — очень важная/масштабная новость).\n"
        "6) language — ISO-код исходного языка ('ru', 'en' и т.п.).\n"
        "7) source_name — заполняй ТОЛЬКО если в тексте явно есть название издания/бренда\n"
        "   (например, 'Bloomberg', 'Reuters', 'Медуза', 'РБК', 'Forbes' и т.п.). Ничего не выдумывай.\n"
        "8) НЕ упоминай Telegram, каналы, подписи вида 'подпишись', 'читать в полном виде' и т.п.\n"
        "9) Стиль текста — как у коротких новостных заметок независимого медиа (в духе Meduza / Meduza LIVE):\n"
        "   нейтральный тон, без оценочных суждений, компактно и по делу.\n\n"
        "Верни СТРОГО один JSON-объект БЕЗ пояснений вокруг."
    )

    user_prompt = (
        f"Язык оригинала (hint): {language}\n"
        f"Название Telegram-канала: {channel_title}\n\n"
        "Сырой текст поста из Telegram:\n"
        "-------------------\n"
        f"{(raw_text or '').strip()}\n"
        "-------------------\n\n"
        "Верни JSON вида:\n"
        "{\n"
        '  \"title\": \"...\",\n'
        '  \"body\": \"...\",\n'
        '  \"tags\": [\"world_news\"],\n'
        '  \"importance_score\": 0.7,\n'
        '  \"language\": \"ru\",\n'
        '  \"source_name\": \"...\" // опционально\n'
        "}"
    )

    payload: Dict[str, Any] = {
        "model": OPENAI_MODEL or "gpt-4.1-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_output_tokens": 800,
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

    # Фоллбек, если совсем ничего не вернулось
    first_line = (raw_text or "").strip().split("\n")[0] or "Новость"
    fallback = {
        "title": first_line[:200],
        "body": (raw_text or "").strip()[:2000] or first_line[:200],
        "tags": [],
        "importance_score": 0.5,
        "language": language,
        "source_name": None,
    }

    if not resp_json:
        return fallback

    content_str = _extract_message_content(resp_json)
    if not content_str.strip():
        logger.error("Empty content in normalize_telegram_post response")
        return fallback

    # Пытаемся честно распарсить JSON
    data: Dict[str, Any] | None = None
    try:
        parsed = json.loads(content_str)
        if isinstance(parsed, dict):
            data = parsed
        elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
            # На всякий случай, если модель вернула список из одного объекта
            data = parsed[0]
    except json.JSONDecodeError:
        logger.warning(
            "Failed to json.loads normalize_telegram_post content, trying loose JSON parse",
            exc_info=True,
        )
        data = _try_loose_json_parse(content_str)

    # Если JSON так и не распарсился — salvage из текстового блока
    if data is None:
        logger.error(
            "normalize_telegram_post JSON parsing failed, trying salvage via CARD_BLOCK_RE"
        )
        salvaged_cards = _parse_openai_cards_from_text(content_str, [])
        if salvaged_cards:
            c = salvaged_cards[0]
            title = str(c.get("title", "")).strip() or fallback["title"]
            body = str(c.get("body", "")).strip() or fallback["body"]
            tags = _normalize_tag_list(c.get("tags"))
            try:
                importance_score = float(c.get("importance_score", 0.5))
            except (TypeError, ValueError):
                importance_score = 0.5
            importance_score = max(0.0, min(1.0, importance_score))

            return {
                "title": title,
                "body": body,
                "tags": tags,
                "importance_score": importance_score,
                "language": language,
                "source_name": None,
            }
        else:
            logger.error("normalize_telegram_post salvage parser also failed, using fallback")
            return fallback

    # Нормализация полей
    title = str(data.get("title") or "").strip()
    body = str(data.get("body") or "").strip()
    tags = _normalize_tag_list(data.get("tags"))
    try:
        importance_score = float(data.get("importance_score", data.get("importance", 0.5)))
    except (TypeError, ValueError):
        importance_score = 0.5
    importance_score = max(0.0, min(1.0, importance_score))

    lang_value = str(data.get("language") or "").strip() or language
    source_name = (data.get("source_name") or "").strip() or None

    if not title:
        title = fallback["title"]
    if not body:
        body = fallback["body"]

    return {
        "title": title,
        "body": body,
        "tags": tags,
        "importance_score": importance_score,
        "language": lang_value,
        "source_name": source_name,
    }

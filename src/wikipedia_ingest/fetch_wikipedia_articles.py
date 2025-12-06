# file: src/wikipedia_ingest/fetch_wikipedia_articles.py
import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote

import requests
from supabase import create_client, Client

# ==========
# Логирование
# ==========

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Пути к общему коду (как в telegram_ingest)
# ==========

CURRENT_DIR = Path(__file__).resolve()
SRC_DIR = CURRENT_DIR.parents[1]  # .../src
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from webapp_backend.openai_client import normalize_telegram_post
from webapp_backend.cards_service import _insert_cards_into_db

# ==========
# Supabase
# ==========

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========
# Настройки Wikipedia
# ==========

# Жёсткий список статей по нашим темам.
# Потом можно будет заменить на что-то более умное.
WIKIPEDIA_ARTICLES: Dict[str, List[Dict[str, Any]]] = {
    "en": [
        {"title": "Artificial intelligence", "tags": ["tech", "world_news"]},
        {"title": "Startup company", "tags": ["business", "careers"]},
        {
            "title": "Higher education in the United Kingdom",
            "tags": ["uk_students", "education"],
        },
        {"title": "Streaming media", "tags": ["entertainment", "movies"]},
        {"title": "Climate change", "tags": ["world_news", "society"]},
    ],
    "ru": [
        {"title": "Искусственный интеллект", "tags": ["tech", "world_news", "russia"]},
        {"title": "Стартап", "tags": ["business", "careers"]},
        {
            "title": "Высшее образование в Великобритании",
            "tags": ["uk_students", "education"],
        },
        {"title": "Стриминг", "tags": ["entertainment", "movies"]},
        {"title": "Изменение климата", "tags": ["world_news", "society"]},
    ],
}

# Сколько статей максимум брать на один язык за запуск
WIKIPEDIA_MAX_PER_LANG = int(os.getenv("WIKIPEDIA_MAX_PER_LANG", "5"))

USER_AGENT = os.getenv(
    "WIKIPEDIA_USER_AGENT",
    "EYYE-NewsBot/0.1 (https://example.com; contact@example.com)",
)


def _build_wikipedia_url(lang: str, title: str) -> str:
    encoded = quote(title.replace(" ", "_"))
    return f"https://{lang}.wikipedia.org/wiki/{encoded}"


def _fetch_page_summary(lang: str, title: str) -> Dict[str, Any] | None:
    """
    Берём summary статьи из REST API Wikipedia.
    """
    encoded = quote(title.replace(" ", "_"))
    url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{encoded}"

    try:
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
    except Exception:
        log.exception("Failed to call Wikipedia API for %s (%s)", title, lang)
        return None

    if resp.status_code != 200:
        log.warning(
            "Wikipedia API returned %s for %s (%s)", resp.status_code, title, lang
        )
        return None

    data = resp.json()
    extract = (data.get("extract") or "").strip()
    if not extract:
        return None

    page_title = (data.get("title") or title).strip()
    content_urls = data.get("content_urls") or {}
    desktop = content_urls.get("desktop") or {}
    page_url = desktop.get("page") or _build_wikipedia_url(lang, page_title)

    return {
        "title": page_title,
        "extract": extract,
        "url": page_url,
        "lang": lang,
    }


def _merge_tags(base_tags: List[str], model_tags: Any) -> List[str]:
    """
    Объединяем теги из нашей конфигурации и из модели OpenAI.
    """
    result: List[str] = []

    def _add_many(items: Any):
        nonlocal result
        if not items:
            return
        if isinstance(items, str):
            items = [items]
        if not isinstance(items, list):
            return
        for t in items:
            if not isinstance(t, str):
                continue
            v = t.strip().lower()
            if not v:
                continue
            result.append(v)

    _add_many(base_tags)
    _add_many(model_tags)

    seen = set()
    deduped: List[str] = []
    for t in result:
        if t not in seen:
            seen.add(t)
            deduped.append(t)
    return deduped


def _card_exists(url: str) -> bool:
    """
    Проверяем, есть ли уже карточка с таким source_ref (URL).
    """
    resp = (
        supabase.table("cards")
        .select("id")
        .eq("source_type", "wikipedia")
        .eq("source_ref", url)
        .limit(1)
        .execute()
    )
    data = resp.data or []
    return len(data) > 0


def fetch_wikipedia_articles() -> None:
    """
    Основной пайплайн: забираем статьи из Wikipedia и создаём карточки в cards.
    """
    all_cards: List[Dict[str, Any]] = []

    for lang, articles in WIKIPEDIA_ARTICLES.items():
        log.info("Processing Wikipedia articles for lang=%s", lang)

        for article in articles[:WIKIPEDIA_MAX_PER_LANG]:
            title = article["title"]
            base_tags = article.get("tags") or []

            summary = _fetch_page_summary(lang, title)
            if not summary:
                log.warning("No summary for %s (%s), skipping", title, lang)
                continue

            url = summary["url"]
            if _card_exists(url):
                log.info("Card for %s already exists, skipping", url)
                continue

            raw_text = summary["extract"]

            try:
                # Используем тот же нормализатор, что и для Telegram — он просто нормализует текст.
                normalized = normalize_telegram_post(
                    raw_text=raw_text,
                    channel_title=f"Wikipedia ({lang})",
                    language=lang,
                )
            except Exception:
                log.exception(
                    "Failed to normalize Wikipedia article %s (%s)", title, lang
                )
                continue

            tags = _merge_tags(base_tags, normalized.get("tags"))
            source_name = (
                (normalized.get("source_name") or "").strip()
                or f"Wikipedia {lang.upper()}"
            )
            language = (normalized.get("language") or "").strip() or lang

            card = {
                "title": (normalized.get("title") or summary["title"]).strip(),
                "body": (normalized.get("body") or raw_text).strip(),
                "tags": tags,
                "importance_score": float(normalized.get("importance_score", 0.4)),
                "language": language,
                "source_name": source_name,
                "source_ref": url,
            }

            log.info(
                "Prepared Wikipedia card: title=%r, source_name=%r, tags=%r",
                card["title"],
                source_name,
                tags,
            )
            all_cards.append(card)

    if not all_cards:
        log.info("No Wikipedia cards prepared on this run")
        return

    # Вставляем пачкой через общий helper (как в Telegram ingest)
    inserted = _insert_cards_into_db(
        supabase,
        all_cards,
        language=None,  # язык берём из самих карточек
        source_type="wikipedia",
        fallback_source_name="Wikipedia",
        source_ref=None,
    )
    log.info("Inserted %d Wikipedia cards", len(inserted))


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    # CLI: PYTHONPATH=src python -m wikipedia_ingest.fetch_wikipedia_articles
    main()

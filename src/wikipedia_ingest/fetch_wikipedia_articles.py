# file: src/wikipedia_ingest/fetch_wikipedia_articles.py
import os
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from supabase import Client, create_client
from dotenv import load_dotenv  # 🔹 добавили

from webapp_backend.openai_client import normalize_telegram_post

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Supabase
# ==========

# 🔹 грузим .env (как в telegram_ingest)
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_KEY is not set. Check your .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========
# Конфиг Wikipedia / Wikimedia
# ==========

WIKIPEDIA_LANGS: List[str] = ["en", "ru"]

# Базовые (seed) статьи на случай, если trending не работает
WIKIPEDIA_SEED_ARTICLES: Dict[str, List[str]] = {
    "en": [
        "Artificial_intelligence",
        "Startup_company",
        "Universities_in_the_United_Kingdom",
        "Streaming_media",
        "Climate_change",
    ],
    "ru": [
        "Искусственный_интеллект",
        "Стартап",
        "Система_образования_Великобритании",
        "Потоковое_мультимедиа",
        "Изменение_климата",
    ],
}

# Проекты для Wikimedia API
WIKIMEDIA_PROJECTS: Dict[str, str] = {
    "en": "en.wikipedia.org",
    "ru": "ru.wikipedia.org",
}

# Ещё немного конфигов
WIKIPEDIA_TRENDING_TITLES_PER_LANG = int(
    os.getenv("WIKIPEDIA_TRENDING_TITLES_PER_LANG", "20")
)

WIKIMEDIA_USER_AGENT = os.getenv(
    "WIKIMEDIA_USER_AGENT",
    "EYYE-MVP/0.1 (https://github.com/artemarnautov/eyye-tg-bot; contact: dev@eyye.local)",
)

WIKIMEDIA_TOP_URL_TEMPLATE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
    "{project}/all-access/{year}/{month}/all-days"
)

WIKIPEDIA_API_URL_TEMPLATE = "https://{lang}.wikipedia.org/w/api.php"

# Сколько карточек максимально вставляем одним батчем
SUPABASE_INSERT_BATCH_SIZE = int(os.getenv("WIKIPEDIA_INSERT_BATCH_SIZE", "50"))

# ==========
# Вспомогательные функции
# ==========


def _card_exists(source_ref: str) -> bool:
    """
    Проверяем, есть ли уже карточка с таким source_type/source_ref.
    """
    resp = (
        supabase.table("cards")
        .select("id")
        .eq("source_type", "wikipedia")
        .eq("source_ref", source_ref)
        .limit(1)
        .execute()
    )
    data = resp.data or []
    return len(data) > 0


def _fetch_trending_titles_for_lang(lang: str) -> List[str]:
    """
    Берём самые популярные статьи за текущий месяц из Wikimedia Pageviews API.
    Если что-то ломается (403 и т.п.) — возвращаем пустой список, а дальше
    выше по стеку упадём на seed-статьи.
    """
    project = WIKIMEDIA_PROJECTS.get(lang)
    if not project:
        log.warning("No Wikimedia project configured for lang=%s", lang)
        return []

    today = datetime.utcnow()
    url = WIKIMEDIA_TOP_URL_TEMPLATE.format(
        project=project,
        year=today.year,
        month=f"{today.month:02d}",
    )

    headers = {
        "User-Agent": WIKIMEDIA_USER_AGENT,
        "accept": "application/json",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch trending articles for %s: %s", lang, e)
        return []

    data = resp.json() or {}
    items = data.get("items") or []
    if not items:
        log.warning("No items in Wikimedia top response for lang=%s", lang)
        return []

    # В ответе items[0].articles — список статей
    first_item = items[0] or {}
    articles = first_item.get("articles") or []

    titles: List[str] = []
    for art in articles:
        title = art.get("article")
        if not isinstance(title, str):
            continue

        # Отбрасываем служебные страницы
        if title.startswith("Special:") or title.startswith("Main_Page"):
            continue

        titles.append(title)
        if len(titles) >= WIKIPEDIA_TRENDING_TITLES_PER_LANG:
            break

    log.info(
        "Fetched %d trending titles for lang=%s",
        len(titles),
        lang,
    )
    return titles


def _build_titles_for_lang(lang: str) -> List[str]:
    """
    Собираем список статей для конкретного языка:
    - seed-статьи (AI, стартапы, UK unis, стриминг, климат)
    - плюс trending из Wikimedia, если получилось их получить
    """
    titles: List[str] = []

    seed = WIKIPEDIA_SEED_ARTICLES.get(lang, [])
    titles.extend(seed)

    trending = _fetch_trending_titles_for_lang(lang)
    for t in trending:
        if t not in titles:
            titles.append(t)

    return titles


def _fetch_article_extract(lang: str, title: str) -> Optional[str]:
    """
    Тянем краткий текст статьи через Wikipedia API.
    Используем prop=extracts, plaintext, ограничиваем по длине.
    """
    api_url = WIKIPEDIA_API_URL_TEMPLATE.format(lang=lang)

    headers = {
        "User-Agent": WIKIMEDIA_USER_AGENT,
    }

    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": True,
        "exchars": 2000,  # примерно первые ~2k символов
        "redirects": 1,
        "titles": title,
    }

    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.warning(
            "Failed to fetch Wikipedia article '%s' (lang=%s): %s",
            title,
            lang,
            e,
        )
        return None

    data = resp.json() or {}
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        log.warning("No pages in Wikipedia response for title=%r, lang=%s", title, lang)
        return None

    page = next(iter(pages.values()))
    extract = page.get("extract")
    if not extract or not str(extract).strip():
        log.warning("Empty extract for title=%r, lang=%s", title, lang)
        return None

    return str(extract)


def _normalize_to_card(
    lang: str,
    title: str,
    url: str,
    extract: str,
) -> Optional[Dict[str, Any]]:
    """
    Прогоняем текст Вики через уже существующий normalize_telegram_post,
    чтобы получить title/body/tags/importance_score.

    Доп. логика:
    - если source_name от модели содержит 'wikipedia', мы его перетираем на
      что-то нейтральное, чтобы не писать пользователю, что источник — Википедия.
    """
    # Lang для нашей модели: оставим "en"/"ru", как и есть
    normalized = normalize_telegram_post(
        raw_text=extract,
        channel_title=f"Wikipedia ({lang})",
        language=lang,
    )

    tags = normalized.get("tags") or []
    if not isinstance(tags, list):
        tags = []

    # Заголовок / тело
    norm_title = (normalized.get("title") or "").strip()
    if not norm_title:
        norm_title = title.replace("_", " ")

    norm_body = (normalized.get("body") or "").strip()
    if not norm_body:
        # fallback: кусок оригинального текста
        norm_body = extract[:800]

    # Важность
    try:
        importance = float(normalized.get("importance_score", 0.5))
    except Exception:
        importance = 0.5

    # Источник: не хотим показывать пользователю слово "Wikipedia"
    source_name = (normalized.get("source_name") or "").strip()
    if not source_name or "wikipedia" in source_name.lower():
        source_name = "EYYE • AI-подборка"

    card: Dict[str, Any] = {
        "title": norm_title,
        "body": norm_body,
        "tags": tags,
        "importance_score": importance,
        "language": "en" if lang == "en" else "ru",
        "is_active": True,
        "source_type": "wikipedia",
        "source_ref": url,
        "meta": {"source_name": source_name},
    }

    log.info(
        "Prepared Wikipedia card: title=%r, source_name=%r, tags=%r",
        card["title"],
        source_name,
        tags,
    )
    return card


def _insert_cards(cards: List[Dict[str, Any]]) -> None:
    """
    Вставляем карточки в Supabase пачками.
    """
    if not cards:
        return

    total = len(cards)
    idx = 0
    while idx < total:
        batch = cards[idx : idx + SUPABASE_INSERT_BATCH_SIZE]
        resp = supabase.table("cards").insert(batch).execute()
        inserted = len(resp.data or [])
        log.info("Inserted %d Wikipedia cards (batch size=%d)", inserted, len(batch))
        idx += SUPABASE_INSERT_BATCH_SIZE


# ==========
# Основной пайплайн
# ==========


def fetch_wikipedia_articles() -> None:
    """
    Основной воркер:
    - по каждому языку (en/ru) берёт список статей (seed + trending),
    - для каждой статьи:
        - строит URL,
        - проверяет, нет ли уже карточки с таким source_ref,
        - тянет текст из Wikipedia,
        - нормализует в формат нашей карточки,
        - добавляет в список для вставки.
    """
    prepared_cards: List[Dict[str, Any]] = []

    for lang in WIKIPEDIA_LANGS:
        log.info("Processing Wikipedia articles for lang=%s", lang)

        titles = _build_titles_for_lang(lang)
        if not titles:
            log.warning("No titles for lang=%s, skipping", lang)
            continue

        for title in titles:
            url = f"https://{lang}.wikipedia.org/wiki/{title}"

            if _card_exists(url):
                # Не дублируем то, что уже есть
                continue

            extract = _fetch_article_extract(lang, title)
            if not extract:
                continue

            try:
                card = _normalize_to_card(lang, title, url, extract)
            except Exception:
                log.exception(
                    "Failed to normalize Wikipedia article %s (%s)",
                    title,
                    lang,
                )
                continue

            if card:
                prepared_cards.append(card)

    if not prepared_cards:
        log.info("No Wikipedia cards prepared on this run")
        return

    _insert_cards(prepared_cards)
    log.info("Wikipedia ingest finished, total cards prepared=%d", len(prepared_cards))


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    main()

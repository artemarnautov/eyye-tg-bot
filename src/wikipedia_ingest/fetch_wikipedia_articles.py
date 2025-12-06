# file: src/wikipedia_ingest/fetch_wikipedia_articles.py
"""
Wikipedia ingest worker для EYYE.

Логика v2.0:
- для каждого языка (en, ru) берём топовые статьи по просмотрам за последние дни
  через Wikimedia Pageviews API;
- для каждой статьи тянем summary через REST API Wikipedia;
- прогоняем summary через normalize_telegram_post, чтобы получить
  title/body/tags/importance_score в нашей общей схеме;
- фильтруем по тегам под наши топики (как в TikTok-лайт персонализации);
- тянем HTML статьи и выдёргиваем внешние источники (домены ссылок);
- в cards.meta.source_name пишем реальные источники (BBC, The Guardian и т.п.),
  а не "Wikipedia" — сама Википедия хранится в meta.via = "Wikipedia";
- сохраняем карточки в таблицу cards с source_type = "wikipedia";
- /api/feed автоматически начнёт подмешивать их вместе с Telegram-карточками.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import quote, urlparse
from datetime import datetime, timedelta, timezone

import requests
from supabase import create_client, Client
from dotenv import load_dotenv

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Подтягиваем .env
load_dotenv()

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
# Конфиг Wikipedia-инжестора
# ==========

# Языки, которые обрабатываем
# Пример: "en,ru"
WIKIPEDIA_LANGS = os.getenv("WIKIPEDIA_LANGS", "en,ru").split(",")
WIKIPEDIA_LANGS = [lang.strip() for lang in WIKIPEDIA_LANGS if lang.strip()]

# На сколько дней назад смотреть статистику просмотров
# (по умолчанию — вчера)
WIKIPEDIA_DAYS_BACK = int(os.getenv("WIKIPEDIA_DAYS_BACK", "1"))

# Максимум статей на язык за один запуск
WIKIPEDIA_MAX_PER_LANG = int(os.getenv("WIKIPEDIA_MAX_PER_LANG", "5"))

# Разрешённые теги — фильтр, чтобы в фид попадали только релевантные
# нашему продукту топики
ALLOWED_TAGS = {
    "world_news",
    "business",
    "tech",
    "entertainment",
    "society",
    "uk_students",
    "politics",
    "education",
    "science",
    "russia",
    "movies",
    "finance",
    "careers",
}


# ==========
# Вспомогательные функции
# ==========


def _fetch_trending_titles(lang: str, max_count: int) -> List[str]:
    """
    Берём топовые статьи по просмотрам за день (по умолчанию — вчера)
    через Wikimedia Pageviews API.

    Документация:
    https://wikitech.wikimedia.org/wiki/Analytics/AQS/Pageviews#Top_articles
    """
    project = f"{lang}.wikipedia"
    today = datetime.now(timezone.utc).date()
    day = today - timedelta(days=WIKIPEDIA_DAYS_BACK)

    url = (
        "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
        f"{project}/all-access/{day.year}/{day.month:02d}/{day.day:02d}"
    )

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Failed to fetch trending articles for %s: %s", lang, exc)
        return []

    data = resp.json()
    items = data.get("items") or []
    if not items:
        return []

    articles = items[0].get("articles") or []
    titles: List[str] = []

    for a in articles:
        title = a.get("article")
        if not isinstance(title, str):
            continue
        # Отсекаем системные и служебные страницы
        if title in ("Main_Page",):
            continue
        if ":" in title:  # Special:, Category:, etc.
            continue

        titles.append(title.replace("_", " "))
        if len(titles) >= max_count:
            break

    log.info("Trending for %s: %r", lang, titles)
    return titles


def _fetch_summary(lang: str, title: str) -> Dict[str, Any]:
    """
    Тянем краткое содержание статьи через REST API Wikipedia:
    /page/summary/{title}
    """
    base = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/"
    url = base + quote(title.replace(" ", "_"))
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch summary for {lang}:{title}: {resp.status_code}"
        )
    return resp.json()


def _fetch_external_sources(lang: str, title: str, max_sources: int = 5) -> List[str]:
    """
    Тянем HTML статьи и выдёргиваем внешние ссылки (References) — берём домены.
    Это даёт нам реальные источники (BBC, The Guardian, ...), а не "Wikipedia".
    """
    html_url = (
        f"https://{lang}.wikipedia.org/api/rest_v1/page/html/"
        f"{quote(title.replace(' ', '_'))}"
    )

    try:
        resp = requests.get(html_url, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Failed to fetch HTML for sources %s:%s: %s", lang, title, exc)
        return []

    from html.parser import HTMLParser

    class ExternalLinkParser(HTMLParser):
        def __init__(self) -> None:
            super().__init__()
            self.domains: List[str] = []

        def handle_starttag(self, tag: str, attrs):
            if tag != "a":
                return

            attrs_dict = dict(attrs)
            href = attrs_dict.get("href")
            if not href:
                return
            if not href.startswith("http"):
                return

            # Ищем внешние ссылки из референсов
            rel = attrs_dict.get("rel", "")
            css_class = attrs_dict.get("class", "")

            if "nofollow" not in rel and "external" not in css_class:
                return

            netloc = urlparse(href).netloc
            if not netloc:
                return
            if netloc.endswith(".wikipedia.org"):
                return
            if netloc.startswith("www."):
                netloc = netloc[4:]

            if netloc not in self.domains:
                self.domains.append(netloc)

    parser = ExternalLinkParser()
    parser.feed(resp.text)
    return parser.domains[:max_sources]


def _card_exists(url: str) -> bool:
    """
    Проверяем, есть ли уже карточка с таким source_ref и source_type = 'wikipedia'.
    Чтобы не плодить дубли при повторных запусках воркера.
    """
    resp = (
        supabase.table("cards")
        .select("id")
        .eq("source_type", "wikipedia")
        .eq("source_ref", url)
        .limit(1)
        .execute()
    )
    exists = bool(resp.data)
    if exists:
        log.info("Card for %s already exists, skipping", url)
    return exists


# ==========
# Основной пайплайн
# ==========


def fetch_wikipedia_articles() -> None:
    """
    Основной воркер:
    - по каждому языку берём трендовые статьи,
    - нормализуем в наш формат,
    - фильтруем по тегам,
    - сохраняем в cards.
    """
    all_cards: List[Dict[str, Any]] = []

    for lang in WIKIPEDIA_LANGS:
        lang = lang.strip()
        if not lang:
            continue

        log.info("Processing Wikipedia articles for lang=%s", lang)

        trending_titles = _fetch_trending_titles(lang, WIKIPEDIA_MAX_PER_LANG)
        if not trending_titles:
            log.warning("No trending titles for lang=%s, skipping", lang)
            continue

        for title in trending_titles:
            # 1) summary
            try:
                summary_json = _fetch_summary(lang, title)
            except Exception as exc:
                log.warning(
                    "Failed to fetch summary for %s:%s: %s", lang, title, exc
                )
                continue

            url = summary_json.get("content_urls", {}).get("desktop", {}).get("page")
            if not url:
                url = f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"

            # 2) дидуп по source_ref
            if _card_exists(url):
                continue

            extract = (
                summary_json.get("extract") or summary_json.get("description") or ""
            )
            if not extract or len(extract.strip()) < 40:
                log.info("Summary too short for %s, skipping", url)
                continue

            channel_title = f"Wikipedia ({lang})"

            # 3) нормализация через OpenAI (та же логика, что у Telegram-постов)
            try:
                normalized = normalize_telegram_post(
                    raw_text=extract,
                    channel_title=channel_title,
                    language=lang,
                )
            except Exception as exc:
                log.exception(
                    "Failed to normalize Wikipedia article %s (%s): %s",
                    title,
                    lang,
                    exc,
                )
                continue

            tags = normalized.get("tags") or []
            tags_norm = [str(t).strip().lower() for t in tags if str(t).strip()]

            # 4) фильтрация по нашим топикам (TikTok-лайт логика)
            if ALLOWED_TAGS and not any(t in ALLOWED_TAGS for t in tags_norm):
                log.info(
                    "Skipping article %s (%s) due to unrelated tags %r",
                    title,
                    lang,
                    tags_norm,
                )
                continue

            # 5) реальные источники из статьи
            external_sources = _fetch_external_sources(lang, title, max_sources=5)
            if external_sources:
                source_name = ", ".join(external_sources[:3])
            else:
                # если совсем ничего нет — всё равно ставим что-то
                source_name = f"Wikipedia {lang.upper()}"

            meta = {
                "source_name": source_name,      # это увидит пользователь
                "sources": external_sources,     # список доменов
                "via": "Wikipedia",              # внутренняя пометка
            }

            card_payload = {
                "title": normalized.get("title")
                or summary_json.get("title")
                or title,
                "body": normalized.get("body") or extract,
                "tags": tags_norm,
                "importance_score": float(
                    normalized.get("importance_score", 0.6)
                ),
                "language": lang,
                "is_active": True,
                "source_type": "wikipedia",
                "source_ref": url,
                "meta": meta,
            }

            log.info(
                "Prepared Wikipedia card: title=%r, source_name=%r, tags=%r",
                card_payload["title"],
                source_name,
                card_payload["tags"],
            )
            all_cards.append(card_payload)

    if not all_cards:
        log.info("No Wikipedia cards prepared on this run")
        return

    # 6) Вставка в cards одним батчем
    inserted = _insert_cards_into_db(
        supabase,
        all_cards,
        language=None,         # язык берём из каждой карточки
        source_type="wikipedia",
        fallback_source_name=None,
        source_ref=None,
    )
    log.info("Inserted %d Wikipedia cards", len(inserted))


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    # CLI-режим:
    # PYTHONPATH=src python -m wikipedia_ingest.fetch_wikipedia_articles
    main()

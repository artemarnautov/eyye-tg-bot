# file: src/wikipedia_ingest/fetch_wikipedia_articles.py
import os
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# Сначала подтягиваем .env, чтобы SUPABASE_*/OPENAI_API_KEY были видны
load_dotenv()

from webapp_backend.openai_client import normalize_telegram_post

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Supabase
# ==========

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL or SUPABASE_KEY is not set. Check your .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==========
# Базовые топики (канонические теги EYYE)
# ==========

ALLOWED_TOPIC_TAGS: List[str] = [
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
]

ALLOWED_TOPIC_TAGS_SET = set(ALLOWED_TOPIC_TAGS)

TAG_SYNONYMS: Dict[str, str] = {
    # синонимы из старых схем и возможных ответов модели
    "uk_students": "education",
    "students": "education",
    "student": "education",
    "careers": "education",
    "career": "education",
    "jobs": "education",

    "movies": "entertainment",
    "movie": "entertainment",
    "film": "entertainment",
    "films": "entertainment",
    "tv": "entertainment",
    "television": "entertainment",
    "series": "entertainment",
    "cinema": "entertainment",

    "crypto": "finance",
    "cryptocurrency": "finance",
    "cryptocurrencies": "finance",
    "economy": "finance",
    "markets": "finance",
    "stock_market": "finance",

    "ai": "tech",
    "it": "tech",
    "software": "tech",
    "internet": "tech",

    "war": "world_news",
    "geopolitics": "world_news",
    "russia": "world_news",
    "ukraine": "world_news",
    "usa": "world_news",
    "europe": "world_news",
    "news": "world_news",

    "health": "lifestyle",
    "wellness": "lifestyle",
    "nutrition": "lifestyle",

    "games": "gaming",
    "esports": "gaming",

    "education": "education",
    "university": "education",
    "universities": "education",
    "school": "education",
    "schools": "education",

    "sport": "sports",
    "football": "sports",
    "soccer": "sports",
    "basketball": "sports",
    "tennis": "sports",
}

# ==========
# Конфиг Wikipedia / Wikimedia
# ==========

# Языки можно переопределить через .env: WIKIPEDIA_LANGS=en,ru,de
WIKIPEDIA_LANGS_ENV = os.getenv("WIKIPEDIA_LANGS", "en,ru")
WIKIPEDIA_LANGS: List[str] = [
    lang.strip() for lang in WIKIPEDIA_LANGS_ENV.split(",") if lang.strip()
]
if not WIKIPEDIA_LANGS:
    WIKIPEDIA_LANGS = ["en", "ru"]

# Seed-статьи под каждый из базовых топиков
WIKIPEDIA_SEED_ARTICLES: Dict[str, List[str]] = {
    "en": [
        "Artificial_intelligence",              # tech / science
        "Startup_company",                      # business
        "Cryptocurrency",                       # finance
        "Video_game",                           # gaming
        "Association_football",                 # sports
        "Lifestyle_(sociology)",                # lifestyle
        "Universities_in_the_United_Kingdom",   # education / world_news
        "Streaming_media",                      # entertainment / tech
        "Climate_change",                       # science / society / world_news
        "World_politics",                       # politics / world_news
        "History_of_Europe",                    # history / world_news
    ],
    "ru": [
        "Искусственный_интеллект",
        "Стартап",
        "Криптовалюта",
        "Видеоигра",
        "Футбол",
        "Образ_жизни",
        "Система_образования_Великобритании",
        "Потоковое_мультимедиа",
        "Изменение_климата",
        "Мировая_политика",
        "История_Европы",
    ],
}

# Fallback-теги для seed-статей — только из ALLOWED_TOPIC_TAGS
SEED_TITLE_TAGS: Dict[tuple, List[str]] = {
    ("en", "Artificial_intelligence"): ["tech", "science"],
    ("en", "Startup_company"): ["business"],
    ("en", "Cryptocurrency"): ["finance", "tech"],
    ("en", "Video_game"): ["gaming", "entertainment"],
    ("en", "Association_football"): ["sports"],
    ("en", "Lifestyle_(sociology)"): ["lifestyle", "society"],
    ("en", "Universities_in_the_United_Kingdom"): ["education", "world_news"],
    ("en", "Streaming_media"): ["entertainment", "tech"],
    ("en", "Climate_change"): ["science", "world_news", "society"],
    ("en", "World_politics"): ["politics", "world_news"],
    ("en", "History_of_Europe"): ["history", "world_news"],

    ("ru", "Искусственный_интеллект"): ["tech", "science"],
    ("ru", "Стартап"): ["business"],
    ("ru", "Криптовалюта"): ["finance", "tech"],
    ("ru", "Видеоигра"): ["gaming", "entertainment"],
    ("ru", "Футбол"): ["sports"],
    ("ru", "Образ_жизни"): ["lifestyle", "society"],
    ("ru", "Система_образования_Великобритании"): ["education", "world_news"],
    ("ru", "Потоковое_мультимедиа"): ["entertainment", "tech"],
    ("ru", "Изменение_климата"): ["science", "world_news", "society"],
    ("ru", "Мировая_политика"): ["politics", "world_news"],
    ("ru", "История_Европы"): ["history", "world_news"],
}

# Проекты для Wikimedia API
WIKIMEDIA_PROJECTS: Dict[str, str] = {
    "en": "en.wikipedia.org",
    "ru": "ru.wikipedia.org",
}

# Объём и режимы забора

# Сколько уникальных trending-страниц на язык мы агрегируем
# (после склейки топов за несколько дней)
WIKIPEDIA_TRENDING_TITLES_PER_LANG = int(
    os.getenv("WIKIPEDIA_TRENDING_TITLES_PER_LANG", "600")
)

# За сколько дней назад берём trending (агрегация просмотров)
WIKIPEDIA_TRENDING_DAYS = int(
    os.getenv("WIKIPEDIA_TRENDING_DAYS", "7")
)

# Режим работы:
# - "bulk": разово добираем общее количество wiki-карт до WIKIPEDIA_BULK_TARGET_TOTAL
# - "daily": обычный режим, до 50 карт на каждый топик за запуск
WIKIPEDIA_INGEST_MODE = os.getenv("WIKIPEDIA_INGEST_MODE", "daily").lower()

# Цель для bulk-запуска (сколько wiki-карточек в сумме хотим иметь)
WIKIPEDIA_BULK_TARGET_TOTAL = int(
    os.getenv("WIKIPEDIA_BULK_TARGET_TOTAL", "1000")
)

# Лимит карточек на один топик за один daily-запуск
WIKIPEDIA_PER_TOPIC_DAILY_LIMIT = int(
    os.getenv("WIKIPEDIA_PER_TOPIC_DAILY_LIMIT", "50")
)

# Общий лимит карточек за один daily-запуск (по умолчанию 13 * 50 = 650)
WIKIPEDIA_MAX_CARDS_PER_RUN = int(
    os.getenv(
        "WIKIPEDIA_MAX_CARDS_PER_RUN",
        str(len(ALLOWED_TOPIC_TAGS) * WIKIPEDIA_PER_TOPIC_DAILY_LIMIT),
    )
)

WIKIMEDIA_USER_AGENT = os.getenv(
    "WIKIMEDIA_USER_AGENT",
    "EYYE-MVP/0.1 (https://github.com/artemarnautov/eyye-tg-bot; contact: dev@eyye.local)",
)

WIKIMEDIA_TOP_URL_TEMPLATE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
    "{project}/all-access/{year}/{month}/{day}"
)

WIKIPEDIA_API_URL_TEMPLATE = "https://{lang}.wikipedia.org/w/api.php"

# Сколько карточек максимально вставляем одним батчем
SUPABASE_INSERT_BATCH_SIZE = int(os.getenv("WIKIPEDIA_INSERT_BATCH_SIZE", "50"))

# ==========
# Вспомогательные функции
# ==========


def _normalize_tags(raw_tags: List[Any]) -> List[str]:
    """
    Приводим теги к каноническому списку ALLOWED_TOPIC_TAGS.
    - мапим через TAG_SYNONYMS,
    - добавляем простые эвристики,
    - всегда возвращаем 1+ тег (fallback: world_news).
    """
    result: List[str] = []

    for t in raw_tags or []:
        key = str(t or "").strip().lower()
        if not key:
            continue

        tag_id: Optional[str] = None

        if key in ALLOWED_TOPIC_TAGS_SET:
            tag_id = key
        elif key in TAG_SYNONYMS:
            tag_id = TAG_SYNONYMS[key]
        else:
            # Простые эвристики по подстрокам
            if "crypto" in key or "биткоин" in key or "крипто" in key:
                tag_id = "finance"
            elif "blockchain" in key:
                tag_id = "finance"
            elif "ai" in key or "искусственный интеллект" in key:
                tag_id = "tech"
            elif "game" in key or "игр" in key:
                tag_id = "gaming"
            elif "sport" in key or "спорт" in key or "league" in key:
                tag_id = "sports"
            elif "university" in key or "университет" in key or "образование" in key:
                tag_id = "education"
            elif "history" in key or "история" in key:
                tag_id = "history"
            elif "climate" in key or "климат" in key:
                tag_id = "science"
            elif "politic" in key or "политик" in key:
                tag_id = "politics"
            elif "film" in key or "movie" in key or "cinema" in key or "сериал" in key:
                tag_id = "entertainment"

        if tag_id and tag_id in ALLOWED_TOPIC_TAGS_SET and tag_id not in result:
            result.append(tag_id)

    if not result:
        # если вообще не удалось определить – считаем, что это мир/новости
        result = ["world_news"]

    return result


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
    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    data = data or []
    return len(data) > 0


def _count_existing_wikipedia_cards() -> int:
    """
    Считаем, сколько wiki-карточек уже есть в БД.
    """
    try:
        resp = (
            supabase.table("cards")
            .select("id", count="exact")
            .eq("source_type", "wikipedia")
            .execute()
        )
    except Exception as e:
        log.warning("Failed to count existing wikipedia cards: %s", e)
        return 0

    cnt = getattr(resp, "count", None)
    if isinstance(cnt, int):
        return cnt

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    data = data or []
    return len(data)


def _fetch_trending_for_lang(lang: str) -> List[Dict[str, Any]]:
    """
    Берём топовые статьи за несколько последних дней из Wikimedia Pageviews API.
    Агрегируем просмотры по title, сортируем по сумме views.

    Возвращаем список dict:
    {
      "title": str,
      "views": int,
      "rank": int,
      "is_seed": bool (False)
    }
    """
    project = WIKIMEDIA_PROJECTS.get(lang)
    if not project:
        log.warning("No Wikimedia project configured for lang=%s", lang)
        return []

    aggregated: Dict[str, Dict[str, Any]] = {}
    today = datetime.utcnow()

    for offset in range(1, WIKIPEDIA_TRENDING_DAYS + 1):
        day = today - timedelta(days=offset)
        url = WIKIMEDIA_TOP_URL_TEMPLATE.format(
            project=project,
            year=day.year,
            month=f"{day.month:02d}",
            day=f"{day.day:02d}",
        )

        headers = {
            "User-Agent": WIKIMEDIA_USER_AGENT,
            "accept": "application/json",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            log.warning(
                "Failed to fetch trending articles for %s (offset=%d): %s",
                lang,
                offset,
                e,
            )
            continue

        data = resp.json() or {}
        items = data.get("items") or []
        if not items:
            continue

        first_item = items[0] or {}
        articles = first_item.get("articles") or []

        for art in articles:
            title = art.get("article")
            if not isinstance(title, str):
                continue

            # служебные страницы не берём
            if title.startswith("Special:") or title.startswith("Main_Page"):
                continue

            views = int(art.get("views") or 0)
            rank = int(art.get("rank") or 9999)

            rec = aggregated.get(title)
            if rec is None:
                aggregated[title] = {
                    "title": title,
                    "views": views,
                    "rank": rank,
                    "is_seed": False,
                }
            else:
                rec["views"] += views
                if rank < rec["rank"]:
                    rec["rank"] = rank

    if not aggregated:
        log.warning("No trending articles aggregated for lang=%s", lang)
        return []

    articles = list(aggregated.values())
    # сортируем по просмотрам (desc), при равенстве — по лучшему рангу (asc)
    articles.sort(key=lambda a: (-a["views"], a["rank"]))

    if len(articles) > WIKIPEDIA_TRENDING_TITLES_PER_LANG:
        articles = articles[:WIKIPEDIA_TRENDING_TITLES_PER_LANG]

    log.info(
        "Aggregated %d trending articles for lang=%s over %d days",
        len(articles),
        lang,
        WIKIPEDIA_TRENDING_DAYS,
    )
    return articles


def _build_articles_for_lang(lang: str) -> List[Dict[str, Any]]:
    """
    Собираем список кандидатных статей для конкретного языка:
    - seed-статьи (под все базовые топики),
    - trending (агрегированные по нескольким дням).
    Формат элемента:
    {
      "title": str,
      "views": int,
      "rank": int,
      "is_seed": bool,
    }
    """
    articles: List[Dict[str, Any]] = []

    seed_titles = WIKIPEDIA_SEED_ARTICLES.get(lang, [])
    for idx, title in enumerate(seed_titles):
        articles.append(
            {
                "title": title,
                "views": 0,
                "rank": 1000 + idx,
                "is_seed": True,
            }
        )

    trending = _fetch_trending_for_lang(lang)
    title_set = {a["title"] for a in articles}
    for art in trending:
        if art["title"] in title_set:
            continue
        articles.append(art)
        title_set.add(art["title"])

    return articles


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


def _load_global_topic_demand() -> Dict[str, float]:
    """
    Смотрим таблицу user_topic_weights и агрегируем веса по тегам.
    Возвращаем нормализованные значения [0..1] по тегам.
    Это глобальный "вектор интересов" пользователей, которым мы подстраиваем wiki-ингест.
    """
    try:
        resp = supabase.table("user_topic_weights").select("tag,weight").execute()
    except Exception as e:
        log.warning("Failed to load user_topic_weights for wiki ingest: %s", e)
        return {}

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    data = data or []

    demand_raw: Dict[str, float] = {}
    for row in data:
        tag = str(row.get("tag") or "").strip()
        if not tag:
            continue
        try:
            w = float(row.get("weight") or 0.0)
        except Exception:
            w = 0.0
        demand_raw[tag] = demand_raw.get(tag, 0.0) + w

    if not demand_raw:
        return {}

    max_val = max(demand_raw.values())
    if max_val <= 0:
        return {}

    demand_norm = {tag: val / max_val for tag, val in demand_raw.items()}
    log.info("Loaded global topic demand for %d tags", len(demand_norm))
    return demand_norm


def _normalize_to_card(
    lang: str,
    title: str,
    url: str,
    extract: str,
    *,
    popularity_score: float,
    global_topic_demand: Dict[str, float],
) -> Optional[Dict[str, Any]]:
    """
    Прогоняем текст Вики через уже существующий normalize_telegram_post,
    чтобы получить title/body/tags/importance_score.

    Доп. логика:
    - приводим теги к каноническим топикам EYYE;
    - усиливаем importance_score в зависимости от:
        * глобального спроса по этим тегам (user_topic_weights),
        * популярности wiki-страницы (pageviews);
    - в meta сохраняем вспомогательную информацию.
    """
    normalized = normalize_telegram_post(
        raw_text=extract,
        channel_title=f"Wikipedia ({lang})",
        language=lang,
    )

    raw_tags = normalized.get("tags") or []
    if not isinstance(raw_tags, list):
        raw_tags = [raw_tags]

    tags = _normalize_tags(raw_tags)

    # 🔹 fallback-теги для seed-страниц, если вдруг ничего не вышло
    if not tags:
        tags = SEED_TITLE_TAGS.get((lang, title), ["world_news"])

    # Заголовок / тело
    norm_title = (normalized.get("title") or "").strip()
    if not norm_title:
        norm_title = title.replace("_", " ")

    norm_body = (normalized.get("body") or "").strip()
    if not norm_body:
        norm_body = extract[:800]

    # Базовая важность от модели
    try:
        base_importance = float(normalized.get("importance_score", 0.7))
    except Exception:
        base_importance = 0.7

    # Спрос по тегам: максимум по всем тегам карточки в глобальном векторе
    topic_demand_score = 0.0
    for t in tags:
        topic_demand_score = max(topic_demand_score, float(global_topic_demand.get(t, 0.0)))

    # Популярность статьи: уже нормализованная [0..1]
    popularity_score = max(0.0, min(float(popularity_score), 1.0))

    # Усиливаем важность:
    # - фактор по популярности: 0.6..1.3
    # - фактор по спросу:      0.7..1.3
    importance = base_importance
    importance *= 0.6 + 0.7 * popularity_score
    importance *= 0.7 + 0.6 * topic_demand_score
    # лёгкие границы
    if importance < 0.2:
        importance = 0.2
    if importance > 3.0:
        importance = 3.0

    # Источник: не хотим показывать пользователю слово "Wikipedia"
    source_name = (normalized.get("source_name") or "").strip()
    if not source_name or "wikipedia" in source_name.lower():
        source_name = "EYYE • AI-подборка"

    lang_code = "en" if lang == "en" else "ru"

    meta: Dict[str, Any] = {
        "source_name": source_name,
        "wiki_lang": lang,
        "wiki_title": title,
        "wiki_url": url,
        "wiki_popularity": popularity_score,
        "wiki_topic_demand": topic_demand_score,
    }

    card: Dict[str, Any] = {
        "title": norm_title,
        "body": norm_body,
        "tags": tags,
        "importance_score": importance,
        "language": lang_code,
        "is_active": True,
        "source_type": "wikipedia",
        "source_ref": url,
        "meta": meta,
    }

    log.info(
        "Prepared Wikipedia card: title=%r, tags=%r, importance=%.3f, pop=%.2f, demand=%.2f",
        card["title"],
        tags,
        importance,
        popularity_score,
        topic_demand_score,
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
        inserted = len(getattr(resp, "data", None) or getattr(resp, "model", None) or [])
        log.info("Inserted %d Wikipedia cards (batch size=%d)", inserted, len(batch))
        idx += SUPABASE_INSERT_BATCH_SIZE


def _select_cards_with_topic_limits(
    cards: List[Dict[str, Any]],
    max_total: int,
    per_topic_limit: int,
) -> List[Dict[str, Any]]:
    """
    Выбираем карточки с учётом:
    - общего лимита max_total;
    - лимита per_topic_limit на каждый тег из ALLOWED_TOPIC_TAGS.
    """
    topic_counts: Dict[str, int] = {t: 0 for t in ALLOWED_TOPIC_TAGS}
    selected: List[Dict[str, Any]] = []

    for card in cards:
        tags = card.get("tags") or []
        if not isinstance(tags, list):
            tags = [tags]

        canonical_tags = [t for t in tags if t in ALLOWED_TOPIC_TAGS_SET]
        if not canonical_tags:
            canonical_tags = ["world_news"]

        # если по всем тегам карточки лимит уже выбран — пропускаем
        if all(topic_counts.get(t, 0) >= per_topic_limit for t in canonical_tags):
            continue

        selected.append(card)

        for t in canonical_tags:
            if topic_counts.get(t, 0) < per_topic_limit:
                topic_counts[t] = topic_counts.get(t, 0) + 1

        if len(selected) >= max_total:
            break

    log.info("Topic quotas after selection: %s", topic_counts)
    return selected


# ==========
# Основной пайплайн
# ==========


def fetch_wikipedia_articles() -> None:
    """
    Основной воркер:
    - подтягивает глобальный вектор интересов пользователей (user_topic_weights),
    - по каждому языку (en/ru) берёт список статей (seed + trending за несколько дней),
    - для каждой статьи:
        - строит URL,
        - проверяет, нет ли уже карточки с таким source_ref,
        - тянет текст из Wikipedia,
        - нормализует в формат нашей карточки с учётом популярности и спроса,
    - сортирует карточки по importance_score,
    - в режиме "bulk" добирает общее число wiki-карт до ~WIKIPEDIA_BULK_TARGET_TOTAL,
    - в режиме "daily" вставляет до ~50 карточек на каждый топик (пересечение по тегам).
    """
    global_topic_demand = _load_global_topic_demand()
    if not global_topic_demand:
        log.info("Global topic demand is empty, wiki ingest will use content-based scoring only")

    prepared_cards: List[Dict[str, Any]] = []

    for lang in WIKIPEDIA_LANGS:
        log.info("Processing Wikipedia articles for lang=%s", lang)

        articles = _build_articles_for_lang(lang)
        if not articles:
            log.warning("No candidate articles for lang=%s, skipping", lang)
            continue

        max_views = max((a["views"] for a in articles), default=0)
        if max_views <= 0:
            max_views = 1

        for art in articles:
            title = art["title"]
            url = f"https://{lang}.wikipedia.org/wiki/{title}"

            if _card_exists(url):
                # Не дублируем то, что уже есть
                continue

            extract = _fetch_article_extract(lang, title)
            if not extract:
                continue

            # Популярность: для seed с views=0 даём умеренное значение,
            # для trending — нормализуем по максимуму просмотров.
            if art["views"] > 0:
                popularity_score = min(1.0, art["views"] / float(max_views))
            elif art.get("is_seed"):
                popularity_score = 0.5
            else:
                popularity_score = 0.2

            try:
                card = _normalize_to_card(
                    lang=lang,
                    title=title,
                    url=url,
                    extract=extract,
                    popularity_score=popularity_score,
                    global_topic_demand=global_topic_demand,
                )
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

    # Сортируем по importance_score (учитывает и популярность, и интересы)
    prepared_cards.sort(
        key=lambda c: float(c.get("importance_score") or 0.0),
        reverse=True,
    )

    existing_total = _count_existing_wikipedia_cards()
    log.info(
        "Existing wikipedia cards in DB: %d (mode=%s)",
        existing_total,
        WIKIPEDIA_INGEST_MODE,
    )

    # --- Режим bulk: добираем до WIKIPEDIA_BULK_TARGET_TOTAL --- #
    if WIKIPEDIA_INGEST_MODE == "bulk":
        bulk_target = WIKIPEDIA_BULK_TARGET_TOTAL if WIKIPEDIA_BULK_TARGET_TOTAL > 0 else 1000
        remaining = bulk_target - existing_total
        if remaining <= 0:
            log.info(
                "Bulk target already reached (target=%d, existing=%d). Nothing to do.",
                bulk_target,
                existing_total,
            )
            return

        max_total = min(remaining, len(prepared_cards))
        selected_cards = prepared_cards[:max_total]

        log.info(
            "Bulk mode: target=%d, existing=%d, this_run=%d",
            bulk_target,
            existing_total,
            len(selected_cards),
        )
        _insert_cards(selected_cards)
        log.info("Wikipedia bulk ingest finished, total cards inserted=%d", len(selected_cards))
        return

    # --- Обычный daily-режим --- #
    # Тут мы хотим примерно "до 50 карточек на каждый топик" за запуск.
    max_total_daily = min(WIKIPEDIA_MAX_CARDS_PER_RUN, len(prepared_cards))
    selected_cards = _select_cards_with_topic_limits(
        prepared_cards,
        max_total=max_total_daily,
        per_topic_limit=WIKIPEDIA_PER_TOPIC_DAILY_LIMIT,
    )

    if not selected_cards:
        log.info("No wikipedia cards selected after topic limits")
        return

    _insert_cards(selected_cards)
    log.info(
        "Wikipedia daily ingest finished, total cards inserted=%d (max_per_topic=%d, max_total=%d)",
        len(selected_cards),
        WIKIPEDIA_PER_TOPIC_DAILY_LIMIT,
        max_total_daily,
    )


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    main()

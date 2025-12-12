import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

from webapp_backend.openai_client import normalize_wikipedia_article, is_configured

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
# Топики (канонические теги EYYE)
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

WIKIPEDIA_LANGS_ENV = os.getenv("WIKIPEDIA_LANGS", "en,ru")
WIKIPEDIA_LANGS: List[str] = [lang.strip() for lang in WIKIPEDIA_LANGS_ENV.split(",") if lang.strip()]
if not WIKIPEDIA_LANGS:
    WIKIPEDIA_LANGS = ["en", "ru"]

WIKIMEDIA_PROJECTS: Dict[str, str] = {
    "en": "en.wikipedia.org",
    "ru": "ru.wikipedia.org",
}

WIKIMEDIA_USER_AGENT = os.getenv(
    "WIKIMEDIA_USER_AGENT",
    "EYYE-MVP/0.2 (https://github.com/artemarnautov/eyye-tg-bot; contact: dev@eyye.local)",
)

WIKIMEDIA_TOP_URL_TEMPLATE = (
    "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/"
    "{project}/all-access/{year}/{month}/{day}"
)
WIKIPEDIA_API_URL_TEMPLATE = "https://{lang}.wikipedia.org/w/api.php"

# Режимы:
# bulk  - разово/массово (НЕ для hourly)
# daily - умеренно
# hourly - маленький бюджет + строгий why_now
WIKIPEDIA_INGEST_MODE = os.getenv("WIKIPEDIA_INGEST_MODE", "hourly").lower()

# ====== Лимиты для hourly (по умолчанию) ======
WIKIPEDIA_HOURLY_MAX_CARDS_PER_RUN = int(os.getenv("WIKIPEDIA_HOURLY_MAX_CARDS_PER_RUN", "12"))
WIKIPEDIA_HOURLY_MAX_OPENAI_CALLS_PER_RUN = int(os.getenv("WIKIPEDIA_HOURLY_MAX_OPENAI_CALLS_PER_RUN", "20"))
WIKIPEDIA_HOURLY_PER_TOPIC_LIMIT = int(os.getenv("WIKIPEDIA_HOURLY_PER_TOPIC_LIMIT", "2"))

# ====== Кандидаты ======
WIKIPEDIA_RECENTCHANGES_WINDOW_MINUTES = int(os.getenv("WIKIPEDIA_RECENTCHANGES_WINDOW_MINUTES", "240"))
WIKIPEDIA_RECENTCHANGES_LIMIT = int(os.getenv("WIKIPEDIA_RECENTCHANGES_LIMIT", "80"))

WIKIPEDIA_TRENDING_DAYS = int(os.getenv("WIKIPEDIA_TRENDING_DAYS", "2"))  # для hourly разумно 1-2
WIKIPEDIA_TRENDING_TITLES_PER_LANG = int(os.getenv("WIKIPEDIA_TRENDING_TITLES_PER_LANG", "200"))

# Минимальная длина extract, чтобы не было мусора
WIKIPEDIA_MIN_EXTRACT_CHARS = int(os.getenv("WIKIPEDIA_MIN_EXTRACT_CHARS", "450"))
WIKIPEDIA_EXTRACT_CHARS = int(os.getenv("WIKIPEDIA_EXTRACT_CHARS", "2200"))

# Батчи
SUPABASE_INSERT_BATCH_SIZE = int(os.getenv("WIKIPEDIA_INSERT_BATCH_SIZE", "50"))
WIKIPEDIA_FETCH_BULK_TITLES = int(os.getenv("WIKIPEDIA_FETCH_BULK_TITLES", "20"))

# Блоклист по заголовкам (можно переопределить)
DEFAULT_TITLE_BLOCKLIST_REGEX = (
    r"(^Список_|^List_of_)|(\(значения\)$)|(^Категория:)|(^Портал:)|(^Portal:)|(^Category:)"
)
WIKIPEDIA_TITLE_BLOCKLIST_REGEX = os.getenv("WIKIPEDIA_TITLE_BLOCKLIST_REGEX", DEFAULT_TITLE_BLOCKLIST_REGEX)
TITLE_BLOCK_RE = None
try:
    TITLE_BLOCK_RE = __import__("re").compile(WIKIPEDIA_TITLE_BLOCKLIST_REGEX, __import__("re").IGNORECASE)
except Exception:
    TITLE_BLOCK_RE = None


# ==========
# Helpers
# ==========

def _normalize_tags(raw_tags: List[Any]) -> List[str]:
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
        result = ["world_news"]

    return result


def _build_url(lang: str, title: str) -> str:
    safe_title = (title or "").replace(" ", "_")
    return f"https://{lang}.wikipedia.org/wiki/{safe_title}"


def _is_bad_title(title: str) -> bool:
    if not title:
        return True

    # Служебные страницы
    if title.startswith("Special:") or title.startswith("Main_Page"):
        return True

    if TITLE_BLOCK_RE and TITLE_BLOCK_RE.search(title):
        return True

    # Слишком "пустые" названия
    t = title.strip()
    if len(t) < 2:
        return True

    return False


def _load_global_topic_demand() -> Dict[str, float]:
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


def _fetch_existing_source_refs(urls: List[str]) -> set:
    """
    Чтобы НЕ делать 500 запросов вида 'eq(source_ref)=...' — одним запросом вытягиваем что уже есть.
    """
    if not urls:
        return set()

    existing: set = set()
    # PostgREST ограничивает размер URL, так что бьём на чанки
    CHUNK = 60
    for i in range(0, len(urls), CHUNK):
        chunk = urls[i:i + CHUNK]
        try:
            resp = (
                supabase.table("cards")
                .select("source_ref")
                .eq("source_type", "wikipedia")
                .in_("source_ref", chunk)
                .execute()
            )
            rows = getattr(resp, "data", None) or getattr(resp, "model", None) or []
            for r in rows:
                sr = r.get("source_ref")
                if sr:
                    existing.add(sr)
        except Exception as e:
            log.warning("Failed to prefetch existing wiki refs: %s", e)

    return existing


def _insert_cards(cards: List[Dict[str, Any]]) -> None:
    if not cards:
        return

    total = len(cards)
    idx = 0
    while idx < total:
        batch = cards[idx: idx + SUPABASE_INSERT_BATCH_SIZE]
        resp = supabase.table("cards").insert(batch).execute()
        inserted = len(getattr(resp, "data", None) or getattr(resp, "model", None) or [])
        log.info("Inserted %d Wikipedia cards (batch size=%d)", inserted, len(batch))
        idx += SUPABASE_INSERT_BATCH_SIZE


def _select_cards_with_topic_limits(
    cards: List[Dict[str, Any]],
    max_total: int,
    per_topic_limit: int,
) -> List[Dict[str, Any]]:
    topic_counts: Dict[str, int] = {t: 0 for t in ALLOWED_TOPIC_TAGS}
    selected: List[Dict[str, Any]] = []

    for card in cards:
        tags = card.get("tags") or []
        if not isinstance(tags, list):
            tags = [tags]

        canonical_tags = [t for t in tags if t in ALLOWED_TOPIC_TAGS_SET]
        if not canonical_tags:
            canonical_tags = ["world_news"]

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
# Candidate sources: RecentChanges + Trending
# ==========

def _fetch_recent_changes(lang: str) -> List[Dict[str, Any]]:
    api_url = WIKIPEDIA_API_URL_TEMPLATE.format(lang=lang)
    headers = {"User-Agent": WIKIMEDIA_USER_AGENT}

    params = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "list": "recentchanges",
        "rcnamespace": 0,  # main/articles
        "rctype": "edit|new",
        "rcprop": "title|ids|timestamp|comment|flags|sizes",
        "rclimit": WIKIPEDIA_RECENTCHANGES_LIMIT,
        "rcshow": "!bot|!minor|!redirect",
    }

    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch recentchanges for lang=%s: %s", lang, e)
        return []

    data = resp.json() or {}
    rcs = (data.get("query") or {}).get("recentchanges") or []
    if not isinstance(rcs, list):
        return []

    # фильтр по окну времени
    now = datetime.now(timezone.utc)
    min_ts = now - timedelta(minutes=WIKIPEDIA_RECENTCHANGES_WINDOW_MINUTES)

    out: List[Dict[str, Any]] = []
    for rc in rcs:
        title = str(rc.get("title") or "").strip()
        if _is_bad_title(title):
            continue

        ts = str(rc.get("timestamp") or "").strip()
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            dt = None

        if dt and dt < min_ts:
            continue

        out.append(
            {
                "title": title,
                "source": "recentchanges",
                "rc_timestamp": ts,
                "rc_comment": (rc.get("comment") or "")[:200],
                "rc_type": "new" if rc.get("type") == "new" else "edit",
                "score_hint": 0.90,  # сильный why_now сигнал
            }
        )

    return out


def _fetch_trending_for_lang(lang: str) -> List[Dict[str, Any]]:
    project = WIKIMEDIA_PROJECTS.get(lang)
    if not project:
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
        headers = {"User-Agent": WIKIMEDIA_USER_AGENT, "accept": "application/json"}

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            log.warning("Failed to fetch trending for %s (offset=%d): %s", lang, offset, e)
            continue

        data = resp.json() or {}
        items = data.get("items") or []
        if not items:
            continue

        articles = (items[0] or {}).get("articles") or []
        for art in articles:
            title = art.get("article")
            if not isinstance(title, str):
                continue
            if _is_bad_title(title):
                continue

            views = int(art.get("views") or 0)
            rank = int(art.get("rank") or 9999)

            rec = aggregated.get(title)
            if rec is None:
                aggregated[title] = {
                    "title": title,
                    "views": views,
                    "best_rank": rank,
                    "days_seen": 1,
                    "source": "trending",
                }
            else:
                rec["views"] += views
                rec["days_seen"] += 1
                if rank < rec["best_rank"]:
                    rec["best_rank"] = rank

    if not aggregated:
        return []

    articles = list(aggregated.values())
    articles.sort(key=lambda a: (-a["views"], a["best_rank"]))

    if len(articles) > WIKIPEDIA_TRENDING_TITLES_PER_LANG:
        articles = articles[:WIKIPEDIA_TRENDING_TITLES_PER_LANG]

    # Нормализуем score_hint: чем выше (лучше) rank и чем "реже появлялось" (burst), тем выше
    out: List[Dict[str, Any]] = []
    for a in articles:
        best_rank = int(a.get("best_rank") or 9999)
        days_seen = int(a.get("days_seen") or 1)
        rank_score = 1.0 if best_rank <= 30 else (0.8 if best_rank <= 80 else (0.6 if best_rank <= 150 else 0.35))
        burst = 1.0 - min(1.0, days_seen / max(1.0, float(WIKIPEDIA_TRENDING_DAYS)))  # 0..~1
        score_hint = 0.55 * rank_score + 0.45 * burst

        out.append(
            {
                "title": a["title"],
                "source": "trending",
                "views": int(a.get("views") or 0),
                "best_rank": best_rank,
                "days_seen": days_seen,
                "score_hint": float(score_hint),
            }
        )

    return out


def _fetch_articles_bulk(lang: str, titles: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Одним запросом подтягиваем extract + info + ревизию.
    """
    if not titles:
        return {}

    api_url = WIKIPEDIA_API_URL_TEMPLATE.format(lang=lang)
    headers = {"User-Agent": WIKIMEDIA_USER_AGENT}

    params = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "prop": "extracts|info|revisions",
        "explaintext": True,
        "exchars": WIKIPEDIA_EXTRACT_CHARS,
        "redirects": 1,
        "inprop": "url",
        "rvprop": "ids|timestamp",
        "rvlimit": 1,
        "titles": "|".join(titles),
    }

    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=12)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch bulk articles for lang=%s: %s", lang, e)
        return {}

    data = resp.json() or {}
    pages = (data.get("query") or {}).get("pages") or []
    if not isinstance(pages, list):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for p in pages:
        if not isinstance(p, dict):
            continue
        if p.get("missing"):
            continue

        title = str(p.get("title") or "").strip()
        extract = str(p.get("extract") or "").strip()
        fullurl = str(p.get("fullurl") or "").strip()
        pageid = p.get("pageid")
        lastrevid = p.get("lastrevid")
        touched = p.get("touched")

        rev_ts = None
        revs = p.get("revisions") or []
        if isinstance(revs, list) and revs:
            rev_ts = revs[0].get("timestamp")

        if title:
            out[title] = {
                "title": title,
                "extract": extract,
                "fullurl": fullurl,
                "pageid": pageid,
                "lastrevid": lastrevid,
                "touched": touched,
                "rev_ts": rev_ts,
            }

    return out


# ==========
# Main pipeline (hourly)
# ==========

def fetch_wikipedia_articles() -> None:
    """
    Hourly режим:
    - кандидаты: recentchanges + trending
    - жёсткий OpenAI-gate is_newsworthy + why_now
    - маленький бюджет: max_openai_calls + max_cards_per_run
    """
    if not is_configured():
        log.info("OpenAI is not configured -> Wikipedia ingest is disabled in hourly mode (to avoid 'encyclopedia' spam).")
        return

    global_topic_demand = _load_global_topic_demand()

    max_cards = WIKIPEDIA_HOURLY_MAX_CARDS_PER_RUN if WIKIPEDIA_INGEST_MODE == "hourly" else int(
        os.getenv("WIKIPEDIA_MAX_CARDS_PER_RUN", "40")
    )
    max_openai_calls = WIKIPEDIA_HOURLY_MAX_OPENAI_CALLS_PER_RUN if WIKIPEDIA_INGEST_MODE == "hourly" else int(
        os.getenv("WIKIPEDIA_MAX_OPENAI_CALLS_PER_RUN", "60")
    )
    per_topic_limit = WIKIPEDIA_HOURLY_PER_TOPIC_LIMIT if WIKIPEDIA_INGEST_MODE == "hourly" else int(
        os.getenv("WIKIPEDIA_PER_TOPIC_DAILY_LIMIT", "6")
    )

    candidates: List[Dict[str, Any]] = []

    for lang in WIKIPEDIA_LANGS:
        log.info("Collecting candidates for lang=%s", lang)

        # 1) свежие правки (главный источник why_now)
        rc = _fetch_recent_changes(lang)
        candidates.extend([{**x, "lang": lang} for x in rc])

        # 2) тренды по просмотрам (вторичный источник why_now)
        tr = _fetch_trending_for_lang(lang)
        candidates.extend([{**x, "lang": lang} for x in tr])

    if not candidates:
        log.info("No Wikipedia candidates found on this run")
        return

    # Дедуп по (lang,title,source)
    seen_key = set()
    uniq: List[Dict[str, Any]] = []
    for c in candidates:
        key = (c.get("lang"), c.get("title"), c.get("source"))
        if key in seen_key:
            continue
        seen_key.add(key)
        uniq.append(c)
    candidates = uniq

    # Быстрый предфильтр: выкинуть слабые trending (чтобы меньше тратить OpenAI)
    filtered: List[Dict[str, Any]] = []
    for c in candidates:
        if c.get("source") == "trending":
            # в hourly хотим только те, у кого реально есть шанс why_now
            if float(c.get("score_hint") or 0.0) < 0.45:
                continue
        filtered.append(c)
    candidates = filtered

    # Сорт по score_hint + доп эвристика
    def _cand_score(c: Dict[str, Any]) -> float:
        base = float(c.get("score_hint") or 0.0)
        if c.get("source") == "recentchanges":
            base += 0.25
        if c.get("source") == "trending":
            best_rank = int(c.get("best_rank") or 9999)
            if best_rank <= 50:
                base += 0.15
        return base

    candidates.sort(key=_cand_score, reverse=True)

    # Сформируем URLs и одним запросом узнаем, что уже есть
    urls = [_build_url(c["lang"], c["title"]) for c in candidates]
    existing_refs = _fetch_existing_source_refs(urls)
    log.info("Candidates total=%d, already_in_db=%d", len(candidates), len(existing_refs))

    # Ограничим список кандидатов под OpenAI бюджет
    selected_candidates: List[Dict[str, Any]] = []
    for c in candidates:
        url = _build_url(c["lang"], c["title"])
        if url in existing_refs:
            continue
        c["url"] = url
        selected_candidates.append(c)
        if len(selected_candidates) >= max_openai_calls:
            break

    if not selected_candidates:
        log.info("No new candidates (all exist already) -> nothing to do")
        return

    prepared_cards: List[Dict[str, Any]] = []
    openai_calls = 0

    # Обрабатываем батчами: вытягиваем extract пачкой, затем поштучно нормализуем через OpenAI
    for i in range(0, len(selected_candidates), WIKIPEDIA_FETCH_BULK_TITLES):
        batch = selected_candidates[i:i + WIKIPEDIA_FETCH_BULK_TITLES]
        lang_groups: Dict[str, List[str]] = {}
        for c in batch:
            lang_groups.setdefault(c["lang"], []).append(c["title"])

        for lang, titles in lang_groups.items():
            data_map = _fetch_articles_bulk(lang, titles)

            for c in batch:
                if c["lang"] != lang:
                    continue
                if len(prepared_cards) >= max_cards:
                    break
                if openai_calls >= max_openai_calls:
                    break

                title = c["title"]
                url = c["url"]
                page = data_map.get(title)

                if not page:
                    continue

                extract = (page.get("extract") or "").strip()
                if len(extract) < WIKIPEDIA_MIN_EXTRACT_CHARS:
                    continue

                # why_now_hints — ровно то, что модель может использовать, не выдумывая факты
                why_now_hints = {
                    "source": c.get("source"),
                    "recentchange_timestamp": c.get("rc_timestamp"),
                    "recentchange_comment": c.get("rc_comment"),
                    "trending_best_rank": c.get("best_rank"),
                    "trending_days_seen": c.get("days_seen"),
                    "trending_views_agg": c.get("views"),
                    "page_touched": page.get("touched"),
                    "rev_timestamp": page.get("rev_ts"),
                }

                openai_calls += 1
                normalized = normalize_wikipedia_article(
                    extract=extract,
                    wiki_title=title,
                    wiki_url=url,
                    language=lang,
                    why_now_hints=why_now_hints,
                )

                if not normalized or not normalized.get("is_newsworthy"):
                    continue

                raw_tags = normalized.get("tags") or []
                if not isinstance(raw_tags, list):
                    raw_tags = [raw_tags]
                tags = _normalize_tags(raw_tags)

                # importance 0..1 базовая -> усиливаем спросом и (слегка) трендом
                base_importance = float(normalized.get("importance_score") or 0.6)
                if base_importance < 0.0:
                    base_importance = 0.0
                if base_importance > 1.0:
                    base_importance = 1.0

                topic_demand_score = 0.0
                for t in tags:
                    topic_demand_score = max(topic_demand_score, float(global_topic_demand.get(t, 0.0)))

                pop = 0.5
                if c.get("source") == "trending":
                    best_rank = int(c.get("best_rank") or 9999)
                    pop = 0.9 if best_rank <= 30 else (0.75 if best_rank <= 80 else 0.55)

                importance = base_importance
                importance *= (0.75 + 0.35 * pop)           # 0.75..1.10
                importance *= (0.85 + 0.40 * topic_demand_score)  # 0.85..1.25
                # приводим к "нашему" диапазону 0.2..2.2
                importance = max(0.2, min(2.2, importance * 2.0))

                lang_code = lang if lang in ("en", "ru") else "en"

                meta: Dict[str, Any] = {
                    "source_name": "EYYE • Контекст",
                    "wiki_lang": lang,
                    "wiki_title": title,
                    "wiki_url": url,
                    "wiki_pageid": page.get("pageid"),
                    "wiki_lastrevid": page.get("lastrevid"),
                    "wiki_touched": page.get("touched"),
                    "wiki_rev_ts": page.get("rev_ts"),
                    "why_now": normalized.get("why_now"),
                    "why_now_source": c.get("source"),
                    "trending_best_rank": c.get("best_rank"),
                    "trending_days_seen": c.get("days_seen"),
                    "trending_views_agg": c.get("views"),
                    "recentchange_timestamp": c.get("rc_timestamp"),
                    "recentchange_comment": c.get("rc_comment"),
                }

                card: Dict[str, Any] = {
                    "title": (normalized.get("title") or "").strip()[:240] or title.replace("_", " "),
                    "body": (normalized.get("body") or "").strip()[:2400] or extract[:1200],
                    "tags": tags,
                    "importance_score": importance,
                    "language": lang_code,
                    "is_active": True,
                    "source_type": "wikipedia",
                    "source_ref": url,
                    "meta": meta,
                }

                prepared_cards.append(card)

        if len(prepared_cards) >= max_cards or openai_calls >= max_openai_calls:
            break

    if not prepared_cards:
        log.info("No Wikipedia cards prepared on this run (after strict why_now gate)")
        return

    # Сортируем внутри run по importance_score
    prepared_cards.sort(key=lambda c: float(c.get("importance_score") or 0.0), reverse=True)

    # Диверсификация по топикам
    selected_cards = _select_cards_with_topic_limits(
        prepared_cards,
        max_total=max_cards,
        per_topic_limit=per_topic_limit,
    )

    if not selected_cards:
        log.info("No wikipedia cards selected after topic limits")
        return

    _insert_cards(selected_cards)
    log.info(
        "Wikipedia ingest finished: inserted=%d, openai_calls=%d, mode=%s",
        len(selected_cards),
        openai_calls,
        WIKIPEDIA_INGEST_MODE,
    )


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    main()

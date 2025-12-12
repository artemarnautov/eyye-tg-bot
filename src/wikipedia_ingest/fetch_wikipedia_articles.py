import os
import logging
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

from webapp_backend.openai_client import (
    normalize_telegram_post,
    normalize_wikipedia_article,
    is_configured as openai_is_configured,
)

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
# Канонические теги (должны совпадать с openai_client.normalize_telegram_post)
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
    "city",
    "uk_students",
]
ALLOWED_TOPIC_TAGS_SET = set(ALLOWED_TOPIC_TAGS)

TAG_SYNONYMS: Dict[str, str] = {
    "students": "uk_students",
    "student": "uk_students",
    "uk": "uk_students",
    "university": "education",
    "universities": "education",
    "school": "education",
    "schools": "education",
    "career": "education",
    "jobs": "education",
    "movies": "entertainment",
    "movie": "entertainment",
    "film": "entertainment",
    "tv": "entertainment",
    "series": "entertainment",
    "cinema": "entertainment",
    "crypto": "finance",
    "cryptocurrency": "finance",
    "economy": "finance",
    "markets": "finance",
    "stock_market": "finance",
    "ai": "tech",
    "it": "tech",
    "software": "tech",
    "internet": "tech",
    "war": "world_news",
    "geopolitics": "world_news",
    "health": "lifestyle",
    "wellness": "lifestyle",
    "nutrition": "lifestyle",
    "games": "gaming",
    "esports": "gaming",
    "sport": "sports",
    "football": "sports",
    "soccer": "sports",
}

# ==========
# Конфиг Wikipedia
# ==========

WIKIPEDIA_LANGS_ENV = os.getenv("WIKIPEDIA_LANGS", "en,ru")
WIKIPEDIA_LANGS: List[str] = [x.strip() for x in WIKIPEDIA_LANGS_ENV.split(",") if x.strip()] or ["en", "ru"]

WIKIMEDIA_USER_AGENT = os.getenv(
    "WIKIMEDIA_USER_AGENT",
    "EYYE-MVP/0.1 (https://github.com/artemarnautov/eyye-tg-bot; contact: dev@eyye.local)",
)

# Режимы:
# - hourly: берём "mostread" за сегодня/вчера (строгий why_now), вставляем только НОВЫЕ страницы.
# - daily: берём несколько дней mostread, больше охват.
# - bulk: добираем общий корпус (для холодного старта).
WIKIPEDIA_INGEST_MODE = os.getenv("WIKIPEDIA_INGEST_MODE", "hourly").lower()

# Небольшой бюджет: максимум сколько новых карточек на язык за запуск
WIKIPEDIA_MAX_NEW_PER_LANG = int(os.getenv("WIKIPEDIA_MAX_NEW_PER_LANG", "6"))

# Сколько дней mostread брать:
# hourly: 2 (сегодня+вчера), daily: 7
WIKIPEDIA_MOSTREAD_DAYS = int(os.getenv("WIKIPEDIA_MOSTREAD_DAYS", "2"))

# Сколько символов extract тащим
WIKIPEDIA_EXTRACT_CHARS = int(os.getenv("WIKIPEDIA_EXTRACT_CHARS", "1400"))

# Лимит LLM-нормализаций за запуск (всего, не на язык)
WIKIPEDIA_LLM_MAX_PER_RUN = int(os.getenv("WIKIPEDIA_LLM_MAX_PER_RUN", "10"))

# В hourly режиме мы хотим строгий why_now — без LLM “фантазий”.
# Поэтому why_now строим rule-based и передаём как hint в normalize_wikipedia_article.
WIKIPEDIA_USE_WIKI_NORMALIZER = os.getenv("WIKIPEDIA_USE_WIKI_NORMALIZER", "true").lower() in ("1", "true", "yes")

# Batch insert
SUPABASE_INSERT_BATCH_SIZE = int(os.getenv("WIKIPEDIA_INSERT_BATCH_SIZE", "50"))

# Wikipedia REST featured feed (mostread)
WIKI_FEATURED_URL = "https://{lang}.wikipedia.org/api/rest_v1/feed/featured/{yyyy}/{mm}/{dd}"
WIKI_API_URL = "https://{lang}.wikipedia.org/w/api.php"


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
            # простые эвристики
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


def _card_exists(source_ref: str) -> bool:
    resp = (
        supabase.table("cards")
        .select("id")
        .eq("source_type", "wikipedia")
        .eq("source_ref", source_ref)
        .limit(1)
        .execute()
    )
    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
    return len(data) > 0


def _count_existing_wikipedia_cards() -> int:
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
    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
    return len(data)


def _load_global_topic_demand() -> Dict[str, float]:
    """
    Глобальный спрос = сумма weight по всем пользователям (user_topic_weights).
    Нормируем в [0..1].
    """
    try:
        resp = supabase.table("user_topic_weights").select("tag,weight").execute()
    except Exception as e:
        log.warning("Failed to load user_topic_weights for wiki ingest: %s", e)
        return {}

    data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
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
    return {tag: val / max_val for tag, val in demand_raw.items()}


def _fetch_mostread(lang: str, day: date) -> List[Dict[str, Any]]:
    """
    Возвращает список mostread:
    [{title(str, с пробелами), views(int), rank(int=позиция), day(YYYY-MM-DD)}]
    """
    url = WIKI_FEATURED_URL.format(lang=lang, yyyy=day.year, mm=f"{day.month:02d}", dd=f"{day.day:02d}")
    headers = {"User-Agent": WIKIMEDIA_USER_AGENT, "accept": "application/json"}

    try:
        r = requests.get(url, headers=headers, timeout=12)
        r.raise_for_status()
        data = r.json() or {}
    except Exception as e:
        log.warning("Failed to fetch featured/mostread for %s %s: %s", lang, day.isoformat(), e)
        return []

    mostread = (data.get("mostread") or {})
    articles = mostread.get("articles") or []
    out: List[Dict[str, Any]] = []
    for idx, a in enumerate(articles, start=1):
        title = a.get("title")
        if not isinstance(title, str) or not title.strip():
            continue
        # отсекаем служебные штуки
        if title.startswith("Special:") or title.startswith("Main Page") or title.startswith("Main_Page"):
            continue
        views = int(a.get("views") or 0)
        out.append({"title": title.strip(), "views": views, "rank": idx, "day": day.isoformat()})
    return out


def _fetch_article_extract(lang: str, title: str) -> Optional[str]:
    api_url = WIKI_API_URL.format(lang=lang)
    headers = {"User-Agent": WIKIMEDIA_USER_AGENT}
    params = {
        "action": "query",
        "format": "json",
        "prop": "extracts",
        "explaintext": True,
        "exchars": WIKIPEDIA_EXTRACT_CHARS,
        "redirects": 1,
        "titles": title,
    }
    try:
        resp = requests.get(api_url, headers=headers, params=params, timeout=12)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Failed to fetch Wikipedia extract '%s' (lang=%s): %s", title, lang, e)
        return None

    data = resp.json() or {}
    pages = data.get("query", {}).get("pages", {})
    if not pages:
        return None
    page = next(iter(pages.values()))
    extract = page.get("extract")
    if not extract or not str(extract).strip():
        return None
    return str(extract)


def _build_wiki_url(lang: str, title: str) -> str:
    # title в featured feed идёт с пробелами => делаем /wiki/Title_with_underscores
    normalized = title.replace(" ", "_")
    return f"https://{lang}.wikipedia.org/wiki/{quote(normalized)}"


def _strict_why_now(
    *,
    lang: str,
    day: str,
    rank: int,
    views_today: int,
    views_yesterday: Optional[int],
) -> str:
    """
    Строгий why_now: только из метрик mostread.
    """
    base = f"В топе самых читаемых Wikipedia ({lang}) за {day}: место #{rank}."
    if views_yesterday is not None and views_yesterday > 0:
        ratio = views_today / float(views_yesterday)
        if ratio >= 1.5:
            base += f" Рост интереса ~x{ratio:.1f} к вчера."
    return base


def _insert_cards(cards: List[Dict[str, Any]]) -> None:
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


def fetch_wikipedia_articles() -> None:
    global_topic_demand = _load_global_topic_demand()
    if not global_topic_demand:
        log.info("Global topic demand empty -> scoring only by popularity/LLM importance")

    now = datetime.now(timezone.utc).date()

    # hourly: today+ вчера
    # daily: N дней назад
    days: List[date] = []
    if WIKIPEDIA_INGEST_MODE == "hourly":
        days = [now, now - timedelta(days=1)]
    else:
        n = max(1, WIKIPEDIA_MOSTREAD_DAYS)
        days = [(now - timedelta(days=i)) for i in range(0, n)]

    llm_calls_left = WIKIPEDIA_LLM_MAX_PER_RUN if openai_is_configured() else 0
    prepared_cards: List[Dict[str, Any]] = []

    for lang in WIKIPEDIA_LANGS:
        log.info("Wikipedia ingest lang=%s mode=%s days=%s", lang, WIKIPEDIA_INGEST_MODE, [d.isoformat() for d in days])

        # mostread today/yesterday -> для why_now и velocity
        mostread_by_day: Dict[str, List[Dict[str, Any]]] = {}
        for d in days:
            mostread_by_day[d.isoformat()] = _fetch_mostread(lang, d)

        if not any(mostread_by_day.values()):
            log.warning("No mostread data for lang=%s -> skip", lang)
            continue

        # индекс views по title для yesterday (для growth)
        yesterday_iso = (now - timedelta(days=1)).isoformat()
        y_index: Dict[str, int] = {}
        for row in mostread_by_day.get(yesterday_iso, []):
            y_index[row["title"]] = int(row.get("views") or 0)

        # берём кандидатов из "самого свежего дня" (today, если пусто -> yesterday)
        primary_day = now.isoformat()
        primary_rows = mostread_by_day.get(primary_day) or mostread_by_day.get(yesterday_iso) or []
        if not primary_rows:
            continue

        # ограничение по бюджету: берём только top-K новых на язык
        new_for_lang = 0
        for row in primary_rows:
            if new_for_lang >= WIKIPEDIA_MAX_NEW_PER_LANG:
                break

            title = row["title"]
            views_today = int(row.get("views") or 0)
            rank = int(row.get("rank") or 9999)
            day_iso = str(row.get("day") or primary_day)

            url = _build_wiki_url(lang, title)
            if _card_exists(url):
                continue

            extract = _fetch_article_extract(lang, title)
            if not extract:
                continue

            views_yesterday = y_index.get(title)
            why_now = _strict_why_now(
                lang=lang,
                day=day_iso,
                rank=rank,
                views_today=views_today,
                views_yesterday=views_yesterday,
            )

            # popularity_score 0..1 по месту в топе (простая, дешёвая)
            # top-50 => ~1..0
            popularity_score = max(0.1, min(1.0, (60 - min(rank, 60)) / 60.0))

            # demand score: мы ещё не знаем теги -> берём позже после нормализации,
            # но popularity даст baseline importance.
            base_importance = 0.55 + 0.35 * popularity_score  # 0.55..0.90

            # --- LLM нормализация (ограничена budget) ---
            normalized: Dict[str, Any] = {}
            if llm_calls_left > 0:
                try:
                    if WIKIPEDIA_USE_WIKI_NORMALIZER:
                        normalized = normalize_wikipedia_article(
                            title_hint=title,
                            raw_text=extract,
                            language=lang,
                            why_now=why_now,
                        )
                    else:
                        normalized = normalize_telegram_post(
                            raw_text=extract,
                            channel_title=f"Wikipedia ({lang})",
                            language=lang,
                        )
                    llm_calls_left -= 1
                except Exception:
                    log.exception("Failed to normalize wiki article via LLM: %s (%s)", title, lang)
                    normalized = {}
            else:
                # супердешёвый fallback без LLM
                normalized = {
                    "title": title,
                    "body": (extract or "").strip()[:1600],
                    "tags": [],
                    "importance_score": base_importance,
                    "language": lang,
                    "source_name": None,
                    "why_now": why_now,
                }

            raw_tags = normalized.get("tags") or []
            if not isinstance(raw_tags, list):
                raw_tags = [raw_tags]
            tags = _normalize_tags(raw_tags)

            # demand по тегам
            demand = 0.0
            for t in tags:
                demand = max(demand, float(global_topic_demand.get(t, 0.0)))

            # итоговая importance: LLM importance (если есть) + popularity + demand
            try:
                llm_importance = float(normalized.get("importance_score", base_importance))
            except Exception:
                llm_importance = base_importance

            importance = llm_importance
            importance *= (0.75 + 0.5 * popularity_score)  # 0.75..1.25
            importance *= (0.80 + 0.6 * demand)           # 0.80..1.40
            importance = max(0.2, min(3.0, importance))

            # display source_name — НЕ пишем Wikipedia пользователю, но для внутреннего ключа оставим wiki_* в meta
            source_name = (normalized.get("source_name") or "").strip()
            if not source_name:
                source_name = "EYYE • AI-подборка"

            # why_now: строгое, берём из normalized только если это wiki-normalizer (он обязан повторить hint)
            why_now_final = str(normalized.get("why_now") or "").strip() or why_now

            card = {
                "title": (normalized.get("title") or title).strip()[:240],
                "body": (normalized.get("body") or extract).strip()[:2800],
                "tags": tags,
                "importance_score": importance,
                "language": "en" if lang == "en" else "ru",
                "is_active": True,
                "source_type": "wikipedia",
                "source_ref": url,
                "meta": {
                    "source_name": source_name,
                    "wiki_lang": lang,
                    "wiki_title": title,
                    "wiki_url": url,
                    "wiki_day": day_iso,
                    "wiki_rank": rank,
                    "wiki_views": views_today,
                    "why_now": why_now_final,
                    "popularity_score": popularity_score,
                    "topic_demand": demand,
                },
            }

            prepared_cards.append(card)
            new_for_lang += 1

        log.info("Prepared %d new wiki cards for lang=%s (LLM remaining=%d)", new_for_lang, lang, llm_calls_left)

    if not prepared_cards:
        log.info("No Wikipedia cards prepared on this run")
        return

    prepared_cards.sort(key=lambda c: float(c.get("importance_score") or 0.0), reverse=True)

    existing_total = _count_existing_wikipedia_cards()
    log.info("Existing wikipedia cards in DB: %d (mode=%s)", existing_total, WIKIPEDIA_INGEST_MODE)

    _insert_cards(prepared_cards)
    log.info("Wikipedia ingest finished, inserted=%d", len(prepared_cards))


def main() -> None:
    fetch_wikipedia_articles()


if __name__ == "__main__":
    main()

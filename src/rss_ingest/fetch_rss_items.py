# file: src/rss_ingest/fetch_rss_items.py
import os
import re
import json
import time
import hashlib
import logging
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Iterable, Set
from email.utils import parsedate_to_datetime

import httpx
from dotenv import load_dotenv
from supabase import create_client, Client

from webapp_backend.openai_client import (
    call_openai_chat,
    _extract_message_content,
    _try_loose_json_parse,
    _normalize_tag_list,
    _clean_text,
    _clamp01,
)

load_dotenv()

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

RSS_SOURCES_FILE = os.getenv("RSS_SOURCES_FILE", "rss_sources.txt")
RSS_FEEDS = os.getenv("RSS_FEEDS", "").strip()  # comma-separated, если не хочешь файл
RSS_FETCH_LIMIT_PER_FEED = int(os.getenv("RSS_FETCH_LIMIT_PER_FEED", "30"))
RSS_DEFAULT_LANGUAGE = os.getenv("RSS_DEFAULT_LANGUAGE", "en")

RSS_TITLE_DEDUP_HOURS = int(os.getenv("RSS_TITLE_DEDUP_HOURS", "168"))  # 7 дней
RSS_BATCH_SIZE = int(os.getenv("RSS_OPENAI_BATCH_SIZE", "12"))

# Батч-проверки в Supabase (ключевой фикс)
RSS_SUPABASE_IN_BATCH = int(os.getenv("RSS_SUPABASE_IN_BATCH", "60"))
RSS_PREFETCH_TITLE_FP_LIMIT = int(os.getenv("RSS_PREFETCH_TITLE_FP_LIMIT", "5000"))

# Сеть
RSS_HTTP_TIMEOUT = float(os.getenv("RSS_HTTP_TIMEOUT", "15"))
RSS_HTTP_CONNECT_TIMEOUT = float(os.getenv("RSS_HTTP_CONNECT_TIMEOUT", "5"))
RSS_HTTP_RETRIES = int(os.getenv("RSS_HTTP_RETRIES", "2"))
RSS_HTTP_RETRY_SLEEP_SEC = float(os.getenv("RSS_HTTP_RETRY_SLEEP_SEC", "0.6"))

# Google News RSS “как поиск”
RSS_ENABLE_GOOGLE_NEWS = os.getenv("RSS_ENABLE_GOOGLE_NEWS", "true").lower() in ("1", "true", "yes")
GOOGLE_NEWS_HL = os.getenv("GOOGLE_NEWS_HL", "en-AE")
GOOGLE_NEWS_GL = os.getenv("GOOGLE_NEWS_GL", "AE")
GOOGLE_NEWS_CEID = os.getenv("GOOGLE_NEWS_CEID", "AE:en")

# какие “темы” гоняем как запросы (можешь расширять)
GOOGLE_NEWS_QUERIES = os.getenv(
    "GOOGLE_NEWS_QUERIES",
    "UAE, Dubai, Abu Dhabi, Middle East, MENA, AI, startups, business, finance, tech",
)

ALLOWED_TAGS_CANONICAL = [
    "world_news", "business", "finance", "tech", "science", "history", "politics", "society",
    "entertainment", "gaming", "sports", "lifestyle", "education", "city", "uk_students",
]


def _chunks(xs: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(xs), n):
        yield xs[i:i + n]


def _normalize_title_for_fp(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[\s\.\,\!\?\:\;\-–—]+", " ", t)
    t = " ".join(t.split())
    return t[:220]


def _title_fp(title: str) -> str:
    nt = _normalize_title_for_fp(title)
    if not nt:
        return ""
    return hashlib.sha1(nt.encode("utf-8")).hexdigest()[:16]


def _read_feed_list() -> List[str]:
    if RSS_FEEDS:
        return [f.strip() for f in RSS_FEEDS.split(",") if f.strip()]

    path = os.path.join(os.getcwd(), RSS_SOURCES_FILE)
    if not os.path.exists(path):
        log.warning("RSS sources file not found: %s (set RSS_FEEDS or create rss_sources.txt)", path)
        return []

    feeds: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            feeds.append(line)
    return feeds


def _google_news_rss_url(query: str) -> str:
    q = urllib.parse.quote_plus(query.strip())
    return (
        "https://news.google.com/rss/search?q=" + q +
        f"&hl={urllib.parse.quote_plus(GOOGLE_NEWS_HL)}" +
        f"&gl={urllib.parse.quote_plus(GOOGLE_NEWS_GL)}" +
        f"&ceid={urllib.parse.quote_plus(GOOGLE_NEWS_CEID)}"
    )


def _fetch_xml(url: str) -> str:
    """
    Надёжный фетч: httpx + таймауты + пару ретраев.
    """
    headers = {"User-Agent": "EYYE-Ingest/1.0"}
    timeout = httpx.Timeout(
        RSS_HTTP_TIMEOUT,
        connect=RSS_HTTP_CONNECT_TIMEOUT,
        read=RSS_HTTP_TIMEOUT,
        write=RSS_HTTP_TIMEOUT,
        pool=RSS_HTTP_TIMEOUT,
    )

    last_err: Optional[Exception] = None
    for attempt in range(RSS_HTTP_RETRIES + 1):
        try:
            with httpx.Client(headers=headers, timeout=timeout, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.text
        except Exception as e:
            last_err = e
            if attempt < RSS_HTTP_RETRIES:
                time.sleep(RSS_HTTP_RETRY_SLEEP_SEC * (attempt + 1))
                continue
            break

    raise RuntimeError(f"Failed to fetch RSS url={url!r}: {last_err}")


def _parse_rss_or_atom(xml_text: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Возвращает (feed_title, items[])
    item: {title, url, summary, published_at_raw}
    """
    root = ET.fromstring(xml_text)

    # RSS 2.0
    channel = root.find("channel")
    if channel is not None:
        feed_title = (channel.findtext("title") or "").strip() or "RSS"
        items: List[Dict[str, Any]] = []
        for it in channel.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            desc = (it.findtext("description") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()
            items.append({"title": title, "url": link, "summary": desc, "published_at_raw": pub})
        return feed_title, items

    # Atom
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    feed_title = (root.findtext("atom:title", default="", namespaces=ns) or "").strip() or "Atom"
    items: List[Dict[str, Any]] = []
    for e in root.findall("atom:entry", ns):
        title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("atom:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        summ = (e.findtext("atom:summary", default="", namespaces=ns) or "").strip()
        updated = (e.findtext("atom:updated", default="", namespaces=ns) or "").strip()
        items.append({"title": title, "url": link.strip(), "summary": summ, "published_at_raw": updated})
    return feed_title, items


def _parse_datetime_fuzzy(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None

    # RSS pubDate обычно RFC 2822
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass

    # Atom updated часто ISO
    if "T" in s and ("+" in s or s.endswith("Z")):
        return s

    return None


def _prefetch_existing_source_refs(urls: List[str]) -> Set[str]:
    """
    Вместо 1 запроса на 1 url — батчами .in_()
    """
    existing: Set[str] = set()
    if not urls:
        return existing

    for batch in _chunks(urls, RSS_SUPABASE_IN_BATCH):
        try:
            resp = (
                supabase.table("cards")
                .select("source_ref")
                .eq("source_type", "rss")
                .in_("source_ref", batch)
                .execute()
            )
            for row in (resp.data or []):
                sr = (row.get("source_ref") or "").strip()
                if sr:
                    existing.add(sr)
        except Exception:
            log.exception("Failed batch lookup for source_ref (batch size=%d)", len(batch))

    return existing


def _prefetch_recent_title_fps() -> Set[str]:
    """
    Достаём title_fp за окно дедупа одним проходом.
    Это грубый, но быстрый MVP-фикс вместо contains(meta, {"title_fp": ...}) на каждый айтем.
    """
    since = (datetime.now(timezone.utc) - timedelta(hours=RSS_TITLE_DEDUP_HOURS)).isoformat()
    fps: Set[str] = set()

    try:
        resp = (
            supabase.table("cards")
            .select("meta")
            .eq("source_type", "rss")
            .gte("created_at", since)
            .limit(RSS_PREFETCH_TITLE_FP_LIMIT)
            .execute()
        )
        for row in (resp.data or []):
            meta = row.get("meta") or {}
            if isinstance(meta, dict):
                fp = meta.get("title_fp")
                if isinstance(fp, str) and fp:
                    fps.add(fp)
    except Exception:
        log.exception("Failed prefetch recent title_fps")

    return fps


def _openai_normalize_batch(items: List[Dict[str, Any]], language: str) -> List[Dict[str, Any]]:
    """
    items input: [{id,title,summary,url,source_name}]
    returns: [{id,title,body,tags,importance_score,language,quality}]
    """
    if not items:
        return []

    system_prompt = (
        "Ты нормализуешь новости для ленты EYYE.\n"
        "На вход дается список элементов (title+summary+url+source).\n"
        "Верни валидный JSON {\"items\": [...]}.\n"
        "Правила:\n"
        "1) НЕ выдумывай факты и детали.\n"
        "2) title: одно короткое нейтральное предложение.\n"
        "3) body: 2–4 абзаца по 1–3 предложения, без эмодзи.\n"
        "4) tags: 1–6 тегов только из allowlist.\n"
        "5) importance_score: 0..1\n"
        "6) language: строго '" + language + "'\n"
        "Allowlist tags:\n" + ", ".join(ALLOWED_TAGS_CANONICAL)
    )

    user_payload = {"language": language, "items": items}
    payload = {
        "model": OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        "max_output_tokens": 1400,
        "temperature": 0.25,
        "response_format": {"type": "json_object"},
    }

    resp = call_openai_chat(payload)
    if not resp:
        return []

    content = (_extract_message_content(resp) or "").strip()
    if not content:
        return []

    try:
        parsed = json.loads(content)
    except Exception:
        parsed = _try_loose_json_parse(content)

    if not isinstance(parsed, dict):
        return []

    out = parsed.get("items") or []
    if not isinstance(out, list):
        return []

    results: List[Dict[str, Any]] = []
    for it in out:
        if not isinstance(it, dict):
            continue
        rid = it.get("id")
        title = _clean_text(it.get("title"), 220)
        body = _clean_text(it.get("body") or it.get("summary"), 2600)
        if not rid or not title or not body:
            continue
        tags = _normalize_tag_list(it.get("tags"), fallback=[])
        imp = _clamp01(it.get("importance_score", 0.6))
        results.append({
            "id": rid,
            "title": title,
            "body": body,
            "tags": tags,
            "importance_score": imp,
            "language": language,
            "quality": "ok",
        })
    return results


def _insert_rss_card(
    norm: Dict[str, Any],
    raw: Dict[str, Any],
    recent_title_fps: Set[str],
    existing_source_refs: Set[str],
) -> Optional[int]:
    """
    Вставка + локальные множества для дедупа, чтобы не долбить Supabase lookup'ами.
    """
    source_ref = (raw.get("url") or "").strip()
    if not source_ref:
        return None

    # дедуп по source_ref (самый точный)
    if source_ref in existing_source_refs:
        return None

    title = norm.get("title") or ""
    fp = _title_fp(title)
    if not fp:
        return None

    # дедуп по title_fp (грубый, но быстрый)
    if fp in recent_title_fps:
        return None

    published_at = _parse_datetime_fuzzy(raw.get("published_at_raw") or "")

    meta = {
        "source_name": raw.get("source_name"),
        "feed_title": raw.get("feed_title"),
        "feed_url": raw.get("feed_url"),
        "title_fp": fp,
        "quality": norm.get("quality", "ok"),
        "ingest": "rss",
        "published_at": published_at,
    }

    payload = {
        "title": title,
        "body": norm.get("body"),
        "tags": norm.get("tags") or [],
        "importance_score": float(norm.get("importance_score", 0.6)),
        "language": norm.get("language") or RSS_DEFAULT_LANGUAGE,
        "is_active": True,
        "source_type": "rss",
        "source_ref": source_ref,
        "meta": meta,
    }

    try:
        resp = supabase.table("cards").insert(payload).execute()
        if not resp.data:
            return None

        new_id = int(resp.data[0]["id"])
        # обновляем локальные кэши (важно, чтобы не пытаться вставить снова в этом же прогоне)
        existing_source_refs.add(source_ref)
        recent_title_fps.add(fp)
        return new_id
    except Exception:
        log.exception("Insert failed for rss card source_ref=%s", source_ref)
        return None


def main() -> None:
    feeds = _read_feed_list()

    if RSS_ENABLE_GOOGLE_NEWS:
        for q in [x.strip() for x in GOOGLE_NEWS_QUERIES.split(",") if x.strip()]:
            feeds.append(_google_news_rss_url(q))

    if not feeds:
        log.warning("No RSS feeds configured. Set RSS_FEEDS or create rss_sources.txt")
        return

    # Единоразово префетчим title_fp за окно дедупа (убираем contains(meta,...) на каждый айтем)
    recent_title_fps = _prefetch_recent_title_fps()

    total_new = 0
    for feed_url in feeds:
        try:
            xml_text = _fetch_xml(feed_url)
            feed_title, items = _parse_rss_or_atom(xml_text)
            if not items:
                continue

            # Сначала соберём ограниченный список items (по RSS_FETCH_LIMIT_PER_FEED)
            limited = items[:RSS_FETCH_LIMIT_PER_FEED]

            # Батч-проверка существующих source_ref
            urls = [(it.get("url") or "").strip() for it in limited if (it.get("url") or "").strip()]
            existing_source_refs = _prefetch_existing_source_refs(urls)

            # Соберем кандидатов (без per-item запросов в Supabase)
            candidates: List[Dict[str, Any]] = []
            local_raw_title_fps: Set[str] = set()

            now_ts = int(time.time())
            for idx, it in enumerate(limited):
                url = (it.get("url") or "").strip()
                title = (it.get("title") or "").strip()
                summary = (it.get("summary") or "").strip()

                if not url or not title:
                    continue

                # быстрый дедуп: по source_ref
                if url in existing_source_refs:
                    continue

                # быстрый локальный дедуп по raw title (чтобы не слать дубли в OpenAI)
                raw_fp = _title_fp(title)
                if raw_fp and raw_fp in local_raw_title_fps:
                    continue
                if raw_fp:
                    local_raw_title_fps.add(raw_fp)

                candidates.append({
                    "id": f"{now_ts}-{idx}",
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "source_name": feed_title,
                    "published_at_raw": it.get("published_at_raw") or "",
                })

            if not candidates:
                continue

            # OpenAI нормализация пачками
            lang = RSS_DEFAULT_LANGUAGE
            inserted_here = 0

            for i in range(0, len(candidates), RSS_BATCH_SIZE):
                batch = candidates[i:i + RSS_BATCH_SIZE]
                normalized = _openai_normalize_batch(batch, language=lang)
                if not normalized:
                    continue

                raw_by_id = {x["id"]: x for x in batch}

                for n in normalized:
                    raw = raw_by_id.get(n["id"])
                    if not raw:
                        continue

                    raw2 = {
                        "url": raw["url"],
                        "source_name": raw.get("source_name"),
                        "feed_title": feed_title,
                        "feed_url": feed_url,
                        "published_at_raw": raw.get("published_at_raw") or "",
                    }

                    card_id = _insert_rss_card(
                        n,
                        raw2,
                        recent_title_fps=recent_title_fps,
                        existing_source_refs=existing_source_refs,
                    )
                    if card_id:
                        total_new += 1
                        inserted_here += 1

            log.info(
                "RSS feed processed: %s (%s) candidates=%d inserted=%d",
                feed_title, feed_url, len(candidates), inserted_here
            )

        except Exception:
            log.exception("Failed processing RSS feed: %s", feed_url)

    log.info("RSS ingest done. Inserted %d cards", total_new)


if __name__ == "__main__":
    main()

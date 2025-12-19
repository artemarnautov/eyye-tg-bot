# file: src/rss_ingest/fetch_rss_items.py
import os
import re
import json
import time
import hashlib
import logging
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

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
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

RSS_SOURCES_FILE = os.getenv("RSS_SOURCES_FILE", "rss_sources.txt")
RSS_FEEDS = (os.getenv("RSS_FEEDS") or "").strip()  # comma-separated
RSS_FETCH_LIMIT_PER_FEED = int(os.getenv("RSS_FETCH_LIMIT_PER_FEED", "30"))

# какой язык писать в cards.language
RSS_DEFAULT_LANGUAGE = os.getenv("RSS_DEFAULT_LANGUAGE") or os.getenv("EYYE_OUTPUT_LANGUAGE") or "en"

RSS_TITLE_DEDUP_HOURS = int(os.getenv("RSS_TITLE_DEDUP_HOURS", "168"))  # 7 дней
RSS_BATCH_SIZE = int(os.getenv("RSS_OPENAI_BATCH_SIZE", "8"))  # меньше = меньше таймаутов

# Google News RSS “как поиск”
RSS_ENABLE_GOOGLE_NEWS = (os.getenv("RSS_ENABLE_GOOGLE_NEWS", "true").lower() in ("1", "true", "yes"))
GOOGLE_NEWS_HL = os.getenv("GOOGLE_NEWS_HL", "en-AE")
GOOGLE_NEWS_GL = os.getenv("GOOGLE_NEWS_GL", "AE")
GOOGLE_NEWS_CEID = os.getenv("GOOGLE_NEWS_CEID", "AE:en")
GOOGLE_NEWS_QUERIES = os.getenv(
    "GOOGLE_NEWS_QUERIES",
    "UAE, Dubai, Abu Dhabi, Middle East, MENA, AI, startups, business, finance, tech"
)

ALLOWED_TAGS_CANONICAL = [
    "world_news","business","finance","tech","science","history","politics","society",
    "entertainment","gaming","sports","lifestyle","education","city","uk_students",
]

def _strip_html(s: str) -> str:
    s = s or ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _canonicalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    try:
        p = urllib.parse.urlsplit(url)
        # убираем фрагмент, приводим к норм виду
        url = urllib.parse.urlunsplit((p.scheme, p.netloc, p.path, p.query, ""))
    except Exception:
        pass
    return url

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

def _key_for_url(url: str) -> str:
    u = _canonicalize_url(url)
    if not u:
        return ""
    return hashlib.sha1(u.encode("utf-8")).hexdigest()[:16]

def _read_feed_list() -> List[str]:
    feeds: List[str] = []
    if RSS_FEEDS:
        feeds = [f.strip() for f in RSS_FEEDS.split(",") if f.strip()]
        return feeds

    path = os.path.join(os.getcwd(), RSS_SOURCES_FILE)
    if not os.path.exists(path):
        log.warning("RSS sources file not found: %s (set RSS_FEEDS or create rss_sources.txt)", path)
        return []

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

def _fetch_xml(url: str, timeout: float = 20.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "EYYE-Ingest/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")

def _parse_rss_or_atom(xml_text: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Returns (feed_title, items[])
    item: {title, url, summary, published_at_raw}
    """
    root = ET.fromstring(xml_text)

    channel = root.find("channel")
    if channel is not None:
        feed_title = (channel.findtext("title") or "").strip() or "RSS"
        items: List[Dict[str, Any]] = []
        for it in channel.findall("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            guid = (it.findtext("guid") or "").strip()
            desc = (it.findtext("description") or "").strip()
            pub = (it.findtext("pubDate") or "").strip()

            url = link or guid
            items.append({
                "title": title,
                "url": url,
                "summary": desc,
                "published_at_raw": pub,
            })
        return feed_title, items

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    feed_title = (root.findtext("atom:title", default="", namespaces=ns) or "").strip() or "Atom"

    items: List[Dict[str, Any]] = []
    for e in root.findall("atom:entry", ns):
        title = (e.findtext("atom:title", default="", namespaces=ns) or "").strip()
        link_el = e.find("atom:link", ns)
        link = (link_el.get("href") if link_el is not None else "") or ""
        summ = (e.findtext("atom:summary", default="", namespaces=ns) or "").strip()
        updated = (e.findtext("atom:updated", default="", namespaces=ns) or "").strip()
        items.append({
            "title": title,
            "url": link.strip(),
            "summary": summ,
            "published_at_raw": updated,
        })
    return feed_title, items

def _card_exists_by_source_ref(source_ref: str) -> Optional[int]:
    source_ref = _canonicalize_url(source_ref)
    if not source_ref:
        return None
    try:
        resp = (
            supabase.table("cards")
            .select("id")
            .eq("source_type", "rss")
            .eq("source_ref", source_ref)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if rows:
            return int(rows[0]["id"])
    except Exception:
        log.exception("Failed lookup by source_ref=%r", source_ref)
    return None

def _card_exists_by_title_fp(title_fp: str) -> Optional[int]:
    if not title_fp:
        return None
    since = (datetime.now(timezone.utc) - timedelta(hours=RSS_TITLE_DEDUP_HOURS)).isoformat()
    try:
        resp = (
            supabase.table("cards")
            .select("id")
            .eq("source_type", "rss")
            .gte("created_at", since)
            .contains("meta", {"title_fp": title_fp})
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if rows:
            return int(rows[0]["id"])
    except Exception:
        log.exception("Failed lookup by title_fp=%r", title_fp)
    return None

def _openai_normalize_batch(items: List[Dict[str, Any]], language: str) -> List[Dict[str, Any]]:
    """
    input items: [{key,title,summary,url,source_name}]
    returns: [{key,title,body,tags,importance_score,language,quality}]
    """
    if not items:
        return []

    system_prompt = (
        "Ты нормализуешь новости для ленты EYYE.\n"
        "На вход даётся список элементов (title+summary+url+source+key).\n"
        "Верни валидный JSON строго формата: {\"items\": [ ... ]}.\n"
        "ВАЖНО: поле key верни ТОЧНО таким же, как во входе (не меняй).\n"
        "Правила:\n"
        "1) НЕ выдумывай факты.\n"
        "2) title: одно короткое нейтральное предложение.\n"
        "3) body: 2–4 абзаца по 1–3 предложения, без эмодзи.\n"
        "4) tags: 1–6 тегов только из allowlist.\n"
        "5) importance_score: 0..1\n"
        f"6) language: строго '{language}'\n"
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

    out = parsed.get("items")
    if not isinstance(out, list):
        return []

    results: List[Dict[str, Any]] = []
    for it in out:
        if not isinstance(it, dict):
            continue

        key = str(it.get("key") or "").strip()
        title = _clean_text(it.get("title"), 220)
        body = _clean_text(it.get("body") or it.get("summary"), 2600)
        if not key or not title or not body:
            continue

        tags = _normalize_tag_list(it.get("tags"), fallback=[])
        imp = _clamp01(it.get("importance_score", 0.6))

        results.append({
            "key": key,
            "title": title,
            "body": body,
            "tags": tags,
            "importance_score": imp,
            "language": language,
            "quality": "ok",
        })

    return results

def _insert_rss_card(norm: Dict[str, Any], raw: Dict[str, Any]) -> Optional[int]:
    title = norm.get("title") or ""
    fp = _title_fp(title)
    if not fp:
        return None

    existing_title = _card_exists_by_title_fp(fp)
    if existing_title:
        return existing_title

    source_ref = _canonicalize_url(raw.get("url") or "")
    if not source_ref:
        return None

    meta = {
        "source_name": raw.get("source_name"),
        "feed_title": raw.get("feed_title"),
        "feed_url": raw.get("feed_url"),
        "title_fp": fp,
        "quality": norm.get("quality", "ok"),
        "ingest": "rss",
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

    resp = supabase.table("cards").insert(payload).execute()
    if not resp.data:
        return None
    return int(resp.data[0]["id"])

def main() -> None:
    feeds = _read_feed_list()

    if RSS_ENABLE_GOOGLE_NEWS:
        for q in [x.strip() for x in (GOOGLE_NEWS_QUERIES or "").split(",") if x.strip()]:
            feeds.append(_google_news_rss_url(q))

    if not feeds:
        log.warning("No RSS feeds configured. Set RSS_FEEDS or create rss_sources.txt OR enable Google News")
        return

    total_inserted = 0

    for feed_url in feeds:
        try:
            xml_text = _fetch_xml(feed_url)
            feed_title, items = _parse_rss_or_atom(xml_text)
            if not items:
                continue

            candidates: List[Dict[str, Any]] = []
            for it in items[:RSS_FETCH_LIMIT_PER_FEED]:
                url = _canonicalize_url(it.get("url") or "")
                title = (it.get("title") or "").strip()
                summary = _strip_html(it.get("summary") or "")

                if not url or not title:
                    continue

                if _card_exists_by_source_ref(url):
                    continue

                key = _key_for_url(url)
                if not key:
                    continue

                candidates.append({
                    "key": key,
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "source_name": feed_title,
                })

            if not candidates:
                continue

            lang = RSS_DEFAULT_LANGUAGE

            skipped_no_raw = 0
            inserted = 0
            for i in range(0, len(candidates), RSS_BATCH_SIZE):
                batch = candidates[i:i+RSS_BATCH_SIZE]
                normalized = _openai_normalize_batch(batch, language=lang)

                raw_by_key = {x["key"]: x for x in batch}

                for n in normalized:
                    raw = raw_by_key.get(n["key"])
                    if not raw:
                        skipped_no_raw += 1
                        continue

                    raw2 = {
                        "url": raw["url"],
                        "source_name": raw.get("source_name"),
                        "feed_title": feed_title,
                        "feed_url": feed_url,
                    }

                    card_id = _insert_rss_card(n, raw2)
                    if card_id:
                        inserted += 1
                        total_inserted += 1

            log.info(
                "RSS feed processed: %s (%s) candidates=%d inserted=%d skipped_no_raw=%d",
                feed_title, feed_url, len(candidates), inserted, skipped_no_raw
            )

        except Exception:
            log.exception("Failed processing RSS feed: %s", feed_url)

    log.info("RSS ingest done. Inserted/linked %d cards", total_inserted)

if __name__ == "__main__":
    main()

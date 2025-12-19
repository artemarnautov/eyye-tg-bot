# file: src/rss_ingest/fetch_rss_items.py
import os
import re
import json
import hashlib
import logging
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv
from supabase import create_client, Client

from webapp_backend.openai_client import (
    call_openai_chat,
    _extract_message_content,
    _try_loose_json_parse,
    _normalize_tag_list,
    _clean_text,
    _clamp01,
    get_canonical_topics,
)

load_dotenv()

log = logging.getLogger("rss_ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

RSS_SOURCES_FILE = os.getenv("RSS_SOURCES_FILE", "rss_sources.txt")
RSS_FEEDS = os.getenv("RSS_FEEDS", "").strip()  # comma-separated, optional

RSS_FETCH_LIMIT_PER_FEED = int(os.getenv("RSS_FETCH_LIMIT_PER_FEED", "20"))
RSS_TITLE_DEDUP_HOURS = int(os.getenv("RSS_TITLE_DEDUP_HOURS", "168"))  # 7 дней
RSS_OPENAI_BATCH_SIZE = int(os.getenv("RSS_OPENAI_BATCH_SIZE", os.getenv("RSS_OPENAI_BATCH", "8")))
RSS_FALLBACK_INSERT = os.getenv("RSS_FALLBACK_INSERT", "true").lower() in ("1", "true", "yes")

RSS_DEFAULT_LANGUAGE = os.getenv("RSS_DEFAULT_LANGUAGE") or os.getenv("EYYE_OUTPUT_LANGUAGE") or "en"

# Google News RSS “как поиск”
RSS_ENABLE_GOOGLE_NEWS = os.getenv("RSS_ENABLE_GOOGLE_NEWS", "true").lower() in ("1", "true", "yes")
GOOGLE_NEWS_HL = os.getenv("GOOGLE_NEWS_HL", "en-AE")
GOOGLE_NEWS_GL = os.getenv("GOOGLE_NEWS_GL", "AE")
GOOGLE_NEWS_CEID = os.getenv("GOOGLE_NEWS_CEID", "AE:en")

# NEW: режим генерации queries
GOOGLE_NEWS_QUERIES_MODE = (os.getenv("GOOGLE_NEWS_QUERIES_MODE", "auto") or "auto").strip().lower()
GOOGLE_NEWS_QUERIES = (os.getenv("GOOGLE_NEWS_QUERIES", "AUTO") or "AUTO").strip()  # "AUTO" = автогенерация
GOOGLE_NEWS_MAX_FEEDS = int(os.getenv("GOOGLE_NEWS_MAX_FEEDS", "60"))  # safety cap

GOOGLE_NEWS_LOCAL_HINTS = [
    x.strip() for x in (os.getenv("GOOGLE_NEWS_LOCAL_HINTS", "UAE,Dubai,Abu Dhabi,Middle East,MENA") or "").split(",")
    if x.strip()
]
GOOGLE_NEWS_LOCAL_HINTS_PER_TOPIC = int(os.getenv("GOOGLE_NEWS_LOCAL_HINTS_PER_TOPIC", "2"))

CANONICAL_TOPICS = get_canonical_topics()


# -----------------------------
# Utils
# -----------------------------

def _strip_ns(tag: str) -> str:
    if not tag:
        return ""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag

def _find_child(parent: ET.Element, name: str) -> Optional[ET.Element]:
    for ch in list(parent):
        if _strip_ns(ch.tag) == name:
            return ch
    return None

def _find_children(parent: ET.Element, name: str) -> List[ET.Element]:
    out: List[ET.Element] = []
    for ch in list(parent):
        if _strip_ns(ch.tag) == name:
            out.append(ch)
    return out

def _text(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return (el.text or "").strip()

def _normalize_title_for_fp(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[\s\.\,\!\?\:\;\-–—]+", " ", t)
    t = " ".join(t.split())
    return t[:220]

def _domain_from_url(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower().strip()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc[:120]
    except Exception:
        return ""

def _title_fp(title: str, url: str = "") -> str:
    nt = _normalize_title_for_fp(title)
    if not nt:
        return ""
    dom = _domain_from_url(url)
    base = nt + "|" + (dom or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]

def _stable_item_key(url: str) -> str:
    u = (url or "").strip()
    return hashlib.sha1(u.encode("utf-8")).hexdigest()[:16] if u else ""

def _google_news_rss_url(query: str) -> str:
    q = urllib.parse.quote_plus(query.strip())
    return (
        "https://news.google.com/rss/search?q=" + q +
        f"&hl={urllib.parse.quote_plus(GOOGLE_NEWS_HL)}" +
        f"&gl={urllib.parse.quote_plus(GOOGLE_NEWS_GL)}" +
        f"&ceid={urllib.parse.quote_plus(GOOGLE_NEWS_CEID)}"
    )

def _fetch_xml(url: str, timeout: float = 15.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "EYYE-Ingest/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


# -----------------------------
# rss_sources.txt -> jobs with seed_topic
# -----------------------------

def _read_feed_jobs() -> List[Dict[str, str]]:
    """
    Читает статические RSS фиды.
    Поддерживает секции вида:
      # =========================
      # world_news
      # =========================
      https://...
    Возвращает jobs: [{feed_url, seed_topic}]
    """
    jobs: List[Dict[str, str]] = []

    # RSS_FEEDS env -> просто список
    if RSS_FEEDS:
        for f in [x.strip() for x in RSS_FEEDS.split(",") if x.strip()]:
            jobs.append({"feed_url": f})
        return jobs

    path = os.path.join(os.getcwd(), RSS_SOURCES_FILE)
    if not os.path.exists(path):
        log.warning("RSS sources file not found: %s (set RSS_FEEDS or create rss_sources.txt)", path)
        return []

    current_topic: Optional[str] = None

    def _maybe_set_topic(line: str) -> Optional[str]:
        s = line.strip()
        if not s.startswith("#"):
            return None
        s = s[1:].strip().lower()
        if not s or set(s) <= {"="}:
            return None
        if s in CANONICAL_TOPICS:
            return s
        return None

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue

            if line.startswith("#"):
                maybe = _maybe_set_topic(line)
                if maybe:
                    current_topic = maybe
                continue

            if line.startswith("http://") or line.startswith("https://"):
                jobs.append({
                    "feed_url": line,
                    "seed_topic": current_topic or "unknown",
                })

    return jobs


# -----------------------------
# Topic -> Google News query generator
# -----------------------------

_TOPIC_TO_QUERY = {
    "world_news": ["world news", "top stories"],
    "business": ["business", "startups", "companies"],
    "finance": ["finance", "stock market", "economy"],
    "tech": ["technology", "AI", "cybersecurity"],
    "science": ["science", "space", "climate"],
    "history": ["history", "archaeology"],
    "politics": ["politics", "government"],
    "society": ["society", "migration", "human rights"],
    "entertainment": ["entertainment", "movies", "music"],
    "gaming": ["gaming", "video games"],
    "sports": ["sports", "football"],
    "lifestyle": ["lifestyle", "health", "travel"],
    "education": ["education", "universities", "students"],
    "city": ["Dubai", "Abu Dhabi", "UAE events"],
    "uk_students": ["UK students", "UK universities", "student visa UK"],
}

def _build_google_news_query_jobs() -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    seen = set()

    def _add(seed_topic: str, query: str) -> None:
        q = (query or "").strip()
        if not q:
            return
        k = q.lower()
        if k in seen:
            return
        seen.add(k)
        jobs.append({
            "seed_topic": seed_topic,
            "query": q,
            "feed_url": _google_news_rss_url(q),
        })

    for topic in CANONICAL_TOPICS:
        bases = _TOPIC_TO_QUERY.get(topic) or [topic.replace("_", " ")]

        # global
        for b in bases:
            _add(topic, b)
            _add(topic, f"{b} news")

        # local
        local_hints = GOOGLE_NEWS_LOCAL_HINTS[:max(0, GOOGLE_NEWS_LOCAL_HINTS_PER_TOPIC)]
        for hint in local_hints:
            for b in bases:
                _add(topic, f"{b} {hint}")

    if GOOGLE_NEWS_MAX_FEEDS > 0 and len(jobs) > GOOGLE_NEWS_MAX_FEEDS:
        jobs = jobs[:GOOGLE_NEWS_MAX_FEEDS]

    return jobs


# -----------------------------
# Parse RSS/Atom
# -----------------------------

def _parse_datetime_fuzzy(s: str) -> Optional[str]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        pass
    if "T" in s and ("+" in s or s.endswith("Z")):
        return s
    return None

def _parse_rss_or_atom(xml_text: str) -> Tuple[str, List[Dict[str, Any]]]:
    root = ET.fromstring(xml_text)

    channel = _find_child(root, "channel")
    if channel is not None:
        feed_title = _text(_find_child(channel, "title")) or "RSS"
        items: List[Dict[str, Any]] = []
        for it in _find_children(channel, "item"):
            title = _text(_find_child(it, "title"))
            link = _text(_find_child(it, "link"))
            desc = _text(_find_child(it, "description"))
            pub = _text(_find_child(it, "pubDate"))
            items.append({
                "title": title,
                "url": link,
                "summary": desc,
                "published_at_iso": _parse_datetime_fuzzy(pub),
            })
        return feed_title, items

    feed_title = _text(_find_child(root, "title")) or "Atom"
    items: List[Dict[str, Any]] = []
    for e in _find_children(root, "entry"):
        title = _text(_find_child(e, "title"))
        link = ""

        for l in _find_children(e, "link"):
            rel = (l.get("rel") or "").strip().lower()
            href = (l.get("href") or "").strip()
            if not href:
                continue
            if rel in ("", "alternate"):
                link = href
                break
        if not link:
            l0 = _find_child(e, "link")
            if l0 is not None:
                link = (l0.get("href") or "").strip()

        summ = _text(_find_child(e, "summary")) or _text(_find_child(e, "content"))
        updated = _text(_find_child(e, "updated")) or _text(_find_child(e, "published"))
        items.append({
            "title": title,
            "url": link,
            "summary": summ,
            "published_at_iso": _parse_datetime_fuzzy(updated),
        })
    return feed_title, items


# -----------------------------
# Dedup checks
# -----------------------------

def _card_exists_by_source_ref(source_ref: str) -> Optional[int]:
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


# -----------------------------
# OpenAI normalization
# -----------------------------

def _openai_normalize_batch(items: List[Dict[str, Any]], language: str) -> List[Dict[str, Any]]:
    if not items:
        return []

    allowlist = ", ".join(CANONICAL_TOPICS)

    system_prompt = (
        "Ты нормализуешь новости для ленты EYYE.\n"
        "На вход дается список элементов (title+summary+url+source).\n"
        "Верни валидный JSON строго формата {\"items\": [...]}.\n"
        "Правила:\n"
        "1) НЕ выдумывай факты и детали.\n"
        "2) title: одно короткое нейтральное предложение.\n"
        "3) body: 2–4 абзаца по 1–3 предложения, без эмодзи.\n"
        "4) tags: 1–6 тегов только из allowlist.\n"
        "5) importance_score: число 0..1\n"
        f"6) language: строго '{language}'\n"
        "7) ВАЖНО: поле key верни ТОЧНО таким же, как во входе (не меняй).\n"
        "8) seed_topic — тема, по которой получен item. Используй как подсказку для tags.\n"
        "Allowlist tags:\n" + allowlist
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

    parsed: Any = None
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

        key = it.get("key") or it.get("id")
        title = _clean_text(it.get("title"), 220)
        body = _clean_text(it.get("body") or it.get("summary"), 2600)

        if not key or not title or not body:
            continue

        tags = _normalize_tag_list(it.get("tags"), fallback=[])
        imp = _clamp01(it.get("importance_score", 0.6))

        results.append({
            "key": str(key).strip(),
            "title": title,
            "body": body,
            "tags": tags,
            "importance_score": imp,
            "language": language,
            "quality": "ok",
            "url": (it.get("url") or it.get("link") or "").strip(),
        })
    return results


# -----------------------------
# Insert
# -----------------------------

def _insert_rss_card(norm: Dict[str, Any], raw: Dict[str, Any]) -> Optional[int]:
    title = norm.get("title") or ""
    url = (raw.get("url") or "").strip()
    fp = _title_fp(title, url)
    if not fp:
        return None

    existing_title = _card_exists_by_title_fp(fp)
    if existing_title:
        return existing_title

    meta = {
        "source_name": raw.get("source_name"),
        "feed_title": raw.get("feed_title"),
        "feed_url": raw.get("feed_url"),
        "published_at": raw.get("published_at_iso"),
        "title_fp": fp,
        "quality": norm.get("quality", "ok"),
        "ingest": "rss",
        "raw_title": raw.get("raw_title"),
        "seed_topic": raw.get("seed_topic"),
        "seed_query": raw.get("seed_query"),
    }

    payload = {
        "title": title,
        "body": norm.get("body"),
        "tags": norm.get("tags") or [],
        "importance_score": float(norm.get("importance_score", 0.6)),
        "language": norm.get("language") or RSS_DEFAULT_LANGUAGE,
        "is_active": True,
        "source_type": "rss",
        "source_ref": url,
        "meta": meta,
    }

    resp = supabase.table("cards").insert(payload).execute()
    if not resp.data:
        return None
    return int(resp.data[0]["id"])

def _fallback_insert_raw_card(raw: Dict[str, Any], language: str) -> Optional[int]:
    title = _clean_text(raw.get("raw_title") or raw.get("title") or "", 220)
    if not title:
        return None

    summary = (raw.get("summary") or "").strip()
    summary = re.sub(r"<[^>]+>", " ", summary)
    summary = _clean_text(summary, 2000)
    if not summary:
        summary = "Источник: " + (raw.get("url") or "")

    url = (raw.get("url") or "").strip()
    fp = _title_fp(title, url)
    if not fp:
        return None

    existing_title = _card_exists_by_title_fp(fp)
    if existing_title:
        return existing_title

    meta = {
        "source_name": raw.get("source_name"),
        "feed_title": raw.get("feed_title"),
        "feed_url": raw.get("feed_url"),
        "published_at": raw.get("published_at_iso"),
        "title_fp": fp,
        "quality": "raw_fallback",
        "ingest": "rss",
        "raw_title": raw.get("raw_title") or title,
        "seed_topic": raw.get("seed_topic"),
        "seed_query": raw.get("seed_query"),
    }

    payload = {
        "title": title,
        "body": summary,
        "tags": [],
        "importance_score": 0.45,
        "language": language,
        "is_active": True,
        "source_type": "rss",
        "source_ref": url,
        "meta": meta,
    }

    resp = supabase.table("cards").insert(payload).execute()
    if not resp.data:
        return None
    return int(resp.data[0]["id"])


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    feed_jobs: List[Dict[str, str]] = []
    feed_jobs.extend(_read_feed_jobs())

    if RSS_ENABLE_GOOGLE_NEWS:
        auto_jobs = _build_google_news_query_jobs()

        env_jobs: List[Dict[str, str]] = []
        if GOOGLE_NEWS_QUERIES and GOOGLE_NEWS_QUERIES.upper() != "AUTO":
            for q in [x.strip() for x in GOOGLE_NEWS_QUERIES.split(",") if x.strip()]:
                env_jobs.append({"seed_topic": "manual", "query": q, "feed_url": _google_news_rss_url(q)})

        if GOOGLE_NEWS_QUERIES_MODE == "only":
            feed_jobs.extend(env_jobs)
        elif GOOGLE_NEWS_QUERIES_MODE == "append":
            feed_jobs.extend(auto_jobs)
            feed_jobs.extend(env_jobs)
        else:
            # auto (default)
            feed_jobs.extend(auto_jobs)
            feed_jobs.extend(env_jobs)

    if not feed_jobs:
        log.warning("No RSS feeds configured. Set RSS_FEEDS or create rss_sources.txt")
        return

    total_new = 0
    total_candidates = 0
    total_norm = 0

    lang = RSS_DEFAULT_LANGUAGE

    # in-run dedup
    seen_urls: set[str] = set()
    seen_fp_raw: set[str] = set()

    for job in feed_jobs:
        feed_url = job.get("feed_url") or ""
        seed_topic = job.get("seed_topic")
        seed_query = job.get("query")

        try:
            xml_text = _fetch_xml(feed_url)
            feed_title, items = _parse_rss_or_atom(xml_text)
            if not items:
                continue

            published_by_url = {(x.get("url") or "").strip(): x.get("published_at_iso") for x in items}

            candidates: List[Dict[str, Any]] = []
            for it in items[:RSS_FETCH_LIMIT_PER_FEED]:
                url = (it.get("url") or "").strip()
                raw_title = (it.get("title") or "").strip()
                summary = (it.get("summary") or "").strip()
                if not url or not raw_title:
                    continue

                if url in seen_urls:
                    continue
                seen_urls.add(url)

                fp_raw = _title_fp(raw_title, url)
                if fp_raw and fp_raw in seen_fp_raw:
                    continue
                if fp_raw:
                    seen_fp_raw.add(fp_raw)

                if _card_exists_by_source_ref(url):
                    continue

                candidates.append({
                    "key": _stable_item_key(url),
                    "title": raw_title,
                    "summary": summary,
                    "url": url,
                    "source_name": feed_title,
                    "seed_topic": seed_topic,
                })

            if not candidates:
                continue

            total_candidates += len(candidates)

            for i in range(0, len(candidates), RSS_OPENAI_BATCH_SIZE):
                batch = candidates[i:i + RSS_OPENAI_BATCH_SIZE]

                normalized = _openai_normalize_batch(batch, language=lang)
                total_norm += len(normalized)

                raw_by_key = {str(x["key"]).strip(): x for x in batch}
                url_to_key = {str(x.get("url") or "").strip(): str(x["key"]).strip() for x in batch}

                resolved: List[Dict[str, Any]] = []
                bad_keys: List[str] = []
                for n in normalized:
                    k = str(n.get("key") or "").strip()
                    if k in raw_by_key:
                        resolved.append(n)
                        continue

                    # model accidentally put URL into key
                    if k.startswith("http"):
                        kk = url_to_key.get(k)
                        if kk:
                            n["key"] = kk
                            resolved.append(n)
                            continue

                    # try by url field
                    u = (n.get("url") or "").strip()
                    if u:
                        kk = url_to_key.get(u)
                        if kk:
                            n["key"] = kk
                            resolved.append(n)
                            continue

                    bad_keys.append(k[:120])

                if bad_keys:
                    log.warning("OpenAI returned %d items with keys not in batch (sample): %s", len(bad_keys), bad_keys[:5])

                returned_keys = {str(x.get("key") or "").strip() for x in resolved if x.get("key")}
                expected_keys = set(raw_by_key.keys())
                missing_keys = sorted(list(expected_keys - returned_keys))

                if missing_keys:
                    log.warning(
                        "OpenAI missing %d/%d items in batch. Will fallback raw for missing. seed_topic=%s feed=%s",
                        len(missing_keys), len(expected_keys), seed_topic, feed_title
                    )

                inserted_this_batch = 0

                # insert normalized
                for n in resolved:
                    k = str(n.get("key") or "").strip()
                    raw = raw_by_key.get(k)
                    if not raw:
                        continue

                    url = (raw.get("url") or "").strip()
                    raw2 = {
                        "url": url,
                        "source_name": raw.get("source_name"),
                        "feed_title": feed_title,
                        "feed_url": feed_url,
                        "published_at_iso": published_by_url.get(url),
                        "raw_title": raw.get("title"),
                        "title": raw.get("title"),
                        "summary": raw.get("summary"),
                        "seed_topic": seed_topic,
                        "seed_query": seed_query,
                    }

                    card_id = _insert_rss_card(n, raw2)
                    if card_id:
                        total_new += 1
                        inserted_this_batch += 1

                # fallback raw for missing items (точечно)
                if RSS_FALLBACK_INSERT and missing_keys:
                    for mk in missing_keys:
                        raw = raw_by_key.get(mk)
                        if not raw:
                            continue
                        url = (raw.get("url") or "").strip()
                        raw2 = {
                            "url": url,
                            "source_name": raw.get("source_name"),
                            "feed_title": feed_title,
                            "feed_url": feed_url,
                            "published_at_iso": published_by_url.get(url),
                            "raw_title": raw.get("title"),
                            "title": raw.get("title"),
                            "summary": raw.get("summary"),
                            "seed_topic": seed_topic,
                            "seed_query": seed_query,
                        }
                        cid = _fallback_insert_raw_card(raw2, language=lang)
                        if cid:
                            total_new += 1

            log.info(
                "RSS feed processed: %s candidates=%d seed_topic=%s query=%s",
                feed_title, len(candidates), seed_topic, (seed_query[:60] + "..." if seed_query and len(seed_query) > 60 else seed_query)
            )

        except Exception:
            log.exception("Failed processing RSS feed: %s", feed_url)

    log.info("RSS ingest done. candidates=%d normalized=%d inserted/linked=%d", total_candidates, total_norm, total_new)


if __name__ == "__main__":
    main()

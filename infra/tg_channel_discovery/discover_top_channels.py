# file: infra/tg_channel_discovery/discover_top_channels.py
import json
import time
import random
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Any
from openai import OpenAI
import os

# ==== CONFIG =====

OPENAI_MODEL = "gpt-4.1-mini"
MAX_CHANNELS = 300
PAGES = 8  # каждая страница ~40 каналов, 8 страниц ≈ 300+
BASE_URL = "https://tgstat.ru/ratings/channels?sort=members&page={page}"

OUTPUT_FILE = "data/tg_channels_seed.json"

AVAILABLE_TOPICS = [
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
    "city"
]

# Стоп-слова для фильтра мусора
MUST_SKIP = [
    "казино", "casino", "ставки", "bet", "крипта сигнал", "vip", "прогноз",
    "эрот", "18+", "xxx", "интим", "порно", "магазин", "shop", "sale",
    "ставки", "букмекер", "betting", "binary", "crypto signals"
]


client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# ================= SCRAPER =================

def fetch_top_channels() -> List[Dict[str, Any]]:
    """Парсим TGStat топ каналов (страницы 1..PAGES)."""
    channels = []

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/119.0 Safari/537.36"
        )
    }

    for page in range(1, PAGES + 1):
        url = BASE_URL.format(page=page)
        print(f"[SCRAPE] Loading page {page}: {url}")

        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()
        except Exception as e:
            print(f"[ERROR] Page {page}: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        rows = soup.select(".tv-row")  # каждый канал — карточка
        if not rows:
            print("[WARN] no rows found on this page")
            continue

        for row in rows:
            name_el = row.select_one(".channel-title a")
            if not name_el:
                continue

            title = name_el.get_text(strip=True)
            href = name_el.get("href", "")
            username = href.replace("https://t.me/", "").replace("/", "")

            subs_el = row.select_one(".subscriber-count")
            subs = 0
            if subs_el:
                subs = int(subs_el.get_text(strip=True).replace(" ", "").replace("K", "000"))

            desc_el = row.select_one(".channel-description")
            desc = desc_el.get_text(strip=True) if desc_el else ""

            # мусор-фильтр
            if is_trash(title, desc):
                continue

            channels.append({
                "title": title,
                "username": username,
                "subscribers": subs,
                "description": desc
            })

            if len(channels) >= MAX_CHANNELS:
                print(f"[INFO] Reached MAX_CHANNELS={MAX_CHANNELS}")
                return channels

        # анти-бан задержка
        time.sleep(random.uniform(1.0, 2.5))

    return channels


def is_trash(title: str, desc: str) -> bool:
    text = (title + " " + desc).lower()
    return any(bad in text for bad in MUST_SKIP)


# ================= OPENAI CLASSIFIER =================

def classify_channel(channel: Dict[str, Any]) -> Dict[str, Any]:
    """Классификация канала по твоим топикам."""
    prompt = f"""
Ты — система категоризации Telegram-каналов.

Вот список допустимых топиков:
{json.dumps(AVAILABLE_TOPICS, ensure_ascii=False)}

Задача:
1) Определи, к какому топику относится канал.
2) Если не уверен или канал вне наших тем — верни "none".

Верни JSON вида:
{{
  "topic": "...",
  "confidence": 0-1
}}

Канал:
Название: {channel['title']}
Описание: {channel['description']}
Username: @{channel['username']}
Подписчики: {channel['subscribers']}
"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "system", "content": prompt}],
            max_tokens=50,
            temperature=0
        )

        raw = resp.choices[0].message.content.strip()
        data = json.loads(raw)

        return {
            "topic": data.get("topic", "none"),
            "confidence": data.get("confidence", 0.0)
        }

    except Exception as e:
        print("[OPENAI ERROR]", e)
        return {"topic": "none", "confidence": 0.0}


# ================= MAIN =================

def main():
    print("=== Step 1: Fetch top Telegram channels ===")
    channels = fetch_top_channels()
    print(f"[INFO] collected {len(channels)} channels")

    print("=== Step 2: Classify via OpenAI ===")
    topic_map: Dict[str, List[Dict[str, Any]]] = {t: [] for t in AVAILABLE_TOPICS}

    for ch in channels:
        info = classify_channel(ch)
        topic = info["topic"]
        conf = info["confidence"]

        if topic != "none" and conf >= 0.45:
            ch["confidence"] = conf
            ch["topic"] = topic
            topic_map[topic].append(ch)

        time.sleep(0.3)  # чтобы не заспамить OpenAI

    print("=== Step 3: Save JSON ===")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(topic_map, f, ensure_ascii=False, indent=2)

    print(f"[DONE] Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

# file: infra/tg_channel_discovery/tgstat_scrape_top_channels.py

import json
import os
import re
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set

import requests
from bs4 import BeautifulSoup, Tag

# ==============================
# Конфиг
# ==============================

# минимальное количество подписчиков
MIN_SUBSCRIBERS = 40_000

# максимум каналов на топик
MAX_CHANNELS_PER_TOPIC = 5

# глобальный фильтр мусора
GLOBAL_BANNED_KEYWORDS = [
    # азартка / ставки / казино
    "ставки", "бет ", "bet ", "банкролл", "букмекер", "букмекеры",
    "1xbet", "винлайн", "леонбет", "casino", "казино",
    # крипта-шлак / разводы
    "сигналы", "signal", "памп", "дамп", "инсайд", "слив", "сливы",
    # взрослый контент
    "эрот", "порно", "porno", "xxx", "18+", "onlyfans", "nsfw",
    # инфоцыганщина
    "заработай", "заработок без вложений", "быстрый заработок",
]

# при желании можно добавить include/exclude под конкретные топики
TOPIC_KEYWORDS_INCLUDE: Dict[str, List[str]] = {
    # пример:
    # "business": ["бизнес", "стартап", "предприним", "инвестиции"],
}

TOPIC_KEYWORDS_EXCLUDE: Dict[str, List[str]] = {}

# 🔥 КЛЮЧИ ЗДЕСЬ = ТОЧНО ТВОИ id ИЗ app.js
# URL'ы — это маппинг на категории TGStat (примерные, нужно будет проверить
# и при необходимости заменить на реальные страницы рейтингов).
TOPIC_CONFIG: Dict[str, str] = {
    # Мир
    "world_news":  "https://tgstat.ru/ratings/channels/news?sort=members",

    # Бизнес / деньги
    "business":    "https://tgstat.ru/ratings/channels/business?sort=members",
    "finance":     "https://tgstat.ru/ratings/channels/economics?sort=members",  # финансы/экономика

    # Технологии / наука / история
    "tech":        "https://tgstat.ru/ratings/channels/tech?sort=members",
    "science":     "https://tgstat.ru/ratings/channels/science?sort=members",
    "history":     "https://tgstat.ru/ratings/channels/history?sort=members",

    # Политика / общество
    "politics":    "https://tgstat.ru/ratings/channels/politics?sort=members",
    "society":     "https://tgstat.ru/ratings/channels/society?sort=members",

    # Кино / сериалы / развлечения
    "entertainment": "https://tgstat.ru/ratings/channels/cinema?sort=members",

    # Игры / спорт
    "gaming":      "https://tgstat.ru/ratings/channels/games?sort=members",
    "sports":      "https://tgstat.ru/ratings/channels/sport?sort=members",

    # Лайфстайл
    "lifestyle":   "https://tgstat.ru/ratings/channels/lifestyle?sort=members",

    # Образование / карьера
    "education":   "https://tgstat.ru/ratings/channels/education?sort=members",

    # Город / локальные
    "city":        "https://tgstat.ru/ratings/channels/city?sort=members",  # если нет, заменишь на раздел локальных новостей

    # Студенческая жизнь в UK — берём общую education,
    # потом вручную/через фильтры оставим UK/универы
    "uk_students": "https://tgstat.ru/ratings/channels/education?sort=members",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
}

OUTPUT_PATH = os.path.join("data", "tg_channels_seed.json")

# ==============================
# Модель
# ==============================

@dataclass
class Channel:
    title: str
    username: str
    url: str
    subscribers: int
    topic: str

# ==============================
# Парсинг
# ==============================

SUBS_PATTERNS = [
    re.compile(r"([\d\s\u00A0]+)\s+подписчик", re.IGNORECASE),
    re.compile(r"([\d\s\u00A0]+)\s+subscribers?", re.IGNORECASE),
]


def parse_subscribers(text: str) -> Optional[int]:
    for pattern in SUBS_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1)
            digits = raw.replace(" ", "").replace("\u00A0", "")
            if digits.isdigit():
                return int(digits)
    return None


def text_contains_any(text: str, words: List[str]) -> bool:
    t = text.lower()
    return any(w.lower() in t for w in words)


def is_channel_allowed(channel: Channel, block_text: str) -> bool:
    text = f"{channel.title} {block_text}".lower()

    # глобальный мусор
    if text_contains_any(text, GLOBAL_BANNED_KEYWORDS):
        return False

    # минимум подписчиков
    if channel.subscribers < MIN_SUBSCRIBERS:
        return False

    # топик-специфичные include/exclude
    topic_includes = TOPIC_KEYWORDS_INCLUDE.get(channel.topic)
    topic_excludes = TOPIC_KEYWORDS_EXCLUDE.get(channel.topic)

    if topic_excludes and text_contains_any(text, topic_excludes):
        return False

    if topic_includes and not text_contains_any(text, topic_includes):
        return False

    return True


def extract_channel_blocks(soup: BeautifulSoup) -> List[Tag]:
    """
    Находим HTML-блоки, в которых есть ссылки на t.me.
    Не привязываемся к конкретным классам — берём родителя <tr> или <div>.
    """
    blocks: List[Tag] = []
    seen: Set[int] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "t.me/" not in href:
            continue

        block = a.find_parent("tr") or a.find_parent("div") or a.parent
        if not isinstance(block, Tag):
            continue

        bid = id(block)
        if bid in seen:
            continue
        seen.add(bid)
        blocks.append(block)

    return blocks


def parse_block_to_channel(block: Tag, topic: str) -> Optional[Channel]:
    link = None
    for a in block.find_all("a", href=True):
        if "t.me/" in a["href"]:
            link = a
            break

    if link is None:
        return None

    url = link["href"]
    title = link.get_text(strip=True) or url

    m = re.search(r"t\.me/([\w\d_]+)", url)
    if not m:
        return None
    username = m.group(1)

    block_text = " ".join(block.stripped_strings)
    subs = parse_subscribers(block_text)
    if subs is None:
        return None

    return Channel(
        title=title,
        username=username,
        url=url,
        subscribers=subs,
        topic=topic,
    )

# ==============================
# Основная логика
# ==============================

def scrape_topic(topic: str, url: str) -> List[Channel]:
    print(f"\n=== Топик: {topic} ===")
    print(f"URL: {url}")

    resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    blocks = extract_channel_blocks(soup)
    print(f"Найдено HTML-блоков с t.me: {len(blocks)}")

    channels: List[Channel] = []
    seen_usernames: Set[str] = set()

    for block in blocks:
        ch = parse_block_to_channel(block, topic)
        if ch is None:
            continue
        if ch.username in seen_usernames:
            continue

        block_text = " ".join(block.stripped_strings)
        if not is_channel_allowed(ch, block_text):
            continue

        seen_usernames.add(ch.username)
        channels.append(ch)

    # сортируем по подписчикам и берём топ N
    channels.sort(key=lambda c: c.subscribers, reverse=True)
    selected = channels[:MAX_CHANNELS_PER_TOPIC]

    print(f"Отобрано каналов: {len(selected)} (>= {MIN_SUBSCRIBERS} подписчиков)")
    for c in selected:
        print(f"- {c.title} (@{c.username}) — {c.subscribers}")

    return selected


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    result: Dict[str, List[Dict]] = {}

    for topic, url in TOPIC_CONFIG.items():
        try:
            channels = scrape_topic(topic, url)
            result[topic] = [asdict(ch) for ch in channels]
        except Exception as e:
            print(f"[ERROR] Топик {topic}: {e}")
            result[topic] = []

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\nГотово. Результат сохранён в: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

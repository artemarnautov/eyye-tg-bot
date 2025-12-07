# file: infra/tg_channel_discovery/discover_from_ru_dataset.py

import os
import json
import requests
from openai import OpenAI

DATASET_URL = "https://raw.githubusercontent.com/Alb-310/telegram_channels_dataset/main/channels_ru.json"
OUTPUT_PATH = "data/tg_channels_seed.json"

AVAILABLE_TOPICS = [
    { "id": "world_news", "label": "Мир" },
    { "id": "business", "label": "Бизнес" },
    { "id": "finance", "label": "Финансы / Крипто" },
    { "id": "tech", "label": "Технологии" },
    { "id": "science", "label": "Наука" },
    { "id": "history", "label": "История" },
    { "id": "politics", "label": "Политика" },
    { "id": "society", "label": "Общество" },
    { "id": "entertainment", "label": "Кино / Сериалы" },
    { "id": "gaming", "label": "Игры" },
    { "id": "sports", "label": "Спорт" },
    { "id": "lifestyle", "label": "Лайфстайл" },
    { "id": "education", "label": "Образование / Карьера" },
    { "id": "city", "label": "Город / Локальные новости" },
    { "id": "uk_students", "label": "Студенческая жизнь в UK" }
]

TOPIC_IDS = [t["id"] for t in AVAILABLE_TOPICS]

MIN_SUBSCRIBERS = 40000
MAX_CHANNELS = 300


def download_dataset():
    print(f"[INFO] Downloading dataset from GitHub…")
    response = requests.get(DATASET_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    # dataset structure:
    # { "channels": [ {...}, {...} ] }
    if isinstance(data, dict) and "channels" in data:
        return data["channels"]
    elif isinstance(data, list):
        return data
    else:
        print("[ERROR] Unexpected dataset format")
        return []



def clean_and_filter(channels):
    print("[INFO] Filtering channels…")

    result = []
    for ch in channels:
        subs = ch.get("subscribers", 0) or 0
        lang = (ch.get("language") or "").lower()
        username = ch.get("username") or ""
        title = ch.get("title") or ""

        if subs < MIN_SUBSCRIBERS:
            continue

        # RU dataset — фильтруем только русские
        if lang and not ("ru" in lang):
            continue

        # Пропускаем мусор
        if not username or username.startswith("joinchat"):
            continue
        if "xxx" in title.lower():
            continue
        if "casino" in title.lower():
            continue

        result.append({
            "username": username,
            "title": title,
            "subscribers": subs,
            "description": ch.get("description") or "",
        })

        if len(result) >= MAX_CHANNELS:
            break

    print(f"[INFO] Selected {len(result)} channels for classification")
    return result


def classify_channels(channels):
    print("[INFO] Classifying channels with OpenAI…")

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    result = {}

    for ch in channels:
        prompt = f"""
Ты — модель категоризации контента.
Тебе дан telegram-канал. Нужно определить, к какому из следующих топиков он относится:

{TOPIC_IDS}

Выбери один.
Ответ строго в JSON:
{{"topic": "..."}}

Канал:
Название: {ch["title"]}
Описание: {ch["description"]}
"""

        try:
            resp = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": "Ты — точный и строгий классификатор."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
            )

            topic = "other"
            try:
                data = json.loads(resp.choices[0].message["content"])
                topic = data.get("topic", "other")
            except Exception:
                pass

            if topic not in TOPIC_IDS:
                topic = "other"

            result.setdefault(topic, []).append(ch)

            print(f"[AI] {ch['username']} → {topic}")

        except Exception as e:
            print(f"[ERROR] Failed classify {ch['username']}: {e}")

    return result


def save_json(data):
    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[DONE] Saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    print("=== Step 1: Download dataset ===")
    raw = download_dataset()

    print("=== Step 2: Filter channels ===")
    filtered = clean_and_filter(raw)

    print("=== Step 3: Classify channels ===")
    classified = classify_channels(filtered)

    print("=== Step 4: Save ===")
    save_json(classified)

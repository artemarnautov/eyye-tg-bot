# file: src/webapp_backend/profile_service.py
import json
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

from .openai_client import call_openai_chat

logger = logging.getLogger(__name__)


def _normalize_profile_dict(profile: Dict[str, Any]) -> Dict[str, Any]:
    """
    Нормализация structured_profile:
    - дефолтные поля,
    - приведение типов.
    """
    profile = dict(profile)

    profile.setdefault("location_city", None)
    profile.setdefault("location_country", None)
    profile.setdefault("topics", [])
    profile.setdefault("negative_topics", [])
    profile.setdefault("interests_as_tags", [])
    profile.setdefault("user_meta", {})

    topics = profile.get("topics")
    if not isinstance(topics, list):
        topics = []
    normalized_topics: List[Dict[str, Any]] = []
    for t in topics:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name", "")).strip()
        if not name:
            continue
        weight = t.get("weight", 1.0)
        try:
            weight = float(weight)
        except (TypeError, ValueError):
            weight = 1.0
        category = t.get("category")
        detail = t.get("detail")
        normalized_topics.append(
            {
                "name": name,
                "weight": weight,
                "category": category,
                "detail": detail,
            }
        )
    profile["topics"] = normalized_topics

    neg = profile.get("negative_topics")
    if not isinstance(neg, list):
        neg = []
    profile["negative_topics"] = [str(x).strip() for x in neg if str(x).strip()]

    tags = profile.get("interests_as_tags")
    if not isinstance(tags, list):
        tags = []
    profile["interests_as_tags"] = [str(x).strip() for x in tags if str(x).strip()]

    user_meta = profile.get("user_meta")
    if not isinstance(user_meta, dict):
        user_meta = {}
    profile["user_meta"] = user_meta

    return profile


def _build_fallback_profile(
    city: Optional[str],
    base_tags: List[str],
) -> Dict[str, Any]:
    """
    Простой fallback-профиль:
    - город, если есть,
    - interests_as_tags = выбранные теги.
    """
    tags_no_dups = list(dict.fromkeys([t for t in base_tags if t]))

    return {
        "location_city": city,
        "location_country": None,
        "topics": [],
        "negative_topics": [],
        "interests_as_tags": tags_no_dups,
        "user_meta": {
            "age_group": None,
            "student_status": None,
        },
    }


def _call_openai_structured_profile_sync(
    city: Optional[str],
    tags: List[str],
    language: str = "ru",
) -> Dict[str, Any]:
    """
    Строим structured_profile через OpenAI.
    Вход — город и список тегов интересов пользователя.
    """
    user_payload = {
        "city": city,
        "language": language,
        "tags": tags,
    }

    system_prompt = """
Ты помогаешь новостному рекомендательному сервису EYYE.

По JSON с городом и списком интересов пользователя нужно вернуть
СТРОГО JSON-объект со структурой:

{
  "location_city": string | null,
  "location_country": string | null,
  "topics": [
    {
      "name": string,
      "weight": number,
      "category": string | null,
      "detail": string | null
    },
    ...
  ],
  "negative_topics": [string, ...],
  "interests_as_tags": [string, ...],
  "user_meta": {
    "age_group": string | null,
    "student_status": string | null
  }
}

Требования:
- Никакого текста вне JSON.
- weight от 0.0 до 1.0.
- category — общий род ("business", "sports", "culture", "tech", "education" и т.п.) или null.
- interests_as_tags — короткие теги латиницей ("startups", "premier_league", "uk_universities").
"""

    payload = {
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            },
        ],
        "max_output_tokens": 800,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    resp_json = call_openai_chat(payload)
    if not resp_json:
        logger.warning(
            "OpenAI did not return response JSON for structured_profile. Using fallback."
        )
        return _normalize_profile_dict(_build_fallback_profile(city, tags))

    try:
        choices = resp_json.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("No choices in OpenAI response")

        first_choice = choices[0] or {}
        message = first_choice.get("message") or {}
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Empty content in OpenAI response")

        logger.debug(
            "OpenAI structured_profile raw content (first 200 chars): %s",
            content[:200].replace("\n", " "),
        )

        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("Parsed JSON is not an object")

        return _normalize_profile_dict(parsed)

    except Exception:
        logger.exception("Failed to parse OpenAI structured_profile response. Using fallback.")
        return _normalize_profile_dict(_build_fallback_profile(city, tags))


def build_and_save_structured_profile(
    supabase: Optional[Client],
    user_id: int,
    city: Optional[str],
    base_tags: List[str],
    language: str = "ru",
) -> None:
    """
    Синхронная функция (запускаем через asyncio.to_thread):
    - строит structured_profile через OpenAI (или fallback),
    - сохраняет его в user_profiles.
    """
    if not supabase:
        logger.warning("Supabase is not configured, skip build_and_save_structured_profile")
        return

    base_tags = [t for t in base_tags if t]
    profile = _call_openai_structured_profile_sync(city, base_tags, language)

    raw_interests_lines: List[str] = []
    if city:
        raw_interests_lines.append(f"city: {city}")
    if base_tags:
        raw_interests_lines.append("tags: " + ", ".join(base_tags))
    raw_interests = "\n".join(raw_interests_lines)

    data: Dict[str, Any] = {
        "user_id": user_id,
        "structured_profile": profile,
        "raw_interests": raw_interests,
    }
    if city:
        data["location_city"] = city

    try:
        resp = (
            supabase.table("user_profiles")
            .upsert(data, on_conflict="user_id")
            .execute()
        )
        logger.info(
            "Upsert structured_profile for user_id=%s, resp=%s",
            user_id,
            getattr(resp, "data", None),
        )
    except Exception:
        logger.exception("Error saving structured_profile for user_id=%s", user_id)


def get_or_build_profile_for_feed(
    supabase: Optional[Client],
    user_id: int,
    base_tags: List[str],
) -> Dict[str, Any]:
    """
    Возвращает профиль пользователя для /api/feed:
    - если есть structured_profile — используем его,
      при необходимости добавляем interests_as_tags из base_tags;
    - если нет — строим простой fallback-профиль.
    """
    city: Optional[str] = None
    structured: Optional[Dict[str, Any]] = None

    if supabase:
        try:
            resp = (
                supabase.table("user_profiles")
                .select("location_city, location_country, structured_profile")
                .eq("user_id", user_id)
                .limit(1)
                .execute()
            )
            data = getattr(resp, "data", None) or getattr(resp, "model", None) or []
            if data:
                row = data[0]
                city = row.get("location_city")
                structured_raw = row.get("structured_profile")
                if isinstance(structured_raw, str):
                    try:
                        structured = json.loads(structured_raw)
                    except Exception:
                        structured = None
                elif isinstance(structured_raw, dict):
                    structured = structured_raw
        except Exception:
            logger.exception("Error loading user_profile for user_id=%s", user_id)

    base_tags = [t for t in base_tags if t]

    if structured:
        profile = _normalize_profile_dict(structured)
        if not profile.get("interests_as_tags"):
            profile["interests_as_tags"] = base_tags
        # city в профиле может быть пустым — подставим, если знаем
        if city and not profile.get("location_city"):
            profile["location_city"] = city
        return profile

    return _build_fallback_profile(city, base_tags)

# file: src/webapp_backend/profile_service.py
import json
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

logger = logging.getLogger(__name__)


# ===== Внутренние утилиты для user_profiles =====


def _get_profile_row_by_user_id(
    supabase: Client,
    user_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Достаём одну строку из user_profiles по user_id.

    ВАЖНО: в схеме нет колонки id, поэтому выбираем только user_id и structured_profile.
    """
    try:
        resp = (
            supabase.table("user_profiles")
            .select("user_id, structured_profile")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load user_profiles row for user_id=%s", user_id)
        return None

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    rows = data or []
    if not rows:
        return None
    return rows[0]


def _parse_structured_profile(value: Any) -> Optional[Dict[str, Any]]:
    """
    Превращаем structured_profile в dict, даже если он хранится как JSON-строка.
    """
    if value is None:
        return None

    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj
        except Exception:
            logger.exception("Failed to parse structured_profile JSON")
            return None

    return None


# ===== Публичные функции для API и фида =====


def get_profile_summary(
    supabase: Client,
    user_id: int,
) -> Dict[str, Any]:
    """
    Короткая сводка профиля для /api/profile.

    Возвращает:
    {
      "has_onboarding": bool,
      "city": str | None,
      "tags": [str, ...]
    }
    """
    default = {"has_onboarding": False, "city": None, "tags": []}

    if supabase is None:
        return default

    row = _get_profile_row_by_user_id(supabase, user_id)
    if not row:
        return default

    structured = _parse_structured_profile(row.get("structured_profile"))
    if not structured:
        return default

    city = structured.get("city") or None
    tags = structured.get("interests_as_tags") or []

    if not isinstance(tags, list):
        tags = []

    clean_tags: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s and s not in clean_tags:
            clean_tags.append(s)

    has_onboarding = bool(city or clean_tags)

    return {
        "has_onboarding": has_onboarding,
        "city": city,
        "tags": clean_tags,
    }


def save_onboarding(
    supabase: Client,
    user_id: int,
    city: Optional[str],
    tags: List[str],
) -> None:
    """
    Сохраняем результаты онбординга:
    - city -> structured_profile.city
    - tags -> structured_profile.interests_as_tags

    Если строки в user_profiles ещё нет — создаём.
    """
    if supabase is None:
        return

    row = _get_profile_row_by_user_id(supabase, user_id)
    structured: Dict[str, Any]

    if row and row.get("structured_profile") is not None:
        parsed = _parse_structured_profile(row["structured_profile"])
        structured = parsed if parsed is not None else {}
    else:
        structured = {}

    # Обновляем город
    if city:
        structured["city"] = city
    else:
        # Если нужно, можно удалить ключ:
        # structured.pop("city", None)
        structured["city"] = None

    # Обновляем интересы
    structured["interests_as_tags"] = tags or []

    # Если строки нет — вставляем, иначе обновляем
        # Если строки нет — вставляем, иначе обновляем
    if not row:
        # raw_interests у тебя в БД NOT NULL, поэтому кладём пустую строку
        payload = {
            "user_id": user_id,
            "raw_interests": "",
            "structured_profile": structured,
        }
        try:
            supabase.table("user_profiles").insert(payload).execute()
        except Exception:
            logger.exception(
                "Failed to insert user_profile for user_id=%s", user_id
            )

    else:
        try:
            supabase.table("user_profiles").update(
                {"structured_profile": structured}
            ).eq("user_id", user_id).execute()
        except Exception:
            logger.exception(
                "Failed to update user_profile for user_id=%s", user_id
            )


def get_interest_tags_for_user(
    supabase: Client,
    user_id: int,
) -> List[str]:
    """
    Возвращает interests_as_tags из structured_profile пользователя.
    Если профиля нет или он кривой — возвращает [].
    """
    if supabase is None:
        return []

    row = _get_profile_row_by_user_id(supabase, user_id)
    if not row:
        return []

    structured = _parse_structured_profile(row.get("structured_profile"))
    if not structured:
        return []

    tags = structured.get("interests_as_tags") or []
    if not isinstance(tags, list):
        return []

    cleaned: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s and s not in cleaned:
            cleaned.append(s)

    return cleaned

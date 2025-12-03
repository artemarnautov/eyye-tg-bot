# file: src/webapp_backend/profile_service.py
import json
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

logger = logging.getLogger(__name__)


def _load_structured_profile(
    supabase: Client,
    user_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Достаём structured_profile из user_profiles (если он есть).
    Ничего не строим, просто читаем.
    """
    try:
        resp = (
            supabase.table("user_profiles")
            .select("structured_profile")
            .eq("user_id", user_id)
            .limit(1)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load structured_profile for user_id=%s", user_id)
        return None

    data = getattr(resp, "data", None)
    if data is None:
        data = getattr(resp, "model", None)
    if not data:
        return None

    row = data[0]
    structured = row.get("structured_profile")
    if structured is None:
        return None

    if isinstance(structured, str):
        try:
            structured_obj = json.loads(structured)
        except Exception:
            logger.exception(
                "Failed to parse structured_profile JSON for user_id=%s", user_id
            )
            return None
    else:
        structured_obj = structured

    if not isinstance(structured_obj, dict):
        return None

    return structured_obj


def get_interest_tags_for_user(
    supabase: Client,
    user_id: int,
) -> List[str]:
    """
    Возвращает interests_as_tags из structured_profile пользователя.
    Если профиля нет или он кривой — возвращает [].
    """
    profile = _load_structured_profile(supabase, user_id)
    if not profile:
        return []

    tags = profile.get("interests_as_tags") or []
    if not isinstance(tags, list):
        return []

    cleaned: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s and s not in cleaned:
            cleaned.append(s)

    return cleaned

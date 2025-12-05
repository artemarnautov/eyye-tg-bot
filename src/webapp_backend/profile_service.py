# file: src/webapp_backend/profile_service.py
import json
import logging
from typing import Any, Dict, List, Optional

from supabase import Client

logger = logging.getLogger(__name__)


def _get_profile_row_by_user_id(
    supabase: Client,
    user_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Вытаскиваем строку из user_profiles по user_id.
    Нужна, чтобы при сохранении онбординга понимать, обновлять запись или создавать.
    """
    try:
        resp = (
            supabase.table("user_profiles")
            .select("id, user_id, structured_profile")
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
    if not data:
        return None

    return data[0]


def _parse_structured_profile_value(raw: Any) -> Optional[Dict[str, Any]]:
    """
    Приводим structured_profile к dict:
    - если там dict — просто возвращаем,
    - если там строка — пытаемся распарсить JSON,
    - иначе None.
    """
    if raw is None:
        return None

    if isinstance(raw, dict):
        return raw

    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
        except Exception:
            logger.exception("Failed to parse structured_profile JSON")
            return None
        if isinstance(obj, dict):
            return obj
        return None

    return None


def _load_structured_profile(
    supabase: Client,
    user_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Достаём structured_profile из user_profiles (если он есть).
    Ничего не строим, просто читаем.
    """
    row = _get_profile_row_by_user_id(supabase, user_id)
    if not row:
        return None

    structured = row.get("structured_profile")
    structured_obj = _parse_structured_profile_value(structured)
    return structured_obj


def get_interest_tags_for_user(
    supabase: Client,
    user_id: int,
) -> List[str]:
    """
    Возвращает interests_as_tags из structured_profile пользователя.
    Если их нет, пробует взять topics (которые поставил пользователь при онбординге).

    Если профиля нет или всё криво — возвращает [].
    """
    profile = _load_structured_profile(supabase, user_id)
    if not profile:
        return []

    # 1. Старое поле, которое писал OpenAI
    tags = profile.get("interests_as_tags")

    # 2. Fallback: если interests_as_tags нет/пусто — используем topics, которые выбрал пользователь
    if not tags:
        tags = profile.get("topics")

    if not tags:
        return []

    if not isinstance(tags, list):
        return []

    cleaned: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s and s not in cleaned:
            cleaned.append(s)

    return cleaned


# =========================
# Онбординг: город + темы
# =========================


def get_onboarding_state(
    supabase: Client,
    tg_id: int,
) -> Dict[str, Any]:
    """
    Возвращает минимальное состояние онбординга для WebApp.

    tg_id здесь мы трактуем как user_id (один и тот же идентификатор).
    Формат ответа:
    {
      "has_onboarding": bool,
      "city": str | None,
      "selected_topics": List[str]
    }
    """
    profile = _load_structured_profile(supabase, user_id=tg_id)

    if not profile:
        return {
            "has_onboarding": False,
            "city": None,
            "selected_topics": [],
        }

    city = profile.get("city")
    selected_topics = (
        profile.get("topics")
        or profile.get("selected_topics")
        or []
    )

    if not isinstance(selected_topics, list):
        selected_topics = []

    has_onboarding = bool(city or selected_topics)

    return {
        "has_onboarding": has_onboarding,
        "city": city,
        "selected_topics": selected_topics,
    }


def save_onboarding(
    supabase: Client,
    tg_id: int,
    city: Optional[str],
    selected_topics: List[str],
) -> None:
    """
    Сохраняем город и выбранные темы в user_profiles.structured_profile.

    Логика:
      - ищем user_profiles по user_id == tg_id;
      - если есть запись — обновляем structured_profile;
      - если нет — создаём новую запись.
    """
    row = _get_profile_row_by_user_id(supabase, tg_id)

    # Разбираем текущий structured_profile (если был)
    structured = {}
    if row:
        existing_structured = _parse_structured_profile_value(
            row.get("structured_profile")
        )
        if isinstance(existing_structured, dict):
            structured = dict(existing_structured)  # копия, чтобы не мутировать исходный объект

    # Обновляем/добавляем поля онбординга
    if city is not None:
        city_value = city.strip()
        structured["city"] = city_value or None

    if selected_topics:
        structured["topics"] = [str(t).strip() for t in selected_topics if str(t).strip()]

    payload = {
        "user_id": tg_id,
        "structured_profile": structured,
    }

    try:
        if row:
            # Обновляем существующую запись
            supabase.table("user_profiles").update(payload).eq("id", row["id"]).execute()
        else:
            # Создаём новую запись
            supabase.table("user_profiles").insert(payload).execute()
    except Exception:
        logger.exception("Failed to save onboarding for tg_id=%s", tg_id)

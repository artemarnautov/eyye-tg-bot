# file: src/webapp_backend/cards_service_vector.py
import base64
import json
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import Client

CARD_FIELDS = (
    "id,source_type,source_ref,title,body,tags,category,language,"
    "importance_score,created_at,is_active,meta,fingerprint,quality_score,content_type,nsfw"
)


def _b64encode_json(obj: Dict[str, Any]) -> str:
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode_json(s: str) -> Optional[Dict[str, Any]]:
    try:
        pad = "=" * (-len(s) % 4)
        raw = base64.urlsafe_b64decode((s + pad).encode("ascii"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


def _to_float_list(v: Any) -> Optional[List[float]]:
    if v is None:
        return None
    if isinstance(v, list):
        try:
            return [float(x) for x in v]
        except Exception:
            return None
    if isinstance(v, str) and v.startswith("[") and v.endswith("]"):
        try:
            arr = json.loads(v)
            if isinstance(arr, list):
                return [float(x) for x in arr]
        except Exception:
            return None
    return None


def _vec_to_str(emb: List[float]) -> str:
    # максимально совместимо с PostgREST+pgvector (vector input как строка "[...]")
    return "[" + ",".join(f"{float(x):.9g}" for x in emb) + "]"


def _normalize(vec: List[float]) -> List[float]:
    s = 0.0
    for x in vec:
        s += x * x
    n = math.sqrt(s)
    if not math.isfinite(n) or n <= 0:
        return vec
    return [x / n for x in vec]


def _fetch_cards_by_ids(supabase: Client, ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not ids:
        return {}
    resp = supabase.table("cards").select(CARD_FIELDS).in_("id", ids).execute()
    rows = resp.data or []
    return {int(r["id"]): r for r in rows if "id" in r}


def _mark_seen(supabase: Client, user_id: int, card_ids: List[int]) -> None:
    if not card_ids:
        return
    now = datetime.now(timezone.utc).isoformat()
    rows = [{"user_id": user_id, "card_id": int(cid), "seen_at": now} for cid in card_ids]
    supabase.table("user_seen_cards").upsert(rows, on_conflict="user_id,card_id").execute()


def _get_user_profile(supabase: Client, user_id: int) -> Optional[Dict[str, Any]]:
    resp = (
        supabase.table("user_profiles")
        .select("user_id,embedding,embedding_model,embedding_updated_at,onboarding_topics,home_region")
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    return rows[0] if rows else None


def _upsert_user_embedding(supabase: Client, user_id: int, emb: List[float], model: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "user_id": user_id,
        "embedding": _vec_to_str(emb),
        "embedding_model": model,
        "embedding_updated_at": now,
        "updated_at": now,
    }
    supabase.table("user_profiles").upsert(payload, on_conflict="user_id").execute()


def _build_user_vector_from_events(
    supabase: Client,
    user_id: int,
    days: int = 14,
    limit: int = 200,
) -> Optional[List[float]]:
    r = supabase.rpc("user_positive_cards", {"p_tg_id": user_id, "p_days": days, "p_limit": limit}).execute()
    rows = r.data or []
    if len(rows) < 3:
        return None

    ids = [int(x["card_id"]) for x in rows if x.get("card_id") is not None]
    w_map = {int(x["card_id"]): float(x.get("weight") or 0.0) for x in rows if x.get("card_id") is not None}

    resp = supabase.table("cards").select("id,embedding").in_("id", ids).execute()
    cards = resp.data or []

    acc: Optional[List[float]] = None
    w_sum = 0.0

    for c in cards:
        cid = int(c["id"])
        emb = _to_float_list(c.get("embedding"))
        if not emb:
            continue
        w = max(0.0, float(w_map.get(cid, 0.0)))
        if w <= 0:
            continue

        if acc is None:
            acc = [0.0] * len(emb)

        for i, v in enumerate(emb):
            acc[i] += w * float(v)
        w_sum += w

    if acc is None or w_sum <= 0:
        return None

    acc = [x / w_sum for x in acc]
    return _normalize(acc)


def _diversify_ranked(
    ordered_ids: List[int],
    cards_by_id: Dict[int, Dict[str, Any]],
    limit: int,
    max_same_source_in_row: int = 2,
) -> List[int]:
    out: List[int] = []
    last_source: Optional[str] = None
    streak = 0

    for cid in ordered_ids:
        c = cards_by_id.get(cid)
        if not c:
            continue
        src = c.get("source_type") or "unknown"

        if last_source == src:
            if streak >= max_same_source_in_row:
                continue
            streak += 1
        else:
            last_source = src
            streak = 1

        out.append(cid)
        if len(out) >= limit:
            break
    return out


def build_feed_for_user_vector_paginated(
    supabase: Client,
    user_id: int,
    limit: int = 20,
    offset: int = 0,
    cursor: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    # "cursor-like" пагинация через seq (offset сейчас не используется, но оставляем для совместимости API)
    cur = _b64decode_json(cursor) if cursor else None
    seq = 0
    seed = datetime.now(timezone.utc).date().isoformat()
    if isinstance(cur, dict) and cur.get("mode") == "vector":
        seq = int(cur.get("seq", 0) or 0)
        seed = str(cur.get("seed") or seed)

    debug: Dict[str, Any] = {
        "cursor_in": cursor,
        "limit": limit,
        "offset": offset,
        "cursor_bad": False if (cursor is None or cur is not None) else True,
        "cursor_mode": "vector",
        "seq": seq,
        "seed": seed,
    }

    profile = _get_user_profile(supabase, user_id)
    user_emb = _to_float_list(profile.get("embedding")) if profile else None

    built_now = False
    if not user_emb:
        vec = _build_user_vector_from_events(supabase, user_id, days=14, limit=200)
        if vec:
            _upsert_user_embedding(supabase, user_id, vec, model="derived:cards-mean-v1")
            user_emb = vec
            built_now = True

    debug["user_embedding_available"] = bool(user_emb)
    debug["user_embedding_built_now"] = built_now

    # -------- кандидаты --------
    vector_ids: List[int] = []
    vector_sim: Dict[int, float] = {}

    if user_emb:
        # RPC бывает настроен либо на JSON-array, либо на pgvector input string.
        # Делаем best-effort: пробуем list -> если упало, пробуем строку.
        rows: List[Dict[str, Any]] = []
        try:
            r = supabase.rpc(
                "search_cards_for_user",
                {
                    "p_user_id": user_id,
                    "p_query": user_emb,  # type: ignore
                    "p_limit": 250,
                    "p_max_age_hours": 2160,
                    "p_only_active": True,
                },
            ).execute()
            rows = r.data or []
        except Exception:
            r = supabase.rpc(
                "search_cards_for_user",
                {
                    "p_user_id": user_id,
                    "p_query": _vec_to_str(user_emb),
                    "p_limit": 250,
                    "p_max_age_hours": 2160,
                    "p_only_active": True,
                },
            ).execute()
            rows = r.data or []

        for x in rows:
            cid = int(x["id"])
            vector_ids.append(cid)
            vector_sim[cid] = float(x.get("similarity") or 0.0)

    r2 = supabase.rpc(
        "fresh_cards_for_user",
        {"p_user_id": user_id, "p_limit": 200, "p_hours": 48, "p_only_active": True},
    ).execute()
    fresh_rows = r2.data or []
    fresh_ids = [int(x["id"]) for x in fresh_rows if x.get("id") is not None]

    debug["vector_candidates"] = len(vector_ids)
    debug["fresh_candidates"] = len(fresh_ids)

    # если вектора нет -> fresh only
    if not vector_ids:
        chosen_ids = fresh_ids[:limit]
        cards_by_id = _fetch_cards_by_ids(supabase, chosen_ids)
        items = [cards_by_id[cid] for cid in chosen_ids if cid in cards_by_id]
        _mark_seen(supabase, user_id, [int(c["id"]) for c in items])

        next_cursor = _b64encode_json({"mode": "vector", "seq": seq + 1, "seed": seed})
        cursor_obj = {"mode": "cursor", "limit": limit, "cursor_in": cursor, "cursor_out": next_cursor}
        debug["pagination_mode"] = "cursor"
        debug["feed_mode"] = "vector"
        debug["fallback"] = "fresh_only"
        return items, debug, cursor_obj

    # blend 80/20
    take_v = max(10, int(math.ceil(limit * 0.8)))
    take_e = max(5, limit - take_v)

    v_slice = vector_ids[: take_v * 5]
    e_slice = fresh_ids[: take_e * 5]

    merged: List[int] = []
    used = set()

    for cid in v_slice:
        if cid not in used:
            merged.append(cid)
            used.add(cid)
    for cid in e_slice:
        if cid not in used:
            merged.append(cid)
            used.add(cid)

    cards_by_id = _fetch_cards_by_ids(supabase, merged)

    merged_sorted = sorted(
        [cid for cid in merged if cid in cards_by_id],
        key=lambda cid: (vector_sim.get(cid, 0.0), cards_by_id[cid].get("created_at") or ""),
        reverse=True,
    )

    chosen_ids = _diversify_ranked(merged_sorted, cards_by_id, limit=limit, max_same_source_in_row=2)
    items = [cards_by_id[cid] for cid in chosen_ids if cid in cards_by_id]

    _mark_seen(supabase, user_id, [int(c["id"]) for c in items])

    next_cursor = _b64encode_json({"mode": "vector", "seq": seq + 1, "seed": seed})
    cursor_obj = {"mode": "cursor", "limit": limit, "cursor_in": cursor, "cursor_out": next_cursor}

    debug["pagination_mode"] = "cursor"
    debug["feed_mode"] = "vector"
    if items:
        sims = [vector_sim.get(int(c["id"]), 0.0) for c in items]
        debug["avg_similarity"] = float(sum(sims) / max(1, len(sims)))
        debug["min_similarity"] = float(min(sims))
        debug["max_similarity"] = float(max(sims))

    return items, debug, cursor_obj

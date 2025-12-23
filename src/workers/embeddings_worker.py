# file: src/workers/embeddings_worker.py
import argparse
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client

logger = logging.getLogger("eyye.embeddings_worker")

# memoize which store payload style works
_STORE_STYLE: Optional[int] = None


# =====================
# env helpers
# =====================

def _env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None else default


def _require_env(name: str) -> str:
    v = _env(name, "").strip()
    if not v:
        raise RuntimeError(f"{name} is not set")
    return v


# =====================
# OpenAI embeddings (urllib)
# =====================

def _openai_base_url() -> str:
    return (_env("OPENAI_BASE_URL", "https://api.openai.com/v1") or "https://api.openai.com/v1").rstrip("/")


def _openai_api_key() -> str:
    return _require_env("OPENAI_API_KEY")


def _openai_timeout() -> float:
    try:
        return float(_env("OPENAI_TIMEOUT_SECONDS", "30"))
    except Exception:
        return 30.0


def _openai_embeddings_model() -> str:
    return (_env("OPENAI_EMBEDDINGS_MODEL", "text-embedding-3-small") or "text-embedding-3-small").strip()


def _call_openai_embeddings(texts: List[str], model: Optional[str] = None) -> List[List[float]]:
    if not texts:
        return []
    model = (model or _openai_embeddings_model()).strip()

    url = _openai_base_url() + "/embeddings"
    headers = {
        "Authorization": f"Bearer {_openai_api_key()}",
        "Content-Type": "application/json",
    }
    body = {"model": model, "input": texts}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=_openai_timeout()) as resp:
            raw = resp.read().decode("utf-8")
        payload = json.loads(raw)
        items = payload.get("data") or []
        out: List[List[float]] = []
        for it in items:
            emb = it.get("embedding")
            if isinstance(emb, list):
                out.append([float(x) for x in emb])
        return out
    except urllib.error.HTTPError as e:
        try:
            err = e.read().decode("utf-8", errors="replace")
        except Exception:
            err = "<no body>"
        raise RuntimeError(f"OpenAI embeddings HTTPError: code={getattr(e,'code',None)} body={err[:1000]}")
    except Exception as e:
        raise RuntimeError(f"OpenAI embeddings error: {e}")


# =====================
# Supabase RPC: claim
# =====================

def rpc_claim_cards_for_embedding(
    supabase,
    *,
    claim_batch: int,
    claim_seconds: int,
    max_attempts: int,
) -> List[Dict[str, Any]]:
    """
    Канонический вызов под сигнатуру:
      claim_cards_for_embedding(n integer, claim_seconds integer, max_attempts integer)

    + фолбэки, если в БД аргументы называются иначе.
    """
    variants: List[Dict[str, Any]] = [
        {"n": int(claim_batch), "claim_seconds": int(claim_seconds), "max_attempts": int(max_attempts)},
        {"p_n": int(claim_batch), "p_claim_seconds": int(claim_seconds), "p_max_attempts": int(max_attempts)},
        {"limit": int(claim_batch), "claim_seconds": int(claim_seconds), "max_attempts": int(max_attempts)},
        {"p_limit": int(claim_batch), "p_claim_seconds": int(claim_seconds), "p_max_attempts": int(max_attempts)},
        {"batch_size": int(claim_batch), "lease_seconds": int(claim_seconds), "max_attempts": int(max_attempts)},
    ]

    last_err: Optional[Exception] = None
    for args in variants:
        try:
            res = supabase.rpc("claim_cards_for_embedding", args).execute()
            data = getattr(res, "data", None) or []
            return list(data or [])
        except Exception as e:
            last_err = e

    raise RuntimeError(f"claim_cards_for_embedding failed (last_err={last_err})")


# =====================
# Supabase RPC: store embedding
# =====================

def rpc_store_card_embedding(
    supabase,
    *,
    card_id: int,
    embedding: List[float],
    embedding_model: str,
    now_iso: Optional[str] = None,
    error_text: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Пишем эмбеддинг через RPC store_card_embedding.
    Схемы/имена аргументов могли меняться — пробуем несколько вариантов и запоминаем рабочий.
    """
    global _STORE_STYLE

    cid = int(card_id)
    model = str(embedding_model).strip()
    ts = (now_iso or datetime.now(timezone.utc).isoformat())

    # Кандидаты payload-ов (порядок важен)
    candidates: Tuple[Dict[str, Any], ...] = (
        # самый ожидаемый (как часто делают в PL/pgSQL)
        {"p_card_id": cid, "p_embedding": embedding, "p_embedding_model": model, "p_now": ts, "p_error_text": error_text},
        # без времени/ошибки
        {"p_card_id": cid, "p_embedding": embedding, "p_embedding_model": model},
        # более "прямые" имена
        {"card_id": cid, "embedding": embedding, "embedding_model": model, "now_iso": ts, "error_text": error_text},
        {"card_id": cid, "embedding": embedding, "embedding_model": model},
        # иногда id вместо card_id
        {"id": cid, "embedding": embedding, "embedding_model": model},
    )

    def _call(payload: Dict[str, Any]) -> Dict[str, Any]:
        # чистим None, чтобы не ломать сигнатуру
        clean = {k: v for k, v in payload.items() if v is not None}
        res = supabase.rpc("store_card_embedding", clean).execute()
        data = getattr(res, "data", None)
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data:
            # иногда rpc возвращает [{...}]
            if isinstance(data[0], dict):
                return data[0]
        return {"ok": True, "id": cid}

    # если уже знаем рабочий стиль — используем
    if _STORE_STYLE is not None:
        try:
            return _call(candidates[_STORE_STYLE])
        except Exception:
            _STORE_STYLE = None  # переопределим ниже

    last_err: Optional[Exception] = None
    for i, payload in enumerate(candidates):
        try:
            out = _call(payload)
            _STORE_STYLE = i
            return out
        except Exception as e:
            last_err = e

    raise RuntimeError(f"store_card_embedding failed for all variants (last_err={last_err})")


# =====================
# Worker loop helpers
# =====================

def _build_embed_text(row: Dict[str, Any]) -> str:
    title = str(row.get("title") or "").strip()
    body = str(row.get("body") or "").strip()
    # чтобы не раздувать токены: режем тело
    if len(body) > 4000:
        body = body[:4000]
    if title and body:
        return f"{title}\n\n{body}"
    return title or body


def _row_is_too_old(row: Dict[str, Any], max_age_days: int) -> bool:
    """
    Мягкий фильтр "не эмбедить старое".
    Работает только если claim возвращает created_at.
    """
    if max_age_days <= 0:
        return False
    created = row.get("created_at")
    if not created:
        return False
    try:
        # created_at может быть '2025-12-23T...' или с timezone
        s = str(created).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt < (datetime.now(timezone.utc) - timedelta(days=max_age_days))
    except Exception:
        return False


# =====================
# main
# =====================

def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(name)s:%(message)s")

    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="Do one iteration and exit")
    ap.add_argument("--idle-sleep", type=float, default=15.0, help="Sleep when nothing to do")
    ap.add_argument("--claim-batch", type=int, default=200, help="How many cards to claim per iteration")
    ap.add_argument("--embed-batch", type=int, default=40, help="How many to embed per OpenAI request")
    ap.add_argument("--claim-seconds", type=int, default=180, help="Lease seconds for claimed rows")
    ap.add_argument("--max-attempts", type=int, default=5, help="Max embedding attempts per row")
    ap.add_argument("--model", type=str, default="", help="Override embeddings model")
    ap.add_argument("--max-age-days", type=int, default=int(_env("EYYE_EMBED_MAX_AGE_DAYS", "4") or "4"),
                    help="Skip embedding if row.created_at older than N days (if created_at is present in claim result)")
    args = ap.parse_args()

    supabase_url = _require_env("SUPABASE_URL")
    supabase_key = (
        _env("SUPABASE_SERVICE_KEY")
        or _env("SUPABASE_SERVICE_ROLE_KEY")
        or _env("SUPABASE_KEY")
        or _env("SUPABASE_ANON_KEY")
    ).strip()
    if not supabase_key:
        raise RuntimeError("SUPABASE_SERVICE_KEY/SUPABASE_SERVICE_ROLE_KEY/SUPABASE_KEY/SUPABASE_ANON_KEY is not set")

    supabase = create_client(supabase_url, supabase_key)

    model = (args.model.strip() or _openai_embeddings_model()).strip()
    logger.info("Embeddings worker started. model=%s max_age_days=%s", model, int(args.max_age_days))

    while True:
        try:
            claimed = rpc_claim_cards_for_embedding(
                supabase,
                claim_batch=max(1, int(args.claim_batch)),
                claim_seconds=max(30, int(args.claim_seconds)),
                max_attempts=max(1, int(args.max_attempts)),
            )
        except Exception as e:
            logger.exception("claim failed: %s", e)
            if args.once:
                return 2
            time.sleep(max(3.0, float(args.idle_sleep)))
            continue

        if not claimed:
            logger.info("Nothing to embed. sleep=%.1fs", float(args.idle_sleep))
            if args.once:
                return 0
            time.sleep(max(1.0, float(args.idle_sleep)))
            continue

        # если claim вернул created_at — можем скипать старые
        if int(args.max_age_days) > 0:
            before = len(claimed)
            claimed = [r for r in claimed if not _row_is_too_old(r, int(args.max_age_days))]
            skipped = before - len(claimed)
            if skipped:
                logger.info("Skipped %d cards as too old (max_age_days=%d)", skipped, int(args.max_age_days))

        if not claimed:
            logger.info("After max_age_days filter: nothing to embed. sleep=%.1fs", float(args.idle_sleep))
            if args.once:
                return 0
            time.sleep(max(1.0, float(args.idle_sleep)))
            continue

        logger.info("Claimed %d cards", len(claimed))

        rows: List[Dict[str, Any]] = list(claimed)
        i = 0
        while i < len(rows):
            batch = rows[i : i + max(1, int(args.embed_batch))]
            i += len(batch)

            texts = [_build_embed_text(r) for r in batch]
            ids: List[Optional[int]] = []
            for r in batch:
                try:
                    ids.append(int(r.get("id")))
                except Exception:
                    ids.append(None)

            now_iso = datetime.now(timezone.utc).isoformat()

            try:
                embs = _call_openai_embeddings(texts, model=model)
                if len(embs) != len(batch):
                    raise RuntimeError(f"embeddings size mismatch: got={len(embs)} expected={len(batch)}")

                for emb, cid in zip(embs, ids):
                    if cid is None:
                        continue
                    try:
                        rpc_store_card_embedding(
                            supabase,
                            card_id=int(cid),
                            embedding=emb,
                            embedding_model=model,
                            now_iso=now_iso,
                            error_text=None,
                        )
                    except Exception as e:
                        logger.exception("store embedding failed for card_id=%s: %s", cid, e)

            except Exception as e:
                err = str(e)
                logger.exception("OpenAI embeddings failed for batch: %s", err)
                # пробуем записать error_text (если RPC поддерживает)
                for cid in ids:
                    if cid is None:
                        continue
                    try:
                        rpc_store_card_embedding(
                            supabase,
                            card_id=int(cid),
                            embedding=[],
                            embedding_model=model,
                            now_iso=now_iso,
                            error_text=err[:900],
                        )
                    except Exception:
                        logger.exception("failed to record error for card_id=%s", cid)

        if args.once:
            logger.info("Done (once).")
            return 0


if __name__ == "__main__":
    raise SystemExit(main())

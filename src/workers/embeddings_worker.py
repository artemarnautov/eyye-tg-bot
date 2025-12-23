# file: src/workers/embeddings_worker.py
import argparse
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client

logger = logging.getLogger("eyye.embeddings_worker")


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
        raise RuntimeError(f"OpenAI embeddings HTTPError: code={getattr(e,'code',None)} body={err[:1200]}")
    except Exception as e:
        raise RuntimeError(f"OpenAI embeddings error: {e}")


# =====================
# Supabase RPC: claim (canonical)
# =====================
def rpc_claim_cards_for_embedding(
    supabase,
    *,
    claim_batch: int,
    claim_seconds: int,
    max_attempts: int,
) -> List[Dict[str, Any]]:
    """
    Под твою текущую сигнатуру:
      claim_cards_for_embedding(n integer, claim_seconds integer, max_attempts integer)
    """
    args = {"n": int(claim_batch), "claim_seconds": int(claim_seconds), "max_attempts": int(max_attempts)}
    res = supabase.rpc("claim_cards_for_embedding", args).execute()
    data = getattr(res, "data", None) or []
    return list(data or [])


# =====================
# Store embedding via UPDATE (robust)
# =====================
def _vec_to_str(emb: List[float]) -> str:
    # pgvector через PostgREST у тебя хранится строкой вида "[...]"
    return "[" + ",".join(f"{float(x):.9g}" for x in emb) + "]"


def _try_update_cards(supabase, card_id: int, upd: Dict[str, Any]) -> None:
    """
    Пытаемся обновить cards.
    Если какие-то колонки отсутствуют (редко, но бывает) — повторяем с минимальным набором.
    """
    try:
        supabase.table("cards").update(upd).eq("id", int(card_id)).execute()
        return
    except Exception as e:
        s = str(e).lower()
        # минимальный безопасный апдейт
        minimal = {
            "embedding": upd.get("embedding"),
            "embedding_model": upd.get("embedding_model"),
            "embedding_updated_at": upd.get("embedding_updated_at"),
            "embedding_last_error": upd.get("embedding_last_error"),
        }
        # если ошибка не про колонку — тоже попробуем minimal, но потом пробросим
        try:
            supabase.table("cards").update(minimal).eq("id", int(card_id)).execute()
            logger.warning("cards update used MINIMAL columns for card_id=%s due to error=%s", card_id, s[:200])
            return
        except Exception:
            raise


def store_embedding(
    supabase,
    *,
    card_id: int,
    embedding_str: Optional[str],
    embedding_model: str,
    error_text: Optional[str],
    attempts_value: Optional[int],
) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()

    upd: Dict[str, Any] = {
        "embedding": embedding_str,  # None если ошибка
        "embedding_model": embedding_model,
        "embedding_updated_at": now_iso,
        "embedding_last_error": (error_text[:900] if error_text else None),
    }
    if attempts_value is not None:
        upd["embedding_attempts"] = int(attempts_value)

    _try_update_cards(supabase, int(card_id), upd)


# =====================
# Worker loop
# =====================
def _build_embed_text(row: Dict[str, Any]) -> str:
    title = str(row.get("title") or "").strip()
    body = str(row.get("body") or "").strip()
    if len(body) > 4000:
        body = body[:4000]
    if title and body:
        return f"{title}\n\n{body}"
    return title or body


def _age_hours_from_row(row: Dict[str, Any]) -> Optional[float]:
    v = row.get("created_at")
    if not v:
        return None
    try:
        dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
        return delta.total_seconds() / 3600.0
    except Exception:
        return None


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
    ap.add_argument("--max-age-hours", type=int, default=96, help="Process only cards newer than this (default 4 days)")
    args = ap.parse_args()

    supabase_url = _require_env("SUPABASE_URL")
    supabase_key = (
        _env("SUPABASE_SERVICE_KEY")
        or _env("SUPABASE_SERVICE_ROLE_KEY")
        or _env("SUPABASE_KEY")
        or _env("SUPABASE_ANON_KEY")
    ).strip()
    if not supabase_key:
        raise RuntimeError("SUPABASE_SERVICE_KEY/SUPABASE_KEY/SUPABASE_ANON_KEY is not set")

    supabase = create_client(supabase_url, supabase_key)

    model = (args.model.strip() or _openai_embeddings_model()).strip()
    logger.info("Embeddings worker started. model=%s max_age_hours=%s", model, int(args.max_age_hours))

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

        # фильтр по возрасту
        filtered: List[Dict[str, Any]] = []
        for r in claimed:
            age_h = _age_hours_from_row(r)
            if age_h is None or age_h <= float(args.max_age_hours):
                filtered.append(r)

        if not filtered:
            logger.info("Claimed=%d but all are older than max_age_hours=%s", len(claimed), int(args.max_age_hours))
            if args.once:
                return 0
            time.sleep(max(1.0, float(args.idle_sleep)))
            continue

        logger.info("Claimed %d cards (filtered %d)", len(claimed), len(filtered))

        rows = filtered
        i = 0
        while i < len(rows):
            batch = rows[i : i + max(1, int(args.embed_batch))]
            i += len(batch)

            texts = [_build_embed_text(r) for r in batch]

            ids: List[int] = []
            attempts: List[Optional[int]] = []
            for r in batch:
                cid = int(r.get("id"))
                ids.append(cid)
                try:
                    attempts.append(int(r.get("embedding_attempts") or 0) + 1)
                except Exception:
                    attempts.append(None)

            try:
                embs = _call_openai_embeddings(texts, model=model)
                if len(embs) != len(batch):
                    raise RuntimeError(f"embeddings size mismatch: got={len(embs)} expected={len(batch)}")

                for cid, emb, att in zip(ids, embs, attempts):
                    try:
                        store_embedding(
                            supabase,
                            card_id=cid,
                            embedding_str=_vec_to_str(emb),
                            embedding_model=model,
                            error_text=None,
                            attempts_value=att,
                        )
                    except Exception as e:
                        logger.exception("store embedding failed for card_id=%s: %s", cid, e)

            except Exception as e:
                err = str(e)
                logger.exception("OpenAI embeddings failed for batch: %s", err)
                for cid, att in zip(ids, attempts):
                    try:
                        store_embedding(
                            supabase,
                            card_id=cid,
                            embedding_str=None,
                            embedding_model=model,
                            error_text=err[:900],
                            attempts_value=att,
                        )
                    except Exception:
                        logger.exception("failed to record error for card_id=%s", cid)

        if args.once:
            logger.info("Done (once).")
            return 0


if __name__ == "__main__":
    raise SystemExit(main())

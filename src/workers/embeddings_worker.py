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
        # OpenAI returns embeddings in same order
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
# Supabase RPC compatibility layer
# =====================

def rpc_claim_cards_for_embedding(
    supabase,
    *,
    claim_batch: int,
    claim_seconds: int,
    max_attempts: int,
) -> List[Dict[str, Any]]:
    """
    Пытаемся вызвать claim_cards_for_embedding с разными вариантами аргументов,
    потому что сигнатуры могли меняться.
    Возвращает список карточек (dict).
    """
    variants: List[Dict[str, Any]] = [
        {"p_limit": claim_batch, "p_claim_seconds": claim_seconds, "p_max_attempts": max_attempts},
        {"limit": claim_batch, "claim_seconds": claim_seconds, "max_attempts": max_attempts},
        {"batch_size": claim_batch, "lease_seconds": claim_seconds, "max_attempts": max_attempts},
        {"p_batch": claim_batch, "p_lease_seconds": claim_seconds, "p_max_attempts": max_attempts},
        {"p_limit": claim_batch, "p_claim_seconds": claim_seconds},
        {"limit": claim_batch, "claim_seconds": claim_seconds},
        {"batch_size": claim_batch, "lease_seconds": claim_seconds},
        {"p_batch": claim_batch, "p_lease_seconds": claim_seconds},
        {"n": claim_batch},
    ]

    last_err: Optional[Exception] = None
    for args in variants:
        try:
            res = supabase.rpc("claim_cards_for_embedding", args).execute()
            data = getattr(res, "data", None) or getattr(res, "model", None) or []
            return list(data or [])
        except Exception as e:
            last_err = e

    raise RuntimeError(f"claim_cards_for_embedding failed for all arg variants: {last_err}")


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
    """
    args = {
        "n": int(claim_batch),
        "claim_seconds": int(claim_seconds),
        "max_attempts": int(max_attempts),
    }

    try:
        res = supabase.rpc("claim_cards_for_embedding", args).execute()
        data = getattr(res, "data", None) or getattr(res, "model", None) or []
        return list(data or [])
    except Exception as e:
        raise RuntimeError(f"claim_cards_for_embedding failed (args={args}): {e}")




# =====================
# Worker loop
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
    args = ap.parse_args()

    supabase_url = _require_env("SUPABASE_URL")
    supabase_key = (
        _env("SUPABASE_SERVICE_KEY")
        or _env("SUPABASE_KEY")
        or _env("SUPABASE_ANON_KEY")
    ).strip()
    if not supabase_key:
        raise RuntimeError("SUPABASE_SERVICE_KEY/SUPABASE_KEY/SUPABASE_ANON_KEY is not set")

    supabase = create_client(supabase_url, supabase_key)

    model = (args.model.strip() or _openai_embeddings_model()).strip()
    logger.info("Embeddings worker started. model=%s", model)

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

        logger.info("Claimed %d cards", len(claimed))

        # батчим в OpenAI
        rows: List[Dict[str, Any]] = list(claimed)
        i = 0
        while i < len(rows):
            batch = rows[i : i + max(1, int(args.embed_batch))]
            i += len(batch)

            texts = [_build_embed_text(r) for r in batch]
            ids = []
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

                for row, emb, cid in zip(batch, embs, ids):
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
                # если OpenAI упал — всем из батча пишем error (через store/update)
                err = str(e)
                logger.exception("OpenAI embeddings failed for batch: %s", err)
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

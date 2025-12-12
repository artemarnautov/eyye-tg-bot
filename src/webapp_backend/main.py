
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from supabase import Client, create_client

from .cards_service import build_feed_for_user
from .profile_service import get_profile_summary, save_onboarding
from .telemetry_service import EventsRequest, log_events

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Paths
# ==========
THIS_DIR = Path(__file__).resolve().parent          # .../eyye-tg-bot/src/webapp_backend
ROOT_DIR = THIS_DIR.parents[2]                      # .../eyye-tg-bot
WEBAPP_DIR = ROOT_DIR / "webapp"
INDEX_HTML_PATH = WEBAPP_DIR / "index.html"

if not WEBAPP_DIR.exists():
    logger.warning("WEBAPP_DIR not found: %s", WEBAPP_DIR)
if not INDEX_HTML_PATH.exists():
    logger.warning("index.html not found at %s", INDEX_HTML_PATH)

# ==========
# Supabase
# ==========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized in webapp_backend")
    except Exception:
        logger.exception("Failed to init Supabase client")
        supabase = None
else:
    logger.warning("Supabase URL/KEY are not set. /api/feed and /api/profile will not work.")

# ==========
# Optional ranker (не должен валить сервис, если файла нет)
# ==========
try:
    from .feed_ranker import rank_cards_for_user  # type: ignore
except Exception:
    rank_cards_for_user = None  # type: ignore
    logger.info("feed_ranker not available (ok for MVP)")

# ==========
# Helpers
# ==========
def load_user_topic_weights_for_user(tg_id: int) -> Dict[str, float]:
    if supabase is None:
        return {}

    try:
        resp = (
            supabase.table("user_topic_weights")
            .select("tag, weight")
            .eq("tg_id", tg_id)
            .execute()
        )
    except Exception:
        logger.exception("Failed to load user_topic_weights for tg_id=%s", tg_id)
        return {}

    rows = getattr(resp, "data", None) or []
    out: Dict[str, float] = {}

    for row in rows:
        tag = row.get("tag")
        weight = row.get("weight")
        if tag is None:
            continue
        try:
            w = float(weight)
        except (TypeError, ValueError):
            continue

        tag_str = str(tag).strip()
        if tag_str:
            out[tag_str] = w

    return out

# ==========
# App
# ==========
app = FastAPI(title="EYYE WebApp Backend")

@app.on_event("startup")
async def _startup_log() -> None:
    logger.info("FastAPI startup OK. ROOT_DIR=%s WEBAPP_DIR=%s", ROOT_DIR, WEBAPP_DIR)

# Static mount only if directory exists (иначе FastAPI падает на старте)
if WEBAPP_DIR.exists() and WEBAPP_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=str(WEBAPP_DIR)), name="static")
else:
    logger.warning("Skipping /static mount because webapp dir missing: %s", WEBAPP_DIR)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========
# Routes
# ==========

@app.get("/ping")
async def ping() -> Dict[str, Any]:
    return {"status": "ok", "service": "eyye-webapp-backend"}

@app.get("/api/health")
@app.get("/health")
async def api_health() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "eyye-webapp-backend",
        "ts": datetime.now(timezone.utc).isoformat(),
        "supabase_configured": bool(SUPABASE_URL and SUPABASE_KEY),
    }

@app.get("/", response_class=HTMLResponse)
async def index(tg_id: Optional[str] = None) -> HTMLResponse:
    if not INDEX_HTML_PATH.exists():
        raise HTTPException(status_code=500, detail="index.html not found")

    try:
        html = INDEX_HTML_PATH.read_text(encoding="utf-8")
    except Exception:
        logger.exception("Failed to read index.html")
        raise HTTPException(status_code=500, detail="failed to read index.html")

    return HTMLResponse(content=html)

# ---- Profile ----

@app.get("/api/profile")
async def api_profile(tg_id: int = Query(..., alias="tg_id")) -> Dict[str, Any]:
    if supabase is None:
        return {"has_onboarding": False, "city": None, "tags": []}
    return get_profile_summary(supabase, tg_id)

@app.post("/api/profile/onboarding")
async def api_profile_onboarding(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    user_id_raw = payload.get("user_id")
    try:
        user_id = int(user_id_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="user_id must be an integer")

    city = payload.get("city")
    if city is not None:
        city = str(city).strip() or None

    tags = payload.get("tags") or []
    if not isinstance(tags, list):
        raise HTTPException(status_code=400, detail="tags must be a list")

    clean_tags: List[str] = []
    for t in tags:
        s = str(t).strip()
        if s and s not in clean_tags:
            clean_tags.append(s)

    try:
        save_onboarding(supabase, user_id, city, clean_tags)
    except Exception:
        logger.exception("Failed to save onboarding for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail="failed to save profile")

    return get_profile_summary(supabase, user_id)

# ---- Feed ----

@app.get("/api/feed")
async def api_feed(
    tg_id: int = Query(..., alias="tg_id"),
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    items, debug = build_feed_for_user(supabase, tg_id, limit=limit, offset=offset)

    topic_weights = load_user_topic_weights_for_user(tg_id)
    if topic_weights and rank_cards_for_user is not None:
        try:
            items = rank_cards_for_user(items, topic_weights)
            debug = debug or {}
            debug.setdefault("topic_weights", topic_weights)
        except Exception:
            logger.exception("Failed to rank feed for tg_id=%s", tg_id)

    return {"items": items, "debug": debug}

# ---- Events / Telemetry ----

@app.post("/api/events")
async def api_events(payload: EventsRequest) -> Dict[str, Any]:
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    try:
        log_events(supabase, payload)
    except Exception:
        logger.exception("Failed to log events for tg_id=%s", payload.tg_id)
        raise HTTPException(status_code=500, detail="failed to log events")

    return {"status": "ok"}

# backward compatible alias
@app.post("/api/telemetry")
async def api_telemetry(payload: EventsRequest) -> Dict[str, Any]:
    return await api_events(payload)
PY

# file: src/webapp_backend/main.py
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from supabase import Client, create_client

from .profile_service import get_profile_summary, save_onboarding
from .telemetry_service import EventsRequest, log_events

# Важно: используем пагинированный фид с явным курсором
try:
    from .cards_service import build_feed_for_user_paginated  # type: ignore
except Exception:
    build_feed_for_user_paginated = None  # type: ignore
    from .cards_service import build_feed_for_user  # type: ignore

logger = logging.getLogger("eyye.webapp_backend")
logging.basicConfig(level=logging.INFO)

# ==========
# Paths
# ==========
THIS_DIR = Path(__file__).resolve().parent
DEFAULT_ROOT = Path("/root/eyye-tg-bot")


def _detect_root_dir() -> Path:
    """
    Goal: ROOT_DIR must be repo root: /root/eyye-tg-bot

    Prefer:
      1) EYYE_ROOT_DIR env
      2) marker search: parent containing webapp/index.html
      3) cwd containing webapp/
      4) default /root/eyye-tg-bot if exists
      5) fallback to parents[2]
    """
    env_root = os.getenv("EYYE_ROOT_DIR")
    if env_root:
        p = Path(env_root).expanduser().resolve()
        if p.exists():
            return p

    here = Path(__file__).resolve()
    for p in here.parents:
        if (p / "webapp" / "index.html").exists():
            return p

    cwd = Path.cwd().resolve()
    if (cwd / "webapp").exists():
        return cwd

    if (DEFAULT_ROOT / "webapp" / "index.html").exists():
        return DEFAULT_ROOT

    try:
        return here.parents[2]
    except Exception:
        return cwd


ROOT_DIR = _detect_root_dir()
WEBAPP_DIR = ROOT_DIR / "webapp"
INDEX_HTML_PATH = WEBAPP_DIR / "index.html"

# ==========
# Supabase
# ==========
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = (
    os.getenv("SUPABASE_KEY")
    or os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("SUPABASE_SERVICE_KEY")
)

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Supabase client initialized in webapp_backend")
    except Exception:
        logger.exception("Failed to init Supabase client")
        supabase = None
else:
    logger.warning(
        "Supabase URL/KEY are not set. /api/feed and /api/profile will not work."
    )

# ==========
# App
# ==========
app = FastAPI(title="EYYE WebApp Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

api = APIRouter(prefix="/api")


@app.on_event("startup")
async def _startup_log() -> None:
    logger.info(
        "FastAPI startup OK. ROOT_DIR=%s WEBAPP_DIR=%s",
        str(ROOT_DIR),
        str(WEBAPP_DIR),
    )
    if not WEBAPP_DIR.exists():
        logger.warning("WEBAPP_DIR not found: %s", WEBAPP_DIR)
    if not INDEX_HTML_PATH.exists():
        logger.warning("index.html not found at %s", INDEX_HTML_PATH)


# ==========
# Non-API routes
# ==========
@app.get("/ping")
async def ping() -> Dict[str, Any]:
    return {"status": "ok", "service": "eyye-webapp-backend"}


# ==========
# API routes
# ==========
@api.get("/health")
@api.get("/healthz")
async def api_health() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "eyye-webapp-backend",
        "ts": datetime.now(timezone.utc).isoformat(),
        "supabase_configured": bool(SUPABASE_URL and SUPABASE_KEY),
        "root_dir": str(ROOT_DIR),
        "webapp_dir": str(WEBAPP_DIR),
    }


@api.get("/profile")
async def api_profile(tg_id: int = Query(..., alias="tg_id")) -> Dict[str, Any]:
    # MVP: service must be alive even without Supabase
    if supabase is None:
        return {"has_onboarding": False, "city": None, "tags": []}
    return get_profile_summary(supabase, tg_id)


@api.post("/profile/onboarding")
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


@api.get("/feed")
async def api_feed(
    tg_id: int = Query(..., alias="tg_id"),
    limit: int = Query(20, ge=1, le=50),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """
    Важно:
    - Ранжирование + дедуп + диверсификация уже сделаны в cards_service.
    - НЕ делаем второй ranker здесь (он ломал микс источников и порядок).
    """
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    if build_feed_for_user_paginated is not None:
        items, debug, cursor = build_feed_for_user_paginated(
            supabase, tg_id, limit=limit, offset=offset
        )
        return {"items": items, "debug": debug, "cursor": cursor}

    # fallback (если вдруг нет paginated)
    items, debug = build_feed_for_user(supabase, tg_id, limit=limit, offset=offset)
    return {"items": items, "debug": debug}


@api.post("/events")
async def api_events(payload: EventsRequest) -> Dict[str, Any]:
    if supabase is None:
        raise HTTPException(status_code=500, detail="Supabase is not configured")

    try:
        log_events(supabase, payload)
    except Exception:
        logger.exception("Failed to log events for tg_id=%s", payload.tg_id)
        raise HTTPException(status_code=500, detail="failed to log events")

    return {"status": "ok"}


@api.post("/telemetry")
async def api_telemetry(payload: EventsRequest) -> Dict[str, Any]:
    return await api_events(payload)


app.include_router(api)

# ==========
# Serve WebApp from "/"
# ==========
if WEBAPP_DIR.exists() and WEBAPP_DIR.is_dir():
    app.mount("/", StaticFiles(directory=str(WEBAPP_DIR), html=True), name="webapp")
else:
    logger.warning("WEBAPP_DIR missing; WebApp static won't be served: %s", WEBAPP_DIR)

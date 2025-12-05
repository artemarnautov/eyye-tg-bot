# file: src/webapp_backend/main.py
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, List

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from supabase import Client, create_client

from .cards_service import build_feed_for_user
from .profile_service import get_profile_summary, save_onboarding

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ==========
# Пути
# ==========

THIS_DIR = Path(__file__).resolve().parent          # .../src/webapp_backend
ROOT_DIR = THIS_DIR.parents[2]                      # .../eyye-tg-bot
WEBAPP_DIR = ROOT_DIR / "webapp"
INDEX_HTML_PATH = WEBAPP_DIR / "index.html"

if not INDEX_HTML_PATH.exists():
  logger.warning("index.html not found at %s", INDEX_HTML_PATH)

# ==========
# Supabase
# ==========

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Optional[Client] = None
if SUPABASE_URL and SUPABASE_KEY:
  supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
  logger.info("Supabase client initialized in webapp_backend")
else:
  logger.warning("Supabase URL/KEY are not set. /api/feed and /api/profile will not work.")


# ==========
# FastAPI app
# ==========

app = FastAPI(title="EYYE WebApp Backend")

# Статика: webapp/ -> /static/*
app.mount(
  "/static",
  StaticFiles(directory=str(WEBAPP_DIR)),
  name="static",
)

# CORS на будущее, сейчас всё из того же домена
app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_methods=["*"],
  allow_headers=["*"],
)


# ==========
# Маршруты
# ==========


@app.get("/ping")
async def ping() -> dict:
  return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/", response_class=HTMLResponse)
async def index(tg_id: str | None = None) -> HTMLResponse:
  """
  Отдаём index.html WebApp. tg_id читается на фронте из query-параметра.
  """
  if not INDEX_HTML_PATH.exists():
    raise HTTPException(status_code=500, detail="index.html not found")

  try:
    html = INDEX_HTML_PATH.read_text(encoding="utf-8")
  except Exception:
    logger.exception("Failed to read index.html")
    raise HTTPException(status_code=500, detail="failed to read index.html")

  return HTMLResponse(content=html)


# ---- Профиль / онбординг ----


@app.get("/api/profile")
async def api_profile(
  tg_id: int = Query(..., alias="tg_id"),
) -> Dict[str, Any]:
  """
  Короткая сводка профиля:
  {
    "has_onboarding": bool,
    "city": str | None,
    "tags": [str, ...]
  }
  """
  if supabase is None:
    return {"has_onboarding": False, "city": None, "tags": []}

  summary = get_profile_summary(supabase, tg_id)
  return summary


@app.post("/api/profile/onboarding")
async def api_profile_onboarding(
  payload: Dict[str, Any] = Body(...),
) -> Dict[str, Any]:
  """
  Принимаем результат онбординга из WebApp:
  {
    "user_id": int,
    "city": str | null,
    "tags": [str, ...]
  }
  """
  if supabase is None:
    raise HTTPException(
      status_code=500,
      detail="Supabase is not configured",
    )

  user_id_raw = payload.get("user_id")
  try:
    user_id = int(user_id_raw)
  except (TypeError, ValueError):
    raise HTTPException(status_code=400, detail="user_id must be an integer")

  city = payload.get("city")
  if city is not None:
    city = str(city).strip()
    if city == "":
      city = None

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

  # Возвращаем актуальную сводку профиля
  summary = get_profile_summary(supabase, user_id)
  return summary


# ---- Фид ----


@app.get("/api/feed")
async def api_feed(
  tg_id: int = Query(..., alias="tg_id"),
  limit: int = Query(20, ge=1, le=50),
  offset: int = Query(0, ge=0),
) -> dict:
  """
  Основной endpoint для WebApp:
  возвращает персональную ленту карточек для пользователя с поддержкой offset.
  """
  items, debug = build_feed_for_user(
    supabase,
    tg_id,
    limit=limit,
    offset=offset,
  )
  return {"items": items, "debug": debug}

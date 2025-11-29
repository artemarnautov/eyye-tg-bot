# file: src/webapp_backend/main.py

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# ===== Пути до проекта и webapp =====

# main.py лежит в src/webapp_backend/
# поднимаемся на два уровня -> корень репозитория
BASE_DIR = Path(__file__).resolve().parent.parent.parent
WEBAPP_DIR = BASE_DIR / "webapp"

app = FastAPI(title="EYYE WebApp Backend")

# CORS — пока разрешаем всё, потом ограничим доменами (TODO)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # TODO: в проде ограничить доменами
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Раздача статических файлов (JS/CSS) из папки webapp
# /webapp/app.js -> webapp/app.js
# /webapp/styles.css -> webapp/styles.css
app.mount(
    "/webapp",
    StaticFiles(directory=str(WEBAPP_DIR)),
    name="webapp-static",
)


@app.get("/ping")
async def ping():
    """Healthcheck, чтобы убедиться, что backend жив."""
    return {"status": "ok", "service": "eyye-webapp-backend"}


@app.get("/", include_in_schema=False)
async def index():
    """Отдаём index.html WebApp по корню /."""
    index_path = WEBAPP_DIR / "index.html"
    return FileResponse(index_path)

#!/usr/bin/env bash
set -euo pipefail

# Переходим в папку проекта
cd /root/eyye-tg-bot

# Активируем venv
source venv/bin/activate

# На всякий случай выставим PYTHONPATH
export PYTHONPATH=src

# 1. Тянем новые посты из каналов и сразу создаём карточки через OpenAI
PYTHONPATH=src python -m telegram_ingest.fetch_telegram_posts

# 2. Старый батч-процессор через telegram_posts нам сейчас не нужен:
#PYTHONPATH=src python -m telegram_ingest.process_telegram_posts

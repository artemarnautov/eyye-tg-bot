# EYYE Telegram Bot

MVP-бот для персонализированной новостной ленты на основе Telegram-каналов, Supabase и OpenAI.

## Шаги

1. Склонировать репозиторий и перейти в папку `eyye-tg-bot/`.
2. Скопировать `.env.example` в `.env` и заполнить переменные.
3. Добавить каналы в `channels.txt`.
4. Реализовать логику бота в `src/bot.py`.

## Запуск через Docker

1. Собрать и запустить контейнеры:
   ```bash
   docker-compose up --build

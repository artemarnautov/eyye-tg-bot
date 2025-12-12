# file: src/wikipedia_ingest/__init__.py
"""
Wikipedia ingest package for EYYE.

Назначение:
- Фоново подтягивать статьи из Wikipedia (en/ru),
- нормализовать их в формат cards,
- учитывать глобальные интересы пользователей (user_topic_weights),
- приоритезировать популярные и релевантные темы.

Основной скрипт:
- fetch_wikipedia_articles.py — точка входа для systemd-воркера.
"""

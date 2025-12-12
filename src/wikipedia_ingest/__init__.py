"""
Wikipedia ingest package for EYYE.

Новая логика (MVP, hourly):
- Не быть "бездушной энциклопедией".
- Собирать кандидатов из:
  1) RecentChanges (свежие правки) — сигнал "why now"
  2) Wikimedia Pageviews Top — сигнал "why now" по трендам
- Дальше пропускать кандидата через OpenAI-гейт:
  - is_newsworthy=true/false
  - why_now обязателен
  - если нет "почему сейчас" -> SKIP

Точка входа:
- fetch_wikipedia_articles.py
"""

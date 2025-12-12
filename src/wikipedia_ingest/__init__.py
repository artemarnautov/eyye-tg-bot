"""
Wikipedia ingest package for EYYE.

Назначение (MVP):
- Подтягивать самые читаемые страницы Wikipedia (en/ru) (hourly/daily),
- превращать их в cards,
- давать строгий "why_now" (почему эта карточка попала в ленту именно сейчас),
- держать небольшой бюджет (лимиты на количество LLM-нормализаций за запуск),
- смешивать Wikipedia с Telegram карточками в общей таблице cards.

Точка входа:
- fetch_wikipedia_articles.py — запуск через systemd / runner.
"""

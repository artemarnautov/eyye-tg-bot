# file: src/webapp_backend/openai_client.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

import urllib.request
import urllib.error

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_API_BASE = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_CHAT_COMPLETIONS_URL = OPENAI_API_BASE.rstrip("/") + "/chat/completions"
OPENAI_TIMEOUT_SECONDS = float(os.getenv("OPENAI_TIMEOUT_SECONDS", "30"))


def call_openai_chat(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Универсальная обёртка над /v1/chat/completions.
    Используем тот же контракт payload, что и раньше в боте.
    """
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set, skipping OpenAI call")
        return {}

    url = OPENAI_CHAT_COMPLETIONS_URL
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    model = payload.get("model") or OPENAI_MODEL

    messages = payload.get("messages")
    if not messages:
        input_field = payload.get("input")
        if isinstance(input_field, list):
            messages = input_field
        else:
            messages = [{"role": "user", "content": str(input_field)}]

    max_tokens = payload.get("max_tokens")
    if max_tokens is None:
        max_tokens = payload.get("max_output_tokens", 512)
    try:
        max_tokens_int = int(max_tokens)
    except (TypeError, ValueError):
        max_tokens_int = 512

    temperature = payload.get("temperature", 0.2)
    try:
        temperature_float = float(temperature)
    except (TypeError, ValueError):
        temperature_float = 0.2

    body: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens_int,
        "temperature": temperature_float,
    }

    if "response_format" in payload:
        body["response_format"] = payload["response_format"]

    data = json.dumps(body).encode("utf-8")

    started_at = datetime.now(timezone.utc)
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT_SECONDS) as resp:
            raw = resp.read().decode("utf-8")
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.info("OpenAI chat.completions call OK (%.2fs)", elapsed)
        return json.loads(raw)
    except urllib.error.HTTPError as e:
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        try:
            error_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = "<no body>"
        logger.error(
            "OpenAI HTTPError in chat.completions (%.2fs), code=%s, body=%s",
            elapsed,
            e.code,
            error_body[:1000],
        )
        return {}
    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        logger.exception("Error calling OpenAI chat.completions (%.2fs): %s", elapsed, e)
        return {}

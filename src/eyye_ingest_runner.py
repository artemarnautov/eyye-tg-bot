# file: src/eyye_ingest_runner.py
import logging
import subprocess
import sys
from pathlib import Path

# Корень репозитория: /root/eyye-tg-bot (локально у тебя свой путь)
ROOT_DIR = Path(__file__).resolve().parent.parent
log = logging.getLogger(__name__)


def run_step(description: str, cmd: str) -> None:
    """
    Универсальный раннер одного шага инжеста.
    cmd исполняется в корне репозитория.
    """
    log.info("=== [%s] start ===", description)
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=str(ROOT_DIR),
            check=True,
        )
        log.info(
            "=== [%s] done, returncode=%s ===",
            description,
            result.returncode,
        )
    except subprocess.CalledProcessError as e:
        log.exception(
            "Step '%s' failed with returncode=%s",
            description,
            e.returncode,
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    python_bin = sys.executable

    # 1) Telegram-инжест
    # ❗ Если модуль у тебя называется иначе — тут нужно будет поменять путь.
    # Я исхожу из варианта:
    #   src/telegram_ingest/fetch_telegram_posts.py
    #   и запуска:
    #   PYTHONPATH=src python -m telegram_ingest.fetch_telegram_posts
    run_step(
        "telegram_ingest",
        f"PYTHONPATH=src {python_bin} -m telegram_ingest.fetch_telegram_posts",
    )

    # 2) Wikipedia-инжест (режим bulk/daily настраивается через .env)
    run_step(
        "wikipedia_ingest",
        f"PYTHONPATH=src {python_bin} -m wikipedia_ingest.fetch_wikipedia_articles",
    )

    # 3) В будущем сюда же добавим другие источники:
    # run_step("rss_ingest", f"PYTHONPATH=src {python_bin} -m rss_ingest.run")
    # run_step("twitter_ingest", f"PYTHONPATH=src {python_bin} -m twitter_ingest.run")


if __name__ == "__main__":
    main()

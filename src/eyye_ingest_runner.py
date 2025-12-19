import logging
import subprocess
import sys
import os
import argparse
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
log = logging.getLogger(__name__)


def run_step(description: str, cmd: str) -> None:
    log.info("=== [%s] start ===", description)
    try:
        result = subprocess.run(cmd, shell=True, cwd=str(ROOT_DIR), check=True)
        log.info("=== [%s] done, returncode=%s ===", description, result.returncode)
    except subprocess.CalledProcessError as e:
        log.exception("Step '%s' failed with returncode=%s", description, e.returncode)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--steps",
        default=os.getenv("EYYE_INGEST_STEPS", "telegram,wikipedia,rss"),
        help="Comma-separated steps: telegram,wikipedia,rss (telegram включает fetch+process)",
    )
    args = parser.parse_args()

    steps = [s.strip().lower() for s in str(args.steps).split(",") if s.strip()]
    python_bin = sys.executable

    # ---- Telegram: fetch raw -> process batch -> cards ----
    if "telegram" in steps or "telegram_fetch" in steps:
        run_step("telegram_fetch_posts", f"PYTHONPATH=src {python_bin} -m telegram_ingest.fetch_telegram_posts")

    if "telegram" in steps or "telegram_process" in steps:
        run_step("telegram_process_posts", f"PYTHONPATH=src {python_bin} -m telegram_ingest.process_telegram_posts")

    # ---- Wikipedia ----
    if "wikipedia" in steps:
        run_step("wikipedia_ingest", f"PYTHONPATH=src {python_bin} -m wikipedia_ingest.fetch_wikipedia_articles")

    # ---- RSS / Google News RSS ----
    if "rss" in steps:
        run_step("rss_ingest", f"PYTHONPATH=src {python_bin} -m rss_ingest.fetch_rss_items")


if __name__ == "__main__":
    main()

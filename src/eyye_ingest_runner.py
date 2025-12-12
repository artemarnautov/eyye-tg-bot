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
        default=os.getenv("EYYE_INGEST_STEPS", "telegram,wikipedia"),
        help="Comma-separated steps: telegram,wikipedia",
    )
    args = parser.parse_args()

    steps = [s.strip().lower() for s in str(args.steps).split(",") if s.strip()]
    python_bin = sys.executable

    if "telegram" in steps:
        run_step("telegram_ingest", f"PYTHONPATH=src {python_bin} -m telegram_ingest.fetch_telegram_posts")

    if "wikipedia" in steps:
        run_step("wikipedia_ingest", f"PYTHONPATH=src {python_bin} -m wikipedia_ingest.fetch_wikipedia_articles")


if __name__ == "__main__":
    main()

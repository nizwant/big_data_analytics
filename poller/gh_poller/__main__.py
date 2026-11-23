"""Entry point: python -m gh_poller"""

import asyncio
import logging
import signal

from .config import load_config
from .github import UNAUTHORIZED_HELP, Unauthorized
from .runner import run


def _stop_on_sigterm() -> None:
    """docker stop sends SIGTERM: stop like Ctrl+C, so the outputs are flushed and SQLite is closed."""

    def handler(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, handler)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # our own line per request is enough
    cfg = load_config()
    _stop_on_sigterm()
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        logging.getLogger("gh_poller").info("stopped")
    except Unauthorized as exc:
        raise SystemExit(f"{UNAUTHORIZED_HELP} ({exc})") from None


if __name__ == "__main__":
    main()

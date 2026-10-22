"""Entry point: python -m gh_poller"""

import asyncio
import logging

from .config import load_config
from .github import UNAUTHORIZED_HELP, Unauthorized
from .runner import run


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # our own line per request is enough
    cfg = load_config()
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        logging.getLogger("gh_poller").info("stopped")
    except Unauthorized as exc:
        raise SystemExit(f"{UNAUTHORIZED_HELP} ({exc})") from None


if __name__ == "__main__":
    main()

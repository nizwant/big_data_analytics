"""Configuration from environment variables, optionally loaded from a .env file."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # python-dotenv is optional; plain environment variables work too
    load_dotenv = None

CRANTOGIT_URL = "https://raw.githubusercontent.com/r-universe-org/cran-to-git/master/crantogit.csv"


def _bool(value: str | None, default: bool) -> bool:
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    github_token: str
    data_dir: Path
    watchlist_path: Path
    global_track_enabled: bool = True
    global_max_pages: int = 3
    global_min_interval_s: float = 60
    watchlist_max_pages: int = 3
    tier1_interval_s: float = 120
    tier2_interval_s: float | None = None  # None: ceil(tier-2 timelines / tier2_budget_per_h) hours
    tier2_budget_per_h: float = 3500  # tier-2 requests per hour if every poll were a paid 200
    user_agent: str = "bda-cran-poller"
    api_url: str = "https://api.github.com"
    # Watch-list build (python -m gh_poller.watchlist)
    crantogit_url: str = CRANTOGIT_URL
    org_min_r_share: float = 0.5

    @property
    def events_dir(self) -> Path:
        return self.data_dir / "events"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "state.sqlite3"


def load_config(env_file: str | None = ".env") -> Config:
    if load_dotenv is not None and env_file and Path(env_file).exists():
        load_dotenv(env_file, override=False)
    env = os.environ

    token = env.get("GITHUB_TOKEN", "").strip()
    if not token:
        raise SystemExit("GITHUB_TOKEN is not set. Copy .env.example to .env and add your token.")

    data_dir = Path(env.get("DATA_DIR", "./data"))
    cfg = Config(
        github_token=token,
        data_dir=data_dir,
        watchlist_path=Path(env.get("WATCHLIST_PATH") or data_dir / "watchlist.csv"),
        global_track_enabled=_bool(env.get("GLOBAL_TRACK_ENABLED"), True),
        global_max_pages=int(env.get("GLOBAL_MAX_PAGES", "3")),
        global_min_interval_s=float(env.get("GLOBAL_MIN_INTERVAL_S", "60")),
        watchlist_max_pages=int(env.get("WATCHLIST_MAX_PAGES", "3")),
        tier1_interval_s=float(env.get("TIER1_INTERVAL_S", "120")),
        tier2_interval_s=float(env["TIER2_INTERVAL_S"]) if env.get("TIER2_INTERVAL_S", "").strip() else None,
        tier2_budget_per_h=float(env.get("TIER2_BUDGET_PER_H", "3500")),
        user_agent=env.get("USER_AGENT", "bda-cran-poller"),
        api_url=env.get("GITHUB_API_URL", "https://api.github.com").rstrip("/"),
        crantogit_url=env.get("CRANTOGIT_URL", CRANTOGIT_URL),
        org_min_r_share=float(env.get("ORG_MIN_R_SHARE", "0.5")),
    )
    problems = []
    if cfg.global_min_interval_s < 60 or cfg.tier1_interval_s < 60:
        problems.append("GLOBAL_MIN_INTERVAL_S and TIER1_INTERVAL_S must be at least 60 (GitHub's X-Poll-Interval)")
    if cfg.tier2_interval_s is not None and cfg.tier2_interval_s < 900:
        problems.append("TIER2_INTERVAL_S must be at least 900, or empty for the automatic interval")
    if not 0 < cfg.tier2_budget_per_h < 5000:
        problems.append("TIER2_BUDGET_PER_H must be above 0 and below the 5,000 requests/h quota")
    if not 0 <= cfg.org_min_r_share <= 1:
        problems.append("ORG_MIN_R_SHARE must be between 0 and 1")
    if not 1 <= cfg.global_max_pages <= 3 or not 1 <= cfg.watchlist_max_pages <= 3:
        problems.append("GLOBAL_MAX_PAGES and WATCHLIST_MAX_PAGES must be 1-3 (GitHub serves at most 300 events)")
    if problems:
        raise SystemExit("Invalid configuration: " + "; ".join(problems))
    return cfg

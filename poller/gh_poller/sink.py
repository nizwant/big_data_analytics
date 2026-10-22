"""Output: one record per new event, appended to a daily JSONL file."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


def make_record(
    event: dict,
    *,
    track: str,
    endpoint: str,
    fetched_at: str,
    packages_for: Callable[[str | None], list[str]] = lambda _repo: [],
) -> dict:
    """The only place that defines the output format. `event` stays raw, so later formats can be derived."""
    repo = event.get("repo")
    repo_name = repo.get("name") if isinstance(repo, dict) else None
    return {
        "fetched_at": fetched_at,
        "track": track,
        "endpoint": endpoint,
        "packages": packages_for(repo_name),
        "event": event,
    }


class DailyJsonl:
    """Appends JSON lines to <directory>/<prefix><YYYY-MM-DD>.jsonl (UTC date)."""

    def __init__(self, directory: str | Path, prefix: str = ""):
        self.directory = Path(directory)
        self.prefix = prefix
        self.directory.mkdir(parents=True, exist_ok=True)

    def path_for(self, day: str) -> Path:
        return self.directory / f"{self.prefix}{day}.jsonl"

    def write(self, records: Iterable[dict], day: str | None = None) -> int:
        records = list(records)
        if not records:
            return 0
        day = day or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with open(self.path_for(day), "a", encoding="utf-8", newline="\n") as fh:
            for record in records:
                fh.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        return len(records)

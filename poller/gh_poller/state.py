"""Poller state in SQLite: ETags per endpoint and ids of events already written (deduplication)."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS endpoint_state (
    endpoint      TEXT PRIMARY KEY,
    etag          TEXT,
    last_status   INTEGER,
    last_poll_at  REAL
);
CREATE TABLE IF NOT EXISTS seen_events (
    id          TEXT PRIMARY KEY,
    keep_until  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS seen_events_keep_until ON seen_events(keep_until);
"""


class State:
    def __init__(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def get_etag(self, endpoint: str) -> str | None:
        row = self._conn.execute("SELECT etag FROM endpoint_state WHERE endpoint = ?", (endpoint,)).fetchone()
        return row[0] if row else None

    def is_seen(self, event_id: str) -> bool:
        return self._conn.execute("SELECT 1 FROM seen_events WHERE id = ?", (event_id,)).fetchone() is not None

    def filter_new(self, events: Iterable[dict]) -> list[dict]:
        """Events not written before, without duplicates inside the batch; order is kept."""
        out: list[dict] = []
        batch: set[str] = set()
        for event in events:
            event_id = str(event.get("id"))
            if event_id in batch or self.is_seen(event_id):
                continue
            batch.add(event_id)
            out.append(event)
        return out

    def commit_poll(
        self,
        endpoint: str,
        *,
        etag: str | None,
        status: int | None,
        event_ids: Iterable[str],
        keep_s: float,
        now: float | None = None,
    ) -> None:
        """Record one poll in one transaction. Pass every returned id: retention only ever grows to `keep_s`."""
        now = time.time() if now is None else now
        keep_until = now + keep_s
        with self._conn:
            self._conn.executemany(
                "INSERT INTO seen_events(id, keep_until) VALUES (?, ?) "
                "ON CONFLICT(id) DO UPDATE SET keep_until = MAX(keep_until, excluded.keep_until)",
                [(str(i), keep_until) for i in event_ids],
            )
            self._conn.execute(
                "INSERT INTO endpoint_state(endpoint, etag, last_status, last_poll_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(endpoint) DO UPDATE SET etag = excluded.etag, "
                "last_status = excluded.last_status, last_poll_at = excluded.last_poll_at",
                (endpoint, etag, status, now),
            )

    def record_dead(self, endpoint: str, status: int, now: float | None = None) -> None:
        """404/410: remember it, so a restart does not poll the timeline again before the weekly recheck."""
        now = time.time() if now is None else now
        with self._conn:
            self._conn.execute(
                "INSERT INTO endpoint_state(endpoint, etag, last_status, last_poll_at) VALUES (?, NULL, ?, ?) "
                "ON CONFLICT(endpoint) DO UPDATE SET etag = NULL, "
                "last_status = excluded.last_status, last_poll_at = excluded.last_poll_at",
                (endpoint, status, now),
            )

    def endpoint_history(self) -> dict[str, tuple[int | None, float | None]]:
        """endpoint -> (last status, last poll time), to schedule the first poll after a restart."""
        rows = self._conn.execute("SELECT endpoint, last_status, last_poll_at FROM endpoint_state")
        return {endpoint: (status, polled) for endpoint, status, polled in rows}

    def prune(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        with self._conn:
            cur = self._conn.execute("DELETE FROM seen_events WHERE keep_until < ?", (now,))
        return cur.rowcount

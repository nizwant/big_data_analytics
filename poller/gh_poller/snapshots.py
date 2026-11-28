"""Watch-list snapshots in the Kafka topic github_repo_list.

Every watch-list build sends one message per row, all with the same `built_at` and with
`rows_in_snapshot`, the number of rows of that build. The rows of one build are spread over the
topic's partitions, so a snapshot is complete once that many distinct rows with its `built_at`
have been read. A build that failed while publishing leaves an incomplete snapshot, which is
never used. Readers take the newest complete snapshot (`built_at` is UTC in ISO 8601, so it
sorts as text).

Readers assign every partition from the beginning and never commit offsets: the topic's
retention (30 days, deploy/kafka_init.sh) bounds what they read.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, TopicPartition

log = logging.getLogger("gh_poller")

ROW_FIELDS = ("package", "owner", "repo", "endpoint", "tier")
MAX_BATCHES_PER_READ = 100  # 100 x 1,000 messages, so one read() cannot run forever on a busy topic


class SnapshotReadError(Exception):
    """The topic could not be read to its end (Kafka unreachable or the topic missing)."""


@dataclass(frozen=True)
class Snapshot:
    built_at: str
    rows: list[dict]  # package, owner, repo, endpoint, tier: the columns of watchlist.csv


@dataclass
class _Partial:
    expected: int
    rows: dict[tuple, dict] = field(default_factory=dict)  # repeated rows count once


class SnapshotCollector:
    """Groups watch-list messages by build and keeps the newest complete build."""

    def __init__(self) -> None:
        self.latest: Snapshot | None = None
        self._partial: dict[str, _Partial] = {}
        self.skipped = 0  # not a snapshot row, e.g. written before rows_in_snapshot existed

    def add(self, value: bytes | str | None) -> None:
        try:
            record = json.loads(value)
            built_at, expected = record["built_at"], record["rows_in_snapshot"]
            row = {k: record[k] for k in ROW_FIELDS}
        except (TypeError, ValueError, KeyError):
            self.skipped += 1
            return
        if not isinstance(built_at, str) or not isinstance(expected, int) or expected < 1:
            self.skipped += 1
            return
        if self.latest is not None and built_at <= self.latest.built_at:
            return  # an older build, or a repeat of a row of the newest one
        partial = self._partial.setdefault(built_at, _Partial(expected))
        partial.rows[tuple(str(row[k]) for k in ROW_FIELDS)] = row
        if len(partial.rows) >= partial.expected:
            self.latest = Snapshot(built_at, list(partial.rows.values()))
            self._partial = {b: p for b, p in self._partial.items() if b > built_at}


class WatchlistFeed:
    """Reads github_repo_list without blocking, except for the metadata request before the first read.

    `synced` turns true once every partition has been read to its end; before that, an older
    snapshot could still be replaced by a newer one further on, so read() returns None.
    """

    def __init__(self, bootstrap_servers: str, topic: str, *, consumer=None, metadata_timeout_s: float = 5.0):
        self.topic = topic
        self.metadata_timeout_s = metadata_timeout_s
        if consumer is None:
            consumer = Consumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "bda-github-poller",
                    # Required by the client; partitions are assigned by hand and offsets never committed.
                    "group.id": "bda-github-watchlist-reader",
                    "enable.auto.commit": False,
                    "enable.partition.eof": True,
                }
            )
        self.consumer = consumer
        self.collector = SnapshotCollector()
        self.synced = False
        self._partitions: set[int] | None = None
        self._at_end: set[int] = set()

    def read(self) -> Snapshot | None:
        """Take the messages fetched so far; the newest complete snapshot once synced, else None."""
        if self._partitions is None and not self._assign():
            return None
        try:
            for _ in range(MAX_BATCHES_PER_READ):
                messages = self.consumer.consume(num_messages=1000, timeout=0)
                if not messages:
                    break
                for msg in messages:
                    self._take(msg)
        except KafkaException as exc:
            log.warning("reading %s failed, will retry: %s", self.topic, exc)
        if not self.synced and self._at_end >= self._partitions:
            self.synced = True
            if self.collector.skipped:
                log.info("%s: skipped %d messages that are not snapshot rows", self.topic, self.collector.skipped)
        return self.collector.latest if self.synced else None

    def _take(self, msg) -> None:
        err = msg.error()
        if err is None:
            self._at_end.discard(msg.partition())
            self.collector.add(msg.value())
        elif err.code() == KafkaError._PARTITION_EOF:
            self._at_end.add(msg.partition())
        else:
            log.warning("reading %s: %s", self.topic, err)

    def _assign(self) -> bool:
        try:
            meta = self.consumer.list_topics(self.topic, timeout=self.metadata_timeout_s)
        except KafkaException as exc:
            log.warning("Kafka not reachable yet, will retry reading %s: %s", self.topic, exc)
            return False
        topic = meta.topics.get(self.topic)
        if topic is None or topic.error is not None or not topic.partitions:
            log.warning("Kafka topic %s does not exist yet; run deploy/init.sh", self.topic)
            return False
        self._partitions = set(topic.partitions)
        self.consumer.assign([TopicPartition(self.topic, p, OFFSET_BEGINNING) for p in sorted(self._partitions)])
        return True

    def close(self) -> None:
        self.consumer.close()


def read_latest_snapshot(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout_s: float = 120.0,
    consumer=None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> Snapshot | None:
    """Read the whole topic; its newest complete snapshot, or None if it has none."""
    feed = WatchlistFeed(bootstrap_servers, topic, consumer=consumer)
    deadline = now() + timeout_s
    try:
        while True:
            latest = feed.read()
            if feed.synced:
                return latest
            if now() >= deadline:
                raise SnapshotReadError(f"{topic} not read to its end within {timeout_s:g} s; is Kafka reachable?")
            sleep(0.2)
    finally:
        feed.close()

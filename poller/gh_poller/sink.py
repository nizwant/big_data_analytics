"""Output: one record per new event, sent to Kafka and/or appended to a daily JSONL file."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from confluent_kafka import KafkaError, KafkaException, Producer

log = logging.getLogger("gh_poller")


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


class SinkError(Exception):
    """Events were not delivered; the poll's state must not be committed, so they are fetched again."""


def _repo_name(record: dict) -> str | None:
    repo = record.get("event", {}).get("repo")
    return repo.get("name") if isinstance(repo, dict) else None


class KafkaSink:
    """Sends each record as JSON to one Kafka topic, keyed by `event.repo.name` (or by `key_of`).

    write() returns only after the broker confirmed every message (acks=all), so the caller
    commits state only for delivered events. A message larger than the broker accepts is
    logged and dropped instead of blocking that timeline forever.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        *,
        producer=None,
        flush_timeout_s: float = 20.0,
        key_of: Callable[[dict], str | None] = _repo_name,
    ):
        self.topic = topic
        self.key_of = key_of
        self.flush_timeout_s = flush_timeout_s
        if producer is None:
            producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "bda-github-poller",
                    "acks": "all",
                    "enable.idempotence": True,
                    "compression.type": "zstd",
                    "linger.ms": 5,  # write() flushes right away; batching happens within one poll
                    "message.timeout.ms": 15000,  # Kafka down: fail the poll after 15 s, retry later
                }
            )
        self.producer = producer
        self.dropped = 0

    def check_topic(self, timeout_s: float = 10.0) -> bool:
        """True if the broker knows the topic (topic auto-creation is disabled on the cluster)."""
        try:
            meta = self.producer.list_topics(self.topic, timeout=timeout_s)
        except Exception as exc:  # broker not reachable yet: the poller retries on every write
            log.warning("Kafka not reachable yet (%s); events wait in GitHub until it is", exc)
            return False
        topic = meta.topics.get(self.topic)
        if topic is None or topic.error is not None:
            log.warning("Kafka topic %s does not exist yet; run deploy/init.sh", self.topic)
            return False
        return True

    def write(self, records: Iterable[dict], day: str | None = None) -> int:
        errors: list[str] = []

        def on_delivery(err, msg) -> None:
            if err is None:
                return
            if err.code() == KafkaError.MSG_SIZE_TOO_LARGE:
                self._drop(msg.key(), err)
            else:
                errors.append(str(err))

        sent = 0
        for record in records:
            key = self.key_of(record)
            value = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            if self._produce(key, value, on_delivery):
                sent += 1
        if sent == 0:
            return 0
        remaining = self.producer.flush(self.flush_timeout_s)
        if remaining:
            errors.append(f"{remaining} messages not confirmed within {self.flush_timeout_s:g} s")
        if errors:
            raise SinkError("kafka: " + "; ".join(sorted(set(errors))[:3]))
        return sent

    def _produce(self, key: str | None, value: bytes, on_delivery) -> bool:
        """Queue one message; False if it was dropped as too large."""
        while True:
            try:
                self.producer.produce(
                    self.topic,
                    key=key.encode("utf-8") if key else None,
                    value=value,
                    on_delivery=on_delivery,
                )
                return True
            except BufferError:  # local queue full: let it drain, then retry
                self.producer.poll(1.0)
            except KafkaException as exc:
                if exc.args and exc.args[0].code() == KafkaError.MSG_SIZE_TOO_LARGE:
                    self._drop(key, exc.args[0])
                    return False
                raise SinkError(f"kafka: {exc}") from exc

    def _drop(self, key, err) -> None:
        self.dropped += 1
        if isinstance(key, bytes):
            key = key.decode("utf-8", "replace")
        log.warning("event of %s dropped: larger than Kafka accepts (%s)", key, err)

    def close(self) -> None:
        self.producer.flush(self.flush_timeout_s)


class EventSinks:
    """Writes the new events to every configured output: Kafka first, then the JSONL file.

    If Kafka fails, nothing is written to the file either, so a retried poll does not leave
    duplicates there; if the file fails after Kafka, the retry re-sends (at least once).
    """

    def __init__(self, sinks: list):
        if not sinks:
            raise ValueError("at least one output is needed")
        self.sinks = sorted(sinks, key=lambda s: not isinstance(s, KafkaSink))

    def write(self, records: Iterable[dict], day: str | None = None) -> int:
        records = list(records)
        if not records:
            return 0
        for sink in self.sinks:
            try:
                sink.write(records, day=day)
            except SinkError:
                raise
            except Exception as exc:  # e.g. disk full: same handling as a Kafka failure
                raise SinkError(f"{type(sink).__name__}: {exc}") from exc
        return len(records)

    def close(self) -> None:
        for sink in self.sinks:
            close = getattr(sink, "close", None)
            if close is not None:
                close()

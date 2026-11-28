"""Shared test helpers."""

import asyncio
import heapq
import itertools
import json
from pathlib import Path
from types import SimpleNamespace

from confluent_kafka import KafkaError

from gh_poller.config import Config

API = "https://api.github.com"


def make_cfg(tmp_path: Path) -> Config:
    return Config(
        github_token="t",
        data_dir=tmp_path / "data",
        watchlist_path=tmp_path / "watchlist.csv",
        user_agent="test",
        api_url=API,
        global_track_enabled=True,
    )


def read_jsonl(directory: Path) -> list[dict]:
    rows: list[dict] = []
    for f in sorted(directory.glob("*.jsonl")):
        rows += [json.loads(line) for line in f.read_text(encoding="utf-8").splitlines()]
    return rows


class VirtualClock:
    """Virtual time for asyncio code under test.

    sleep() parks the caller. run() lets every task run until all are parked, then moves the time
    to the earliest wake-up, so hours of scheduling take milliseconds and are deterministic.
    """

    YIELDS = 30  # event-loop turns given to the tasks before the time moves on

    def __init__(self, start: float = 1_800_000_000.0):
        self.t = start
        self._sleepers: list = []
        self._seq = itertools.count()

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        fut = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self.t + max(0.0, seconds), next(self._seq), fut))
        await fut

    async def run(self, coro, until: float):
        """Run `coro` until it finishes or the virtual time reaches `until`; returns its result."""
        real_sleep = asyncio.sleep
        task = asyncio.ensure_future(coro)
        try:
            while not task.done():
                for _ in range(self.YIELDS):
                    await real_sleep(0)
                if task.done():
                    break
                while self._sleepers and self._sleepers[0][2].done():
                    heapq.heappop(self._sleepers)  # cancelled sleepers
                if not self._sleepers or self._sleepers[0][0] > until:
                    self.t = max(self.t, until)
                    break
                self.t = max(self.t, self._sleepers[0][0])
                while self._sleepers and self._sleepers[0][0] <= self.t:
                    fut = heapq.heappop(self._sleepers)[2]
                    if not fut.done():
                        fut.set_result(None)
        finally:
            if not task.done():
                task.cancel()
        try:
            return await task
        except asyncio.CancelledError:
            return None


class FakeKafkaMessage:
    """A consumed message, or the end-of-partition marker that enable.partition.eof produces."""

    def __init__(self, partition: int, value: bytes | None = None, eof: bool = False):
        self._partition = partition
        self._value = value
        self._eof = eof

    def error(self):
        return KafkaError(KafkaError._PARTITION_EOF) if self._eof else None

    def partition(self) -> int:
        return self._partition

    def value(self) -> bytes | None:
        return self._value


class FakeConsumer:
    """Hands out the queued batches one fetch at a time: a batch, then nothing (the local queue is
    empty until the next fetch), so each WatchlistFeed.read() takes exactly one batch."""

    def __init__(self, partitions=(0, 1, 2), topic_exists: bool = True):
        self.partitions = tuple(partitions)
        self.topic_exists = topic_exists
        self.batches: list[list[FakeKafkaMessage]] = []
        self.assigned: list[tuple] | None = None
        self.closed = False
        self._fetched = False

    def add_batch(self, messages) -> None:
        self.batches.append(list(messages))

    def list_topics(self, topic, timeout=None):
        if not self.topic_exists:
            return SimpleNamespace(topics={})
        return SimpleNamespace(topics={topic: SimpleNamespace(partitions={p: None for p in self.partitions}, error=None)})

    def assign(self, partitions) -> None:
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in partitions]

    def consume(self, num_messages=1, timeout=-1):
        if self._fetched or not self.batches:
            self._fetched = False
            return []
        self._fetched = True
        return self.batches.pop(0)

    def close(self) -> None:
        self.closed = True


def kafka_messages(records: list[dict], partitions=(0, 1, 2)) -> list[FakeKafkaMessage]:
    """The records as JSON messages, spread over the partitions in turn."""
    return [
        FakeKafkaMessage(partitions[i % len(partitions)], json.dumps(r).encode("utf-8")) for i, r in enumerate(records)
    ]


def end_of(*partitions: int) -> list[FakeKafkaMessage]:
    return [FakeKafkaMessage(p, eof=True) for p in partitions]

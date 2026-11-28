"""Watch-list snapshots in github_repo_list: completeness, newest wins, reading to the end."""

import json

import pytest
from confluent_kafka import OFFSET_BEGINNING

from gh_poller.snapshots import SnapshotCollector, SnapshotReadError, WatchlistFeed, read_latest_snapshot
from gh_poller.watchlist import Row, watchlist_records

from helpers import FakeConsumer, end_of, kafka_messages

TOPIC = "github_repo_list"
ROWS = [
    Row("dplyr", "tidyverse", "dplyr", "/orgs/tidyverse/events", 1),
    Row("", "cran", "", "/orgs/cran/events", 1),
    Row("solo", "alice", "solo", "/repos/alice/solo/events", 2),
]


def values(rows, built_at) -> list[bytes]:
    return [json.dumps(r).encode() for r in watchlist_records(rows, built_at)]


def test_a_snapshot_counts_only_once_all_its_rows_are_read():
    c = SnapshotCollector()
    first, *rest = values(ROWS, "2026-11-27T04:00:05Z")
    c.add(first)
    c.add(first)  # a repeated row does not complete the snapshot
    assert c.latest is None
    for v in rest:
        c.add(v)
    assert c.latest.built_at == "2026-11-27T04:00:05Z"
    assert sorted(r["endpoint"] for r in c.latest.rows) == sorted(r.endpoint for r in ROWS)


def test_newest_complete_snapshot_wins_and_a_partial_one_never_does():
    c = SnapshotCollector()
    for v in values(ROWS, "2026-11-26T04:00:00Z"):
        c.add(v)
    for v in values(ROWS[:2], "2026-11-28T04:00:00Z")[:1]:  # its publishing failed after one row
        c.add(v)
    assert c.latest.built_at == "2026-11-26T04:00:00Z"
    for v in values(ROWS[:2], "2026-11-27T04:00:00Z"):
        c.add(v)
    assert c.latest.built_at == "2026-11-27T04:00:00Z" and len(c.latest.rows) == 2
    for v in values(ROWS, "2026-11-25T04:00:00Z"):  # an older build read later (another partition)
        c.add(v)
    assert c.latest.built_at == "2026-11-27T04:00:00Z"


def test_messages_that_are_not_snapshot_rows_are_skipped():
    c = SnapshotCollector()
    old_format = {"built_at": "2026-11-20T04:00:00Z", "package": "dplyr", "owner": "tidyverse", "repo": "dplyr",
                  "endpoint": "/orgs/tidyverse/events", "tier": 1}  # before rows_in_snapshot existed
    for v in (json.dumps(old_format).encode(), b"not json", None, json.dumps({**old_format, "rows_in_snapshot": 0}).encode()):
        c.add(v)
    assert c.latest is None and c.skipped == 4


def test_feed_reads_every_partition_from_the_beginning_and_waits_for_their_ends():
    consumer = FakeConsumer()
    consumer.add_batch(kafka_messages(watchlist_records(ROWS, "2026-11-27T04:00:00Z")) + end_of(0, 1))
    consumer.add_batch(end_of(2))
    feed = WatchlistFeed("kafka:19092", TOPIC, consumer=consumer)

    assert feed.read() is None and not feed.synced  # complete, but partition 2 may hold a newer one
    assert consumer.assigned == [(TOPIC, p, OFFSET_BEGINNING) for p in (0, 1, 2)]
    assert feed.read().built_at == "2026-11-27T04:00:00Z" and feed.synced

    consumer.add_batch(kafka_messages(watchlist_records(ROWS[:1], "2026-11-28T04:00:00Z")))
    assert feed.read().built_at == "2026-11-28T04:00:00Z"  # after the sync, new builds are taken at once
    feed.close()
    assert consumer.closed


def test_feed_retries_until_the_topic_exists():
    consumer = FakeConsumer(topic_exists=False)
    feed = WatchlistFeed("kafka:19092", TOPIC, consumer=consumer)
    assert feed.read() is None and consumer.assigned is None
    consumer.topic_exists = True
    consumer.add_batch(end_of(0, 1, 2))
    assert feed.read() is None and feed.synced  # an empty topic: synced, no snapshot


def test_read_latest_snapshot_reads_to_the_end():
    consumer = FakeConsumer(partitions=(0,))
    consumer.add_batch(kafka_messages(watchlist_records(ROWS, "2026-11-26T04:00:00Z"), partitions=(0,)))
    consumer.add_batch(kafka_messages(watchlist_records(ROWS[:2], "2026-11-27T04:00:00Z"), partitions=(0,)) + end_of(0))
    latest = read_latest_snapshot("kafka:19092", TOPIC, consumer=consumer, sleep=lambda s: None)
    assert latest.built_at == "2026-11-27T04:00:00Z" and len(latest.rows) == 2
    assert consumer.closed


def test_read_latest_snapshot_gives_up_when_kafka_does_not_answer():
    t = [0.0]

    def sleep(s):
        t[0] += s

    consumer = FakeConsumer(topic_exists=False)
    with pytest.raises(SnapshotReadError, match="not read to its end within 5 s"):
        read_latest_snapshot("kafka:19092", TOPIC, timeout_s=5, consumer=consumer, sleep=sleep, now=lambda: t[0])
    assert consumer.closed

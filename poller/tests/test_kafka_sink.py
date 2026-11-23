"""Kafka output with a fake producer: delivery confirmation decides whether state is committed."""

import asyncio
import json

import httpx
import pytest
import respx
from confluent_kafka import KafkaError, KafkaException

from gh_poller.github import build_headers
from gh_poller.runner import SINK_RETRY_S, Context, Pause, poll_and_store
from gh_poller.sink import DailyJsonl, EventSinks, KafkaSink, SinkError, make_record
from gh_poller.state import State

from helpers import API, make_cfg, read_jsonl


class FakeMsg:
    def __init__(self, key):
        self._key = key

    def key(self):
        return self._key


class FakeProducer:
    """Records produced messages; `fail` sets the delivery error reported on flush."""

    def __init__(self, fail=None, too_large_keys=(), unconfirmed=0):
        self.sent = []
        self.pending = []
        self.fail = fail
        self.too_large_keys = set(too_large_keys)
        self.unconfirmed = unconfirmed

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if key in self.too_large_keys:
            raise KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
        self.pending.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout=None):
        if self.unconfirmed:
            return self.unconfirmed
        for topic, key, value, cb in self.pending:
            if self.fail is None:
                self.sent.append((topic, key, json.loads(value)))
            cb(KafkaError(self.fail) if self.fail is not None else None, FakeMsg(key))
        self.pending = []
        return 0


def record(event_id, repo="tidyverse/dplyr"):
    event = {"id": event_id, "type": "PushEvent", "repo": {"name": repo}}
    return make_record(event, track="watchlist", endpoint="/orgs/tidyverse/events", fetched_at="2026-11-22T18:00:00Z")


def test_records_go_to_the_topic_keyed_by_repo():
    producer = FakeProducer()
    sink = KafkaSink("kafka:19092", "github_repo_updates", producer=producer)
    assert sink.write([record("1"), record("2", "cran/dplyr")]) == 2
    assert [(t, k) for t, k, _ in producer.sent] == [
        ("github_repo_updates", b"tidyverse/dplyr"),
        ("github_repo_updates", b"cran/dplyr"),
    ]
    assert producer.sent[0][2] == record("1")  # value is the same record as in the JSONL file


def test_failed_delivery_raises():
    sink = KafkaSink("x", "github_repo_updates", producer=FakeProducer(fail=KafkaError._MSG_TIMED_OUT))
    with pytest.raises(SinkError, match="kafka"):
        sink.write([record("1")])


def test_unconfirmed_messages_raise():
    sink = KafkaSink("x", "github_repo_updates", producer=FakeProducer(unconfirmed=1), flush_timeout_s=0.1)
    with pytest.raises(SinkError, match="not confirmed"):
        sink.write([record("1")])


def test_too_large_message_is_dropped_not_retried_forever():
    producer = FakeProducer(too_large_keys={b"huge/repo"})
    sink = KafkaSink("x", "github_repo_updates", producer=producer)
    assert sink.write([record("1", "huge/repo"), record("2")]) == 1
    assert sink.dropped == 1 and len(producer.sent) == 1


def test_broker_rejecting_size_is_dropped_too():
    producer = FakeProducer(fail=KafkaError.MSG_SIZE_TOO_LARGE)
    sink = KafkaSink("x", "github_repo_updates", producer=producer)
    sink.write([record("1")])
    assert sink.dropped == 1


def test_kafka_failure_writes_nothing_to_jsonl(tmp_path):
    jsonl = DailyJsonl(tmp_path)
    sinks = EventSinks([jsonl, KafkaSink("x", "t", producer=FakeProducer(fail=KafkaError._TRANSPORT))])
    with pytest.raises(SinkError):
        sinks.write([record("1")], day="2026-11-22")
    assert read_jsonl(tmp_path) == []


@respx.mock
def test_poll_commits_only_after_delivery(tmp_path):
    cfg = make_cfg(tmp_path)
    events = [{"id": "2", "type": "CreateEvent", "repo": {"name": "r-lib/cli"}}, {"id": "1", "type": "PushEvent", "repo": {"name": "r-lib/cli"}}]
    route = respx.get(f"{API}/orgs/r-lib/events").mock(
        return_value=httpx.Response(200, json=events, headers={"ETag": 'W/"e1"', "X-Poll-Interval": "60"})
    )
    producer = FakeProducer(fail=KafkaError._TRANSPORT)

    async def go():
        state = State(cfg.db_path)
        pause = Pause()
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            sinks = EventSinks([KafkaSink("x", "github_repo_updates", producer=producer)])
            ctx = Context(cfg, client, state, sinks, DailyJsonl(cfg.logs_dir, "requests-"), pause)
            failed = await poll_and_store(ctx, "/orgs/r-lib/events", "watchlist", 3, 120)
            after_failure = (state.get_etag("/orgs/r-lib/events"), state.is_seen("1"), pause.until - ctx.clock.now())
            producer.fail = None  # Kafka is back
            pause.until = 0
            ok = await poll_and_store(ctx, "/orgs/r-lib/events", "watchlist", 3, 120)
            after_ok = (state.get_etag("/orgs/r-lib/events"), state.is_seen("1"))
        state.close()
        return failed, after_failure, ok, after_ok

    failed, after_failure, ok, after_ok = asyncio.run(go())

    assert failed[1] == 0
    assert after_failure[:2] == (None, False)  # nothing committed: the next poll fetches the events again
    assert after_failure[2] == pytest.approx(SINK_RETRY_S, abs=1)  # all polling pauses
    assert route.calls[1].request.headers.get("If-None-Match") is None
    assert ok[1] == 2 and after_ok == ('W/"e1"', True)
    assert [k for _, k, _ in producer.sent] == [b"r-lib/cli", b"r-lib/cli"]
    assert [r["event"]["id"] for _, _, r in producer.sent] == ["1", "2"]  # oldest first

    log = read_jsonl(cfg.logs_dir)
    assert "kafka" in log[0]["error"] and log[0]["new_events"] == 0
    assert log[1]["error"] is None and log[1]["new_events"] == 2

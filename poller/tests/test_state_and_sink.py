import json

from gh_poller.sink import DailyJsonl, make_record
from gh_poller.state import State

DAY = 86400


def test_filter_new_dedups_within_batch_and_against_db(tmp_path):
    state = State(tmp_path / "s.sqlite3")
    state.commit_poll("/events", etag="e1", status=200, event_ids=["1"], keep_s=DAY)
    new = state.filter_new([{"id": "2"}, {"id": "1"}, {"id": "2"}, {"id": "3"}])
    assert [e["id"] for e in new] == ["2", "3"]


def test_state_survives_restart(tmp_path):
    path = tmp_path / "s.sqlite3"
    state = State(path)
    state.commit_poll("/orgs/cran/events", etag='W/"x"', status=200, event_ids=["10", "11"], keep_s=DAY)
    state.close()

    again = State(path)
    assert again.get_etag("/orgs/cran/events") == 'W/"x"'
    assert again.is_seen("10") and again.is_seen("11")
    assert not again.is_seen("12")


def test_slow_timeline_extends_retention_of_ids_first_seen_globally(tmp_path):
    state = State(tmp_path / "s.sqlite3")
    t0 = 1_800_000_000.0
    state.commit_poll("/events", etag=None, status=200, event_ids=["7"], keep_s=2 * DAY, now=t0)
    state.commit_poll("/orgs/r-lib/events", etag=None, status=200, event_ids=["7"], keep_s=31 * DAY, now=t0 + 60)

    assert state.prune(now=t0 + 3 * DAY) == 0
    assert state.is_seen("7")
    assert state.prune(now=t0 + 32 * DAY) == 1
    assert not state.is_seen("7")


def test_sink_keeps_raw_event_unchanged(tmp_path):
    raw = {"id": "42", "type": "ReleaseEvent", "repo": {"name": "tidyverse/dplyr"}, "payload": {"x": "zażółć"}}
    sink = DailyJsonl(tmp_path / "events")
    lookup = {"tidyverse/dplyr": ["dplyr"]}
    rec = make_record(
        raw, track="watchlist", endpoint="/orgs/tidyverse/events", fetched_at="2026-10-21T21:00:00Z",
        packages_for=lambda name: lookup.get(name, []),
    )
    assert sink.write([rec], day="2026-10-21") == 1

    lines = (tmp_path / "events" / "2026-10-21.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    stored = json.loads(lines[0])
    assert stored["event"] == raw
    assert stored["track"] == "watchlist"
    assert stored["endpoint"] == "/orgs/tidyverse/events"
    assert stored["packages"] == ["dplyr"]
    assert list(stored) == ["fetched_at", "track", "endpoint", "packages", "event"]


def test_record_without_watchlist_match_has_empty_packages():
    for event in ({"id": "1", "repo": {"name": "someone/website"}}, {"id": "2"}, {"id": "3", "repo": None}):
        assert make_record(event, track="global", endpoint="/events", fetched_at="t")["packages"] == []

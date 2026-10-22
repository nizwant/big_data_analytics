import asyncio
import sqlite3
import time

import httpx
import pytest
import respx

from gh_poller import github
from gh_poller.github import build_headers
from gh_poller.runner import Context, Pause, poll_and_store, prune_loop
from gh_poller.sink import DailyJsonl
from gh_poller.state import State
from gh_poller.watchlist import Watchlist

from helpers import API, make_cfg, read_jsonl

DAY = 86400


@respx.mock
def test_poll_write_then_304_and_dedup_across_tracks(tmp_path):
    cfg = make_cfg(tmp_path)
    events = [
        {"id": "2", "type": "CreateEvent", "created_at": "2026-10-21T12:00:05Z"},
        {"id": "1", "type": "PushEvent", "created_at": "2026-10-21T12:00:00Z"},
    ]
    org = respx.get(f"{API}/orgs/r-lib/events").mock(
        side_effect=[
            httpx.Response(200, json=events, headers={"ETag": 'W/"e1"', "X-Poll-Interval": "60"}),
            httpx.Response(304, headers={"X-Poll-Interval": "60"}),
        ]
    )
    respx.get(f"{API}/events").mock(return_value=httpx.Response(200, json=[events[0], {"id": "3", "type": "WatchEvent"}]))

    async def go():
        state = State(cfg.db_path)
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            ctx = Context(cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause())
            first = await poll_and_store(ctx, "/orgs/r-lib/events", "watchlist", 3, 120)
            second = await poll_and_store(ctx, "/orgs/r-lib/events", "watchlist", 3, 120)
            glob = await poll_and_store(ctx, "/events", "global", 3, 60)
        state.close()
        return first, second, glob

    first, second, glob = asyncio.run(go())

    assert first[1] == 2 and first[2] == 120
    assert org.calls[1].request.headers["If-None-Match"] == 'W/"e1"'
    assert second[0].status == 304 and second[1] == 0
    assert glob[1] == 1  # id 2 already written via the watch-list, only id 3 is new

    written = read_jsonl(cfg.events_dir)
    assert [r["event"]["id"] for r in written] == ["1", "2", "3"]  # oldest first within a poll
    assert [r["track"] for r in written] == ["watchlist", "watchlist", "global"]

    log = read_jsonl(cfg.logs_dir)
    assert [r["status"] for r in log] == [200, 304, 200]
    assert [r["new_events"] for r in log] == [2, 0, 1]
    assert [r["known"] for r in log] == [0, 0, 1]  # the global poll returned id 2 again
    assert [r["dup_in_poll"] for r in log] == [0, 0, 0]


@respx.mock
def test_event_repeated_on_next_page_is_written_once_and_counted(tmp_path):
    # The global feed can move between page requests, so page 2 may repeat the end of page 1.
    page1 = [{"id": str(i), "type": "PushEvent"} for i in range(200, 100, -1)]
    page2 = [{"id": str(i), "type": "PushEvent"} for i in range(101, 1, -1)]  # starts with 101 again

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json=page2)
        return httpx.Response(200, json=page1, headers={"Link": f'<{API}/events?per_page=100&page=2>; rel="next"'})

    respx.get(f"{API}/events").mock(side_effect=handler)
    cfg = make_cfg(tmp_path)

    async def go():
        state = State(cfg.db_path)
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            ctx = Context(cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause())
            await poll_and_store(ctx, "/events", "global", 3, 60)
        state.close()

    asyncio.run(go())
    (line,) = read_jsonl(cfg.logs_dir)
    assert (line["events"], line["dup_in_poll"], line["known"], line["new_events"]) == (200, 1, 0, 199)
    assert len(read_jsonl(cfg.events_dir)) == 199


@respx.mock
def test_failed_later_page_is_fetched_on_next_poll(tmp_path, monkeypatch):
    monkeypatch.setattr(github, "RETRY_DELAYS_S", (0, 0, 0))
    endpoint = "/orgs/r-lib/events"
    next_pages = "/organizations/22618716/events"  # where GitHub's Link header points for this org
    pages = {p: [{"id": str(p * 1000 + i), "type": "PushEvent"} for i in range(100)] for p in (1, 2, 3)}
    page2_down = True

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        if page == 2 and page2_down:
            return httpx.Response(502)
        headers = {"ETag": 'W/"e1"'} if page == 1 else {}
        if page < 3:
            headers["Link"] = (
                f'<{API}{next_pages}?per_page=100&page={page + 1}>; rel="next", '
                f'<{API}{next_pages}?per_page=100&page=3>; rel="last"'
            )
        return httpx.Response(200, json=pages[page], headers=headers)

    respx.get(url__regex=rf"^{API}({endpoint}|{next_pages})").mock(side_effect=handler)
    cfg = make_cfg(tmp_path)

    async def go():
        nonlocal page2_down
        state = State(cfg.db_path)
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            ctx = Context(cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause())
            first = await poll_and_store(ctx, endpoint, "watchlist", 3, 120)
            page2_down = False
            second = await poll_and_store(ctx, endpoint, "watchlist", 3, 120)
        etag = state.get_etag(endpoint)
        state.close()
        return first, second, etag

    first, second, etag = asyncio.run(go())
    assert first[0].error == "HTTP 502" and first[1] == 100
    assert second[0].error is None and second[1] == 200  # pages 2-3; page 1 skipped by dedup
    assert etag == 'W/"e1"'
    ids = [r["event"]["id"] for r in read_jsonl(cfg.events_dir)]
    assert len(ids) == len(set(ids)) == 300


def test_prune_error_does_not_stop_poller(tmp_path, caplog):
    state = State(tmp_path / "s.sqlite3")

    def locked(now=None):
        raise sqlite3.OperationalError("database is locked")

    state.prune = locked

    async def go():
        await asyncio.wait_for(prune_loop(state), timeout=0.2)

    with pytest.raises(asyncio.TimeoutError):  # the loop is still alive after the error
        asyncio.run(go())
    state.close()
    assert "pruning expired event ids failed" in caplog.text



# --- Dedup retention through poll_and_store -------------------------------------------------


def ev(i: str) -> dict:
    return {"id": i, "type": "PushEvent", "created_at": "2026-10-21T12:00:00Z"}


def poll_in_order(cfg, steps: list[tuple[str, str]]) -> None:
    """Run poll_and_store once per (endpoint, track), in order, on one state database."""

    async def go():
        state = State(cfg.db_path)
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            ctx = Context(cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause())
            for endpoint, track in steps:
                await poll_and_store(ctx, endpoint, track, 3, 60)
        state.close()

    asyncio.run(go())


def kept_days(cfg, event_id: str) -> float:
    """Days from now until the dedup table forgets `event_id`."""
    with sqlite3.connect(cfg.db_path) as conn:
        (keep_until,) = conn.execute("SELECT keep_until FROM seen_events WHERE id = ?", (event_id,)).fetchone()
    return (keep_until - time.time()) / DAY


@respx.mock
def test_every_returned_id_is_committed_with_the_track_retention(tmp_path):
    # The global feed writes id 1 (kept 2 days). The org timeline then returns ids 2 and 1: id 1 is
    # not new, but it is committed again and from now on kept 31 days, like id 2.
    respx.get(f"{API}/events").mock(return_value=httpx.Response(200, json=[ev("1")]))
    respx.get(f"{API}/orgs/r-lib/events").mock(return_value=httpx.Response(200, json=[ev("2"), ev("1")]))
    cfg = make_cfg(tmp_path)

    poll_in_order(cfg, [("/events", "global")])
    assert kept_days(cfg, "1") == pytest.approx(2, abs=0.01)

    poll_in_order(cfg, [("/orgs/r-lib/events", "watchlist")])
    assert kept_days(cfg, "1") == pytest.approx(31, abs=0.01)
    assert kept_days(cfg, "2") == pytest.approx(31, abs=0.01)
    assert [r["new_events"] for r in read_jsonl(cfg.logs_dir)] == [1, 1]


@respx.mock
def test_retention_is_not_shortened_when_global_feed_returns_a_watchlist_id(tmp_path):
    respx.get(f"{API}/orgs/r-lib/events").mock(return_value=httpx.Response(200, json=[ev("7")]))
    respx.get(f"{API}/events").mock(return_value=httpx.Response(200, json=[ev("8"), ev("7")]))
    cfg = make_cfg(tmp_path)

    poll_in_order(cfg, [("/orgs/r-lib/events", "watchlist"), ("/events", "global")])
    assert kept_days(cfg, "7") == pytest.approx(31, abs=0.01)  # not cut to 2 days by the global poll
    assert kept_days(cfg, "8") == pytest.approx(2, abs=0.01)


@respx.mock
def test_both_tracks_attach_packages_by_repo_name(tmp_path):
    wl = Watchlist(tiers={"/orgs/tidyverse/events": 1}, packages={"tidyverse/dplyr": ["dplyr"], "mono/big": ["a", "b"]})
    respx.get(f"{API}/orgs/tidyverse/events").mock(return_value=httpx.Response(200, json=[
        {"id": "1", "type": "PushEvent", "repo": {"name": "tidyverse/dplyr"}},
        {"id": "2", "type": "WatchEvent", "repo": {"name": "tidyverse/tidyverse.org"}},  # not a CRAN package
    ]))
    respx.get(f"{API}/events").mock(return_value=httpx.Response(200, json=[
        {"id": "3", "type": "PushEvent", "repo": {"name": "Mono/Big"}},
        {"id": "4", "type": "CreateEvent", "repo": {"name": "cran/RCurl"}},
        {"id": "5", "type": "PushEvent", "repo": {"name": "someone/website"}},
    ]))
    cfg = make_cfg(tmp_path)

    async def go():
        state = State(cfg.db_path)
        async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
            ctx = Context(cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), Pause(),
                          watchlist=wl)
            await poll_and_store(ctx, "/orgs/tidyverse/events", "watchlist", 3, 120)
            await poll_and_store(ctx, "/events", "global", 3, 60)
        state.close()

    asyncio.run(go())
    packages = {r["event"]["id"]: r["packages"] for r in read_jsonl(cfg.events_dir)}
    assert packages == {"1": ["dplyr"], "2": [], "3": ["a", "b"], "4": ["RCurl"], "5": []}

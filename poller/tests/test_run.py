"""The whole poller on virtual time with mocked GitHub: both tracks, one shared quota budget."""

import asyncio
import dataclasses

import httpx
import pytest
import respx

from gh_poller.__main__ import main
from gh_poller.runner import run
from gh_poller.state import State

from helpers import API, VirtualClock, make_cfg, read_jsonl

T0 = 1_800_000_000.0
HEADER = "package,owner,repo,endpoint,tier\n"


def quota(remaining: int) -> dict:
    return {"X-RateLimit-Remaining": str(remaining), "X-RateLimit-Reset": str(int(T0) + 3600), "X-Poll-Interval": "60"}


@respx.mock
def test_both_tracks_share_one_quota_budget(tmp_path):
    cfg = make_cfg(tmp_path)
    cfg.watchlist_path.write_text(
        HEADER + "dplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\nsolo,alice,solo,/repos/alice/solo/events,2\n",
        encoding="utf-8",
    )
    # The tier-2 timeline was last polled before a restart and is due 100 s after the start.
    state = State(cfg.db_path)
    state.commit_poll("/repos/alice/solo/events", etag='W/"a"', status=304, event_ids=[], keep_s=86400, now=T0 - 3500)
    state.close()
    event = {"id": "1", "type": "PushEvent", "repo": {"name": "tidyverse/dplyr"}, "created_at": "2027-01-15T07:55:00Z"}
    # The global feed reports a nearly used-up quota: tier 2 must wait, tier 1 goes on.
    respx.get(f"{API}/events").mock(return_value=httpx.Response(200, json=[event], headers=quota(150)))
    respx.get(f"{API}/orgs/tidyverse/events").mock(return_value=httpx.Response(304, headers=quota(150)))
    solo = respx.get(f"{API}/repos/alice/solo/events").mock(return_value=httpx.Response(304, headers=quota(150)))

    clock = VirtualClock(T0)
    asyncio.run(clock.run(run(cfg, clock), until=T0 + 590))

    log = read_jsonl(cfg.logs_dir)
    global_polls = [r for r in log if r["endpoint"] == "/events"]
    org_polls = [r for r in log if r["endpoint"] == "/orgs/tidyverse/events"]
    assert len(global_polls) == 10 and {r["tier"] for r in global_polls} == {None}
    assert len(org_polls) == 5 and {r["tier"] for r in org_polls} == {1}
    assert solo.call_count == 0  # held back by the budget the global track reported
    (record,) = read_jsonl(cfg.events_dir)  # the same event every minute, written once
    assert record["track"] == "global" and record["packages"] == ["dplyr"]  # looked up by repo.name


@respx.mock
def test_rate_limit_on_the_global_feed_pauses_the_watch_list(tmp_path):
    cfg = make_cfg(tmp_path)
    cfg.watchlist_path.write_text(HEADER + "dplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\n", encoding="utf-8")
    # The org timeline was last polled 20 s before the start, so it is due 100 s after it.
    state = State(cfg.db_path)
    state.commit_poll("/orgs/tidyverse/events", etag='W/"a"', status=304, event_ids=[], keep_s=86400, now=T0 - 20)
    state.close()
    clock = VirtualClock(T0)
    polled: dict[str, list[float]] = {"/events": [], "/orgs/tidyverse/events": []}

    def global_feed(request):
        polled["/events"].append(clock.now() - T0)
        if len(polled["/events"]) == 1:  # a secondary rate limit as GitHub documents it
            return httpx.Response(
                403,
                json={"message": "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."},
                headers={**quota(4000), "Retry-After": "600"},
            )
        return httpx.Response(200, json=[], headers=quota(4000))

    def org(request):
        polled["/orgs/tidyverse/events"].append(clock.now() - T0)
        return httpx.Response(304, headers=quota(4000))

    respx.get(f"{API}/events").mock(side_effect=global_feed)
    respx.get(f"{API}/orgs/tidyverse/events").mock(side_effect=org)
    asyncio.run(clock.run(run(cfg, clock), until=T0 + 700))

    assert polled["/events"][0] == 0
    assert polled["/events"][1] >= 600  # the global loop waited for Retry-After
    assert polled["/orgs/tidyverse/events"][0] >= 600  # so did the watch-list, although due at 100 s


def test_nothing_to_poll_exits(tmp_path):
    cfg = dataclasses.replace(make_cfg(tmp_path), global_track_enabled=False)
    with pytest.raises(SystemExit, match="Nothing to poll"):
        asyncio.run(run(cfg))


@respx.mock
def test_401_ends_the_process_with_a_clear_message(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no .env here: the test must not read a real token
    monkeypatch.setenv("GITHUB_TOKEN", "expired")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    for name in ("WATCHLIST_PATH", "GITHUB_API_URL", "GLOBAL_TRACK_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    respx.get(f"{API}/events").mock(return_value=httpx.Response(401, json={"message": "Bad credentials"}))
    with pytest.raises(SystemExit, match="401 Unauthorized: GITHUB_TOKEN is invalid or expired"):
        main()

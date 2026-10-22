"""The global-feed loop (GET /events), stopped by a fake clock after N sleeps. Shared pause: test_run.py."""

import asyncio
import time

import httpx
import pytest
import respx

from gh_poller import github, runner
from gh_poller.github import PollResult, Unauthorized, build_headers
from gh_poller.runner import ERROR_BACKOFF_S, Context, Pause, endpoint_loop
from gh_poller.sink import DailyJsonl
from gh_poller.state import State

from helpers import API, make_cfg


class Stop(BaseException):
    """Ends the loop under test. BaseException, so the loop's `except Exception` lets it through."""


class FakeTime:
    def __init__(self, stop_after: int):
        self.t = time.time()
        self.sleeps: list[float] = []
        self.stop_after = stop_after

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        if len(self.sleeps) >= self.stop_after:
            raise Stop
        self.t += seconds


def run_loop(tmp_path, endpoint, *, stop_after, min_interval_s=60):
    """Run endpoint_loop until its `stop_after`-th sleep. Returns (clock, stopped by the clock)."""
    cfg = make_cfg(tmp_path)
    clock = FakeTime(stop_after)
    pause = Pause(clock)

    async def go():
        state = State(cfg.db_path)
        try:
            async with httpx.AsyncClient(headers=build_headers("t", "test")) as client:
                ctx = Context(
                    cfg, client, state, DailyJsonl(cfg.events_dir), DailyJsonl(cfg.logs_dir, "requests-"), pause, clock=clock
                )
                await endpoint_loop(ctx, endpoint, "global", 3, min_interval_s)
        finally:
            state.close()

    try:
        asyncio.run(go())
    except Stop:
        return clock, True
    return clock, False


@respx.mock
@pytest.mark.parametrize("x_poll_interval, min_interval_s, expected", [(90, 60, 90), (60, 120, 120)])
def test_next_poll_waits_the_longer_of_x_poll_interval_and_own_interval(
    tmp_path, x_poll_interval, min_interval_s, expected
):
    respx.get(f"{API}/events").mock(
        return_value=httpx.Response(200, json=[], headers={"X-Poll-Interval": str(x_poll_interval)})
    )
    clock, stopped = run_loop(tmp_path, "/events", stop_after=1, min_interval_s=min_interval_s)
    assert stopped and clock.sleeps == [expected]


@respx.mock
def test_server_error_backs_off_for_5_minutes(monkeypatch, tmp_path):
    monkeypatch.setattr(github, "RETRY_DELAYS_S", (0, 0, 0))
    respx.get(f"{API}/events").mock(return_value=httpx.Response(502))
    clock, stopped = run_loop(tmp_path, "/events", stop_after=1, min_interval_s=60)
    assert ERROR_BACKOFF_S == 300
    assert stopped and clock.sleeps == [ERROR_BACKOFF_S]


def test_exception_in_poll_does_not_stop_the_loop(monkeypatch, tmp_path, caplog):
    calls: list[str] = []

    async def flaky(ctx, endpoint, track, max_pages, min_interval_s, **_):
        calls.append(endpoint)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return PollResult(endpoint=endpoint, status=304), 0, min_interval_s

    monkeypatch.setattr(runner, "poll_and_store", flaky)
    clock, stopped = run_loop(tmp_path, "/events", stop_after=2, min_interval_s=60)
    assert stopped and len(calls) == 2
    assert clock.sleeps == [ERROR_BACKOFF_S, 60]
    assert "unexpected error while polling /events" in caplog.text


@respx.mock
def test_401_stops_the_loop_instead_of_backing_off(tmp_path):
    respx.get(f"{API}/events").mock(return_value=httpx.Response(401, json={"message": "Bad credentials"}))
    with pytest.raises(Unauthorized):
        run_loop(tmp_path, "/events", stop_after=1)

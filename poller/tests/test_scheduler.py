"""Watch-list scheduler on virtual time: tiers, priority, first polls, restart, quota, errors, reloads."""

import asyncio

import pytest

from gh_poller.github import PollResult, Unauthorized
from gh_poller.limits import Budget, Pause
from gh_poller.runner import next_interval
from gh_poller.scheduler import DEAD_RECHECK_S, ERROR_BACKOFF_S, MAX_CONCURRENCY, Scheduler, auto_tier2_interval_s

from helpers import VirtualClock

T0 = 1_800_000_000.0
HOUR = 3600


class FakeGitHub:
    """The scheduler's poll function: records every call and answers 304 unless told otherwise."""

    def __init__(self, clock, duration_s=0.0, answers=None):
        self.clock = clock
        self.duration_s = duration_s
        self.answers = answers or {}  # endpoint -> f(n-th call of this endpoint) -> PollResult, or raises
        self.calls = []  # (time, endpoint, tier, base interval, seconds late)
        self.in_flight = self.max_in_flight = 0

    async def __call__(self, endpoint, tier, base_s, late_s):
        self.calls.append((self.clock.now(), endpoint, tier, base_s, late_s))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await self.clock.sleep(self.duration_s)
            answer = self.answers.get(endpoint)
            res = answer(len(self.times(endpoint))) if answer else PollResult(endpoint, status=304)
        finally:
            self.in_flight -= 1
        return res, next_interval(res, base_s)

    def times(self, endpoint):
        """Seconds after T0 at which `endpoint` was polled."""
        return [t - T0 for t, e, *_ in self.calls if e == endpoint]


def gaps(times):
    return [b - a for a, b in zip(times, times[1:])]


def setup(tiers, *, intervals=(120, 900), budget_per_h=3600, history=None, budget=None, pause=None, duration_s=0.0,
          answers=None, **kwargs):
    clock = VirtualClock(T0)
    gh = FakeGitHub(clock, duration_s, answers)
    sched = Scheduler(
        gh,
        tier1_interval_s=intervals[0],
        tier2_interval_s=intervals[1],
        tier2_budget_per_h=budget_per_h,
        clock=clock,
        pause=pause(clock) if pause else Pause(clock),
        budget=budget(clock) if budget else Budget(clock),
        **kwargs,
    )
    sched.update(tiers, None, history)
    return clock, gh, sched


def simulate(clock, sched, seconds, *others):
    async def main():
        await asyncio.gather(sched.run(), *others)

    asyncio.run(clock.run(main(), until=T0 + seconds))


def test_each_tier_is_polled_at_its_own_interval():
    clock, gh, sched = setup({"/orgs/a/events": 1, "/orgs/b/events": 1, "/repos/c/c/events": 2})
    simulate(clock, sched, 2 * HOUR)
    assert set(gaps(gh.times("/orgs/a/events"))) == {120}
    assert set(gaps(gh.times("/orgs/b/events"))) == {120}
    assert set(gaps(gh.times("/repos/c/c/events"))) == {900}


def test_tier1_goes_first_while_tier2_has_a_backlog():
    tiers = {"/orgs/a/events": 1, **{f"/repos/o/r{i}/events": 2 for i in range(600)}}
    clock, gh, sched = setup(tiers, budget_per_h=10**9, duration_s=1.0, max_concurrency=1)
    simulate(clock, sched, 1200)
    a = gh.times("/orgs/a/events")
    assert a[0] == 0  # before the 600 tier-2 timelines due at the same moment
    assert max(gaps(a)) <= 121  # its interval plus one 1-second poll, although tier 2 lags far behind
    assert max(late for _, e, tier, _, late in gh.calls if tier == 2) > 500  # the backlog is real


def test_first_polls_are_spread_out_and_tier1_starts_first():
    tiers = {**{f"/orgs/t{i}/events": 1 for i in range(4)}, **{f"/repos/o/r{i}/events": 2 for i in range(10)}}
    clock, gh, sched = setup(tiers, budget_per_h=360)  # one tier-2 first poll every 10 s
    simulate(clock, sched, 100)
    first = {e: gh.times(e)[0] for e in tiers}
    assert sorted(first[f"/orgs/t{i}/events"] for i in range(4)) == [0, 30, 60, 90]  # over one tier-1 interval
    assert sorted(first[f"/repos/o/r{i}/events"] for i in range(10)) == list(range(0, 100, 10))
    assert gh.calls[0][2] == 1


def test_timelines_added_later_queue_behind_the_cold_start():
    clock, gh, sched = setup({f"/repos/o/r{i}/events": 2 for i in range(10)}, budget_per_h=360)

    async def add_more():
        await clock.sleep(20)
        sched.update({**sched.endpoints, **{f"/repos/n/r{i}/events": 2 for i in range(3)}})

    simulate(clock, sched, 200, add_more())
    assert sorted(gh.times(f"/repos/n/r{i}/events")[0] for i in range(3)) == [100, 110, 120]


def test_restart_continues_from_the_last_poll():
    history = {"/orgs/a/events": (304, T0 - 100), "/repos/gone/x/events": (404, T0 - 86400)}
    clock, gh, sched = setup({"/orgs/a/events": 1, "/repos/gone/x/events": 2}, history=history, max_idle_s=HOUR)
    simulate(clock, sched, 2 * 86400)
    assert gh.times("/orgs/a/events")[0] == 20  # 120 s after the poll before the restart
    assert gh.times("/repos/gone/x/events") == []  # dead: the weekly recheck is still days away


def test_at_most_10_polls_run_at_once():
    clock, gh, sched = setup({f"/repos/o/r{i}/events": 2 for i in range(40)}, budget_per_h=10**9, duration_s=5.0)
    simulate(clock, sched, 60)
    assert MAX_CONCURRENCY == 10
    assert gh.max_in_flight == 10
    assert len(gh.calls) == 40
    assert max(t for t, *_ in gh.calls) - T0 == pytest.approx(15, abs=0.01)  # four waves of ten


def low_quota(remaining, reset_in_s):
    def make(clock):
        budget = Budget(clock)
        budget.update(remaining, int(T0 + reset_in_s))
        return budget

    return make


def test_low_quota_doubles_tier2_intervals_only():
    clock, gh, sched = setup({"/orgs/a/events": 1, "/repos/c/c/events": 2}, budget=low_quota(900, 2 * HOUR))
    simulate(clock, sched, HOUR)
    assert {base for _, e, _, base, _ in gh.calls if e == "/orgs/a/events"} == {120}
    assert {base for _, e, _, base, _ in gh.calls if e == "/repos/c/c/events"} == {1800}
    assert gh.times("/repos/c/c/events") == [0, 1800, 3600]


def test_critical_quota_holds_tier2_until_the_reset():
    clock, gh, sched = setup({"/orgs/a/events": 1, "/repos/c/c/events": 2}, budget=low_quota(150, 600))
    simulate(clock, sched, 1200)
    assert gh.times("/orgs/a/events")[:3] == [0, 120, 240]  # tier 1 goes on
    assert 600 <= gh.times("/repos/c/c/events")[0] <= 605  # tier 2 after the hourly reset


def test_rate_limit_pause_holds_every_tier():
    def paused(clock):
        pause = Pause(clock)
        pause.extend(T0 + 600)
        return pause

    clock, gh, sched = setup({"/orgs/a/events": 1, "/repos/c/c/events": 2}, pause=paused)
    simulate(clock, sched, 700)
    assert min(t for t, *_ in gh.calls) - T0 == 600


def test_x_poll_interval_and_error_backoff():
    answers = {
        "/orgs/slow/events": lambda n: PollResult("/orgs/slow/events", status=304, poll_interval_s=200),
        "/orgs/err/events": lambda n: PollResult("/orgs/err/events", status=502, error="HTTP 502"),
    }
    clock, gh, sched = setup({"/orgs/slow/events": 1, "/orgs/err/events": 1}, answers=answers)
    simulate(clock, sched, HOUR)
    assert set(gaps(gh.times("/orgs/slow/events"))) == {200}  # GitHub asked for more than 120 s
    assert ERROR_BACKOFF_S == 300
    assert set(gaps(gh.times("/orgs/err/events"))) == {ERROR_BACKOFF_S}


def test_dead_timeline_is_checked_again_after_a_week():
    e = "/repos/gone/x/events"
    answers = {e: lambda n: PollResult(e, status=404, dead=True, error="HTTP 404")}
    clock, gh, sched = setup({e: 2}, answers=answers, max_idle_s=HOUR)
    simulate(clock, sched, 8 * 86400)
    assert gh.times(e) == [0, DEAD_RECHECK_S]


def test_exception_in_a_poll_does_not_stop_the_scheduler(caplog):
    def flaky(n):
        if n == 1:
            raise RuntimeError("boom")
        return PollResult("/orgs/a/events", status=304)

    clock, gh, sched = setup({"/orgs/a/events": 1, "/orgs/b/events": 1}, answers={"/orgs/a/events": flaky})
    simulate(clock, sched, 1000)
    assert gh.times("/orgs/a/events")[:2] == [0, ERROR_BACKOFF_S]
    assert set(gaps(gh.times("/orgs/b/events"))) == {120}
    assert "unexpected error while polling /orgs/a/events" in caplog.text


def test_update_while_running_adds_removes_and_retiers():
    clock, gh, sched = setup({"/orgs/a/events": 2, "/orgs/b/events": 1})

    async def change():
        await clock.sleep(1000)
        assert sched.update({"/orgs/a/events": 1, "/orgs/c/events": 1}) == (1, 1, 1)

    simulate(clock, sched, 2000, change())
    assert gh.times("/orgs/a/events")[:4] == [0, 900, 1120, 1240]  # tier 2, then tier 1 from the change
    assert max(gh.times("/orgs/b/events")) < 1000  # removed
    assert 1000 <= gh.times("/orgs/c/events")[0] <= 1005  # added


def test_timeline_removed_and_added_again_keeps_only_its_new_schedule():
    clock, gh, sched = setup({"/orgs/a/events": 1})  # first poll planned at once
    sched.update({})
    sched.update({"/orgs/a/events": 1}, None, {"/orgs/a/events": (304, T0 - 60)})  # back, polled 60 s ago
    simulate(clock, sched, 200)
    assert gh.times("/orgs/a/events") == [60, 180]


def test_401_stops_the_scheduler():
    def unauthorized(n):
        raise Unauthorized("/orgs/a/events: HTTP 401 Bad credentials")

    clock, gh, sched = setup({"/orgs/a/events": 1, "/orgs/b/events": 1}, answers={"/orgs/a/events": unauthorized})
    with pytest.raises(Unauthorized):
        simulate(clock, sched, HOUR)
    assert gh.times("/orgs/a/events") == [0] and gh.times("/orgs/b/events") == []  # b was due at 60 s


def test_automatic_tier2_interval_is_timelines_over_budget_rounded_up_to_hours():
    assert auto_tier2_interval_s(8_470, 3500) == 3 * HOUR  # all owners with >= 2 repos are organisations
    assert auto_tier2_interval_s(16_188, 3500) == 5 * HOUR  # all of them are users: one timeline per repo
    assert auto_tier2_interval_s(3_500, 3500) == HOUR
    assert auto_tier2_interval_s(3_501, 3500) == 2 * HOUR
    assert auto_tier2_interval_s(0, 3500) == HOUR


def test_paid_polls_stay_within_the_tier2_budget_in_the_first_cycles():
    # 25 tier-2 timelines, budget 10 per hour: every 3 h, first polls 6 min apart. Even if every
    # answer is a paid 200, no hour holds more than 10 tier-2 polls.
    tiers = {f"/repos/o/r{i:02}/events": 2 for i in range(25)}
    answers = {e: (lambda n, e=e: PollResult(e, status=200)) for e in tiers}
    clock, gh, sched = setup(tiers, intervals=(120, None), budget_per_h=10, answers=answers)
    simulate(clock, sched, 10 * HOUR)
    times = sorted(t - T0 for t, *_ in gh.calls)
    assert sched.tier_interval(2) == 3 * HOUR
    assert gh.times("/repos/o/r00/events")[:3] == [0, 3 * HOUR, 6 * HOUR]
    assert gh.times("/repos/o/r24/events")[0] == 24 * 360
    assert max(sum(t <= u < t + HOUR for u in times) for t in times) == 10


def test_automatic_tier2_interval_follows_the_watchlist_size():
    tiers = {f"/repos/o/r{i}/events": 2 for i in range(25)}
    clock, gh, sched = setup(tiers, intervals=(120, None), budget_per_h=10)
    assert sched.tier_interval(2) == 3 * HOUR
    sched.update({**tiers, **{f"/repos/n/r{i}/events": 2 for i in range(10)}, "/orgs/a/events": 1})
    assert sched.tier_interval(2) == 4 * HOUR  # 35 timelines / 10 per hour, rounded up


def test_set_tier2_interval_overrides_the_automatic_one():
    clock, gh, sched = setup({f"/repos/o/r{i}/events": 2 for i in range(25)}, intervals=(120, HOUR), budget_per_h=10)
    assert sched.tier_interval(2) == HOUR

import asyncio

import pytest

from gh_poller.limits import Budget, Pause, RequestGate, TokenBucket

from helpers import VirtualClock

T0 = 1_800_000_000.0


def test_token_bucket_allows_a_burst_then_300_per_minute():
    clock = VirtualClock(T0)
    bucket = TokenBucket(per_min=300, burst=10, clock=clock)

    async def take(n):
        for _ in range(n):
            await bucket.take()

    asyncio.run(clock.run(take(10), until=T0 + 3600))
    assert clock.now() == T0  # the burst needs no wait
    asyncio.run(clock.run(take(300), until=T0 + 3600))
    assert clock.now() - T0 == pytest.approx(60, abs=0.5)


def test_gate_waits_for_the_pause_then_for_a_token():
    clock = VirtualClock(T0)
    pause = Pause(clock)
    pause.extend(T0 + 100)
    gate = RequestGate(pause, TokenBucket(clock=clock))
    asyncio.run(clock.run(gate(), until=T0 + 1000))
    assert clock.now() == T0 + 100


def test_budget_follows_the_hourly_quota():
    clock = VirtualClock(T0)
    budget = Budget(clock)
    reset = int(T0) + 1800
    assert budget.current() is None and budget.tier2_factor() == 1 and not budget.tier2_paused()

    assert budget.update(1500, reset) is None
    assert budget.tier2_factor() == 1
    budget.update(999, reset)
    assert budget.tier2_factor() == 2 and not budget.tier2_paused()
    budget.update(1200, reset)  # a slower response from the same hour arrives late
    assert budget.current() == 999
    budget.update(199, reset)
    assert budget.tier2_paused()
    assert budget.update(0, reset) == reset + 1  # used up: pause everything until the reset

    clock.t = reset  # the hour is over: the quota is full again
    assert budget.current() is None and budget.tier2_factor() == 1 and not budget.tier2_paused()
    budget.update(4999, reset + 3600)
    assert budget.update(10, reset) is None and budget.current() == 4999  # late answer from the old hour

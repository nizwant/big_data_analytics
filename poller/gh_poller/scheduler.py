"""Watch-list scheduler: a queue per tier by next poll time, tier 1 first, at most 10 polls at once.

The automatic tier-2 interval is ceil(tier-2 timelines / tier2_budget_per_h) hours, so a whole cycle
of paid 200s (as in the first cycle) stays within the hourly quota.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import logging
import math
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from .github import PollResult, Unauthorized
from .limits import Budget, Clock, Pause

log = logging.getLogger("gh_poller")

MAX_CONCURRENCY = 10
ERROR_BACKOFF_S = 300
DEAD_RECHECK_S = 7 * 86400  # 404/410: look again after a week (repository renamed or deleted)
MAX_IDLE_S = 5.0  # longest sleep between checks; picks up watch-list reloads and quota resets

# (endpoint, tier, base interval in s, seconds the poll starts late) -> (result, seconds to the next poll)
PollFn = Callable[[str, int, float, float], Awaitable[tuple[PollResult, float]]]
# endpoint -> (last HTTP status, last poll time) from the state database
History = dict[str, tuple[int | None, float | None]]


def auto_tier2_interval_s(n_tier2: int, budget_per_h: float) -> float:
    """ceil(n / budget) hours, at least 1 h: one cycle stays within the budget even if every poll is paid."""
    return max(1, math.ceil(n_tier2 / budget_per_h)) * 3600.0


@dataclass(order=True)
class _Due:
    at: float
    seq: int  # unique; an entry is current only while it is its endpoint's latest one
    endpoint: str = field(compare=False)


class Scheduler:
    def __init__(
        self,
        poll: PollFn,
        *,
        tier1_interval_s: float,
        tier2_interval_s: float | None,
        tier2_budget_per_h: float,
        clock: Clock | None = None,
        pause: Pause | None = None,
        budget: Budget | None = None,
        max_concurrency: int = MAX_CONCURRENCY,
        max_idle_s: float = MAX_IDLE_S,
    ) -> None:
        self._poll = poll
        self.tier1_interval_s = tier1_interval_s
        self.tier2_fixed_s = tier2_interval_s  # None: derived from the number of tier-2 timelines
        self.tier2_budget_per_h = tier2_budget_per_h
        self._tier2_auto_s = auto_tier2_interval_s(0, tier2_budget_per_h)
        self.clock = clock or Clock()
        self.pause = pause or Pause(self.clock)
        self.budget = budget or Budget(self.clock)
        self.max_idle_s = max_idle_s
        self._slots = asyncio.Semaphore(max_concurrency)
        self._queues: dict[int, list[_Due]] = {1: [], 2: []}
        self._tier: dict[str, int] = {}  # active endpoints
        self._latest: dict[str, int] = {}  # endpoint -> seq of its current queue entry; older entries are skipped
        self._next_at: dict[str, float] = {}
        self._in_flight: set[str] = set()
        self._seq = itertools.count()
        self._cold_until = 0.0  # last first-poll time already planned for tier 2
        self._tasks: set[asyncio.Task] = set()
        self._fatal: BaseException | None = None

    @property
    def endpoints(self) -> dict[str, int]:
        return dict(self._tier)

    def tier_interval(self, tier: int) -> float:
        if tier == 1:
            return self.tier1_interval_s
        return self.tier2_fixed_s if self.tier2_fixed_s is not None else self._tier2_auto_s

    def base_interval(self, tier: int) -> float:
        base = self.tier_interval(tier)
        return base * self.budget.tier2_factor() if tier == 2 else base

    def update(self, tiers: dict[str, int], order: list[str] | None = None, history: History | None = None) -> tuple[int, int, int]:
        """Apply a watch-list; returns (added, removed, re-tiered).

        Known endpoints resume from `history`; first polls are spread: tier 1 over one interval,
        tier 2 at `tier2_budget_per_h` per hour, in `order`.
        """
        now = self.clock.now()
        history = history or {}
        self._tier2_auto_s = auto_tier2_interval_s(sum(t == 2 for t in tiers.values()), self.tier2_budget_per_h)
        removed = [e for e in self._tier if e not in tiers]
        for e in removed:
            del self._tier[e]
            self._latest.pop(e, None)
            self._next_at.pop(e, None)
        added = retiered = 0
        first: dict[int, list[str]] = {1: [], 2: []}
        for e in order if order is not None else sorted(tiers):
            tier = tiers[e]
            if e in self._tier:
                if self._tier[e] != tier:
                    self._tier[e] = tier
                    retiered += 1
                    if e not in self._in_flight:  # moved to tier 1: no need to wait out a tier-2 interval
                        self._push(e, min(self._next_at[e], now + self.base_interval(tier)))
                continue
            self._tier[e] = tier
            added += 1
            status, last = history.get(e, (None, None))
            if last is None:
                first[tier].append(e)
            else:
                self._push(e, last + (DEAD_RECHECK_S if status in (404, 410) else self.base_interval(tier)))
        for i, e in enumerate(first[1]):
            self._push(e, now + i * self.tier1_interval_s / len(first[1]))
        if first[2]:
            gap = 3600 / self.tier2_budget_per_h
            start = max(now, self._cold_until)
            for i, e in enumerate(first[2]):
                self._push(e, start + i * gap)
            self._cold_until = start + len(first[2]) * gap
        return added, len(removed), retiered

    def _push(self, endpoint: str, at: float) -> None:
        entry = _Due(at, next(self._seq), endpoint)
        self._latest[endpoint] = entry.seq
        self._next_at[endpoint] = at
        heapq.heappush(self._queues[self._tier[endpoint]], entry)

    def _head(self, tier: int) -> _Due | None:
        queue = self._queues[tier]
        while queue and (queue[0].seq != self._latest.get(queue[0].endpoint) or queue[0].endpoint in self._in_flight):
            heapq.heappop(queue)  # removed, rescheduled or re-tiered since it was queued
        return queue[0] if queue else None

    def _pop_due(self, now: float) -> tuple[_Due, int] | None:
        for tier in (1, 2):
            if tier == 2 and self.budget.tier2_paused():
                continue
            head = self._head(tier)
            if head is not None and head.at <= now:
                return heapq.heappop(self._queues[tier]), tier
        return None

    def _idle_s(self, now: float) -> float:
        heads = [h.at for t in (1, 2) if (h := self._head(t)) and not (t == 2 and self.budget.tier2_paused())]
        return min(self.max_idle_s, max(0.0, min(heads, default=now + self.max_idle_s) - now))

    async def run(self) -> None:
        try:
            while True:
                if self._fatal is not None:
                    raise self._fatal
                await self.pause.wait()
                # Take a free slot first and only then pick the timeline, so a tier-1 timeline that
                # fell due while every slot was busy goes before a tier-2 backlog.
                await self._slots.acquire()
                if self._fatal is not None:
                    self._slots.release()
                    raise self._fatal
                now = self.clock.now()
                due = self._pop_due(now)
                if due is None:
                    self._slots.release()
                    await self.clock.sleep(self._idle_s(now))
                    continue
                entry, tier = due
                self._in_flight.add(entry.endpoint)
                task = asyncio.create_task(self._poll_one(entry.endpoint, tier, entry.at))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        finally:
            for task in list(self._tasks):
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _poll_one(self, endpoint: str, tier: int, due_at: float) -> None:
        interval = float(ERROR_BACKOFF_S)
        try:
            res, interval = await self._poll(endpoint, tier, self.base_interval(tier), self.clock.now() - due_at)
            if res.dead:
                interval = DEAD_RECHECK_S
                log.warning("%s returned HTTP %s; next check in %d days", endpoint, res.status, DEAD_RECHECK_S // 86400)
        except Unauthorized as exc:  # a bad token fails every request: stop the scheduler
            self._fatal = exc
            return
        except Exception:  # one bad response must not stop the poller
            log.exception("unexpected error while polling %s", endpoint)
        finally:
            self._slots.release()
            self._in_flight.discard(endpoint)
        if endpoint in self._tier:
            self._push(endpoint, self.clock.now() + interval)

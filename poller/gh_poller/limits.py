"""Limits shared by every GitHub request: clock, rate-limit pause, request pacing, quota budget."""

from __future__ import annotations

import asyncio
import logging
import time

log = logging.getLogger("gh_poller")

REQUESTS_PER_MIN = 300  # page requests of the whole poller; GitHub's secondary limit is 900/min
BURST = 10
QUOTA_LOW = 1000  # X-RateLimit-Remaining below this: tier-2 intervals are doubled
QUOTA_CRITICAL = 200  # below this: tier 2 waits for the hourly reset; tier 1 and the global track go on


class Clock:
    """Real time. Tests pass a virtual clock with the same two methods."""

    def now(self) -> float:
        return time.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class Pause:
    """Shared stop for every request after a rate-limit response or when the quota is used up."""

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = clock or Clock()
        self.until = 0.0

    def extend(self, until: float) -> None:
        if until > self.until:
            self.until = until
            log.warning("rate limited: pausing all polling for %.0f s", until - self.clock.now())

    async def wait(self) -> None:
        while (delay := self.until - self.clock.now()) > 0:
            await self.clock.sleep(delay)


class TokenBucket:
    """At most `per_min` requests per minute on average, `burst` at once."""

    def __init__(self, per_min: float = REQUESTS_PER_MIN, burst: int = BURST, clock: Clock | None = None) -> None:
        self.clock = clock or Clock()
        self.rate = per_min / 60
        self.capacity = float(burst)
        self.tokens = float(burst)
        self.updated = self.clock.now()
        self._lock = asyncio.Lock()  # waiters are served in arrival order

    async def take(self) -> None:
        async with self._lock:
            while True:
                now = self.clock.now()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1 - 1e-6:  # tolerate float rounding of epoch timestamps
                    self.tokens = max(0.0, self.tokens - 1)
                    return
                await self.clock.sleep(max((1 - self.tokens) / self.rate, 0.001))


class RequestGate:
    """Awaited before every page request (retries included): waits out a pause, then for a token."""

    def __init__(self, pause: Pause, bucket: TokenBucket) -> None:
        self.pause = pause
        self.bucket = bucket

    async def __call__(self) -> None:
        await self.pause.wait()
        await self.bucket.take()
        await self.pause.wait()  # a pause may have started while we waited for the token


class Budget:
    """Hourly quota from the X-RateLimit-Remaining / X-RateLimit-Reset headers of every response."""

    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = clock or Clock()
        self.remaining: int | None = None
        self.reset: int | None = None  # epoch seconds

    def update(self, remaining: int | None, reset: int | None) -> float | None:
        """Record one response. Returns the time to pause everything until, when the quota is used up."""
        if remaining is None:
            return None
        if reset is not None and self.reset is not None and reset < self.reset:
            return None  # a slow response from the previous hour
        if reset == self.reset and self.remaining is not None:
            self.remaining = min(self.remaining, remaining)  # concurrent responses can arrive out of order
        else:
            self.remaining, self.reset = remaining, reset
        if self.remaining == 0 and self.reset:
            return float(self.reset) + 1
        return None

    def current(self) -> int | None:
        """Requests left in this hour; None when unknown or the hour is over (the quota is full again)."""
        if self.remaining is None or (self.reset is not None and self.clock.now() >= self.reset):
            return None
        return self.remaining

    def tier2_factor(self) -> int:
        left = self.current()
        return 2 if left is not None and left < QUOTA_LOW else 1

    def tier2_paused(self) -> bool:
        left = self.current()
        return left is not None and left < QUOTA_CRITICAL

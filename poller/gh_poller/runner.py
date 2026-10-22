"""Wiring: the global track as its own loop, the watch-list scheduler, state and output.

Every request of both tracks passes the same pause, request pacing and quota budget.
"""

from __future__ import annotations

import asyncio
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .config import Config
from .github import PollResult, Unauthorized, build_headers, poll
from .limits import Budget, Clock, Pause, RequestGate, TokenBucket
from .scheduler import ERROR_BACKOFF_S, Scheduler
from .sink import DailyJsonl, make_record
from .state import State
from .watchlist import Watchlist, WatchlistError, load_watchlist

log = logging.getLogger("gh_poller")

GLOBAL_ENDPOINT = "/events"
# How long an event id stays in the dedup table. The global feed only shows the last few
# minutes, but org/repo timelines return events up to 30 days old, so their ids must live longer.
KEEP_S = {"global": 2 * 86400, "watchlist": 31 * 86400}
PRUNE_EVERY_S = 3600
WATCHLIST_CHECK_S = 60  # how often the watch-list file's modification time is checked


@dataclass
class Context:
    cfg: Config
    client: httpx.AsyncClient
    state: State
    events_out: DailyJsonl
    requests_log: DailyJsonl
    pause: Pause
    clock: Clock = field(default_factory=Clock)
    budget: Budget | None = None
    gate: RequestGate | None = None  # awaited before every page request
    watchlist: Watchlist = field(default_factory=Watchlist)

    def __post_init__(self) -> None:
        if self.budget is None:
            self.budget = Budget(self.clock)
        if self.gate is None:
            self.gate = RequestGate(self.pause, TokenBucket(clock=self.clock))


def _delays_s(events: list[dict], fetched: datetime) -> list[float]:
    """Seconds between each event's created_at and the moment we fetched it."""
    delays: list[float] = []
    for event in events:
        try:
            created = datetime.strptime(event["created_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except (KeyError, TypeError, ValueError):
            continue
        delays.append((fetched - created).total_seconds())
    return delays


def next_interval(res: PollResult, min_interval_s: float) -> float:
    """Seconds until the next poll: own interval or X-Poll-Interval, whichever is longer;
    at least ERROR_BACKOFF_S after an error other than a rate limit (that pauses everyone)."""
    interval = max(min_interval_s, float(res.poll_interval_s or 0))
    if res.error and not res.pause_until:
        interval = max(interval, ERROR_BACKOFF_S)
    return interval


async def poll_and_store(
    ctx: Context,
    endpoint: str,
    track: str,
    max_pages: int,
    min_interval_s: float,
    *,
    tier: int | None = None,
    late_s: float | None = None,
) -> tuple[PollResult, int, float]:
    """Poll one timeline once, write new events, commit state, log the request.

    Returns (result, number of new events, seconds until the next poll).
    """
    res = await poll(
        ctx.client,
        ctx.cfg.api_url,
        endpoint,
        etag=ctx.state.get_etag(endpoint),
        is_seen=ctx.state.is_seen,
        max_pages=max_pages,
        sleep=ctx.clock.sleep,
        now=ctx.clock.now,
        throttle=ctx.gate,
    )
    now = ctx.clock.now()
    fetched = datetime.fromtimestamp(now, timezone.utc)
    fetched_at = fetched.strftime("%Y-%m-%dT%H:%M:%SZ")
    day = fetched_at[:10]

    # No awaits from here to the commit, so tasks cannot interleave between dedup and commit.
    # Order: write events first, then commit state. A crash in between re-sends a few events
    # after restart (at-least-once); downstream deduplicates on event id.
    new = ctx.state.filter_new(res.events)
    unique = len({str(e.get("id")) for e in res.events})
    ctx.events_out.write(
        (
            make_record(e, track=track, endpoint=endpoint, fetched_at=fetched_at, packages_for=ctx.watchlist.packages_for)
            for e in reversed(new)
        ),
        day=day,
    )
    if res.status in (200, 304):
        ctx.state.commit_poll(
            endpoint,
            etag=res.etag,
            status=res.status,
            event_ids=[str(e.get("id")) for e in res.events],
            keep_s=KEEP_S[track],
            now=now,
        )
    elif res.dead:
        ctx.state.record_dead(endpoint, res.status, now=now)
    quota_pause = ctx.budget.update(res.rate_remaining, res.rate_reset)
    if res.pause_until or quota_pause:
        ctx.pause.extend(max(res.pause_until or 0.0, quota_pause or 0.0))

    interval = next_interval(res, min_interval_s)
    delays = _delays_s(new, fetched)
    ctx.requests_log.write(
        [
            {
                "ts": fetched_at,
                "endpoint": endpoint,
                "track": track,
                "tier": tier,
                "status": res.status,
                "requests": res.requests,
                "events": len(res.events),
                # Same event on two pages of one poll: the timeline moved between page requests.
                "dup_in_poll": len(res.events) - unique,
                # Written before (earlier poll or other track). For the global feed, 0 means this
                # poll did not reach back to the previous one, so events in between may be missing.
                "known": unique - len(new),
                "new_events": len(new),
                "rate_remaining": res.rate_remaining,
                "rate_limited": res.pause_until is not None,
                "poll_interval_s": res.poll_interval_s,
                "next_poll_s": interval,
                # Seconds after its planned time that this poll started (scheduler falling behind).
                "late_s": round(late_s, 1) if late_s is not None else None,
                "delay_p50_s": round(statistics.median(delays), 1) if delays else None,
                "delay_max_s": round(max(delays), 1) if delays else None,
                "error": res.error,
            }
        ],
        day=day,
    )
    log.info(
        "%-32s status=%s new=%d remaining=%s next=%.0fs%s",
        endpoint,
        res.status,
        len(new),
        res.rate_remaining,
        interval,
        f" error={res.error}" if res.error else "",
    )
    return res, len(new), interval


async def endpoint_loop(ctx: Context, endpoint: str, track: str, max_pages: int, min_interval_s: float) -> None:
    """Poll one timeline forever (the global track). Errors, 404/410 included, back off and retry."""
    while True:
        await ctx.pause.wait()
        try:
            _, _, interval = await poll_and_store(ctx, endpoint, track, max_pages, min_interval_s)
        except Unauthorized:
            raise  # a bad token fails every request: stop the poller
        except Exception:  # keep the task alive; one bad response must not stop the poller
            log.exception("unexpected error while polling %s", endpoint)
            interval = ERROR_BACKOFF_S
        await ctx.clock.sleep(interval)


async def poll_watchlist(ctx: Context, endpoint: str, tier: int, base_interval_s: float, late_s: float) -> tuple[PollResult, float]:
    """The scheduler's poll function for watch-list timelines."""
    res, _, interval = await poll_and_store(
        ctx, endpoint, "watchlist", ctx.cfg.watchlist_max_pages, base_interval_s, tier=tier, late_s=late_s
    )
    return res, interval


async def prune_loop(state: State, clock: Clock | None = None) -> None:
    clock = clock or Clock()
    while True:
        try:
            removed = state.prune(now=clock.now())
            if removed:
                log.info("pruned %d expired event ids", removed)
        except Exception:  # e.g. the database file is briefly locked; pruning must not stop polling
            log.exception("pruning expired event ids failed; next try in %d s", PRUNE_EVERY_S)
        await clock.sleep(PRUNE_EVERY_S)


def _signature(path: Path) -> tuple[int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return st.st_mtime_ns, st.st_size


def reload_watchlist(ctx: Context, scheduler: Scheduler, last: tuple[int, int] | None) -> tuple[int, int] | None:
    """Apply the watch-list file if it changed since `last`; a missing or invalid file keeps the current list."""
    path = ctx.cfg.watchlist_path
    sig = _signature(path)
    if sig is None or sig == last:
        return last
    try:
        wl = load_watchlist(path)
    except WatchlistError as exc:
        log.error("watch-list not loaded, keeping the current one: %s", exc)
        return sig
    except OSError as exc:
        log.warning("watch-list %s could not be read, will retry: %s", path, exc)
        return last
    ctx.watchlist = wl
    added, removed, retiered = scheduler.update(wl.tiers, wl.order(), ctx.state.endpoint_history())
    tiers = list(wl.tiers.values())
    n2, every_h = tiers.count(2), scheduler.tier_interval(2) / 3600
    log.info(
        "watch-list %s: %d timelines (tier 1: %d, tier 2: %d every %g h%s); added %d, removed %d, re-tiered %d%s",
        path,
        len(tiers),
        tiers.count(1),
        n2,
        every_h,
        "" if ctx.cfg.tier2_interval_s is not None else ", automatic",
        added,
        removed,
        retiered,
        f"; skipped {wl.skipped_rows} invalid rows" if wl.skipped_rows else "",
    )
    if n2 / every_h > ctx.cfg.tier2_budget_per_h:
        log.warning(
            "tier 2: %d polls/h is above TIER2_BUDGET_PER_H=%g; this holds only while most answers are 304",
            n2 / every_h,
            ctx.cfg.tier2_budget_per_h,
        )
    return sig


async def watchlist_loop(ctx: Context, scheduler: Scheduler, last: tuple[int, int] | None) -> None:
    while True:
        await ctx.clock.sleep(WATCHLIST_CHECK_S)
        last = reload_watchlist(ctx, scheduler, last)


async def run_until_first_error(coros: list) -> None:
    """Run the tasks until one fails; then cancel the others and raise that error."""
    tasks = [asyncio.ensure_future(c) for c in coros]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in done:
            if not task.cancelled() and task.exception() is not None:
                raise task.exception()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def run(cfg: Config, clock: Clock | None = None) -> None:
    clock = clock or Clock()
    state = State(cfg.db_path)
    try:
        async with httpx.AsyncClient(
            headers=build_headers(cfg.github_token, cfg.user_agent),
            timeout=httpx.Timeout(30.0),
            follow_redirects=True,
        ) as client:
            pause = Pause(clock)
            ctx = Context(
                cfg=cfg,
                client=client,
                state=state,
                events_out=DailyJsonl(cfg.events_dir),
                requests_log=DailyJsonl(cfg.logs_dir, prefix="requests-"),
                pause=pause,
                clock=clock,
            )
            scheduler = Scheduler(
                lambda endpoint, tier, base, late: poll_watchlist(ctx, endpoint, tier, base, late),
                tier1_interval_s=cfg.tier1_interval_s,
                tier2_interval_s=cfg.tier2_interval_s,
                tier2_budget_per_h=cfg.tier2_budget_per_h,
                clock=clock,
                pause=pause,
                budget=ctx.budget,
            )
            last = reload_watchlist(ctx, scheduler, None)
            if not scheduler.endpoints:
                if not cfg.global_track_enabled:
                    raise SystemExit(f"Nothing to poll: the global track is disabled and {cfg.watchlist_path} is missing or empty.")
                log.warning("no watch-list at %s yet; polling the global track until it appears", cfg.watchlist_path)
            tasks = [prune_loop(state, clock), watchlist_loop(ctx, scheduler, last), scheduler.run()]
            if cfg.global_track_enabled:
                tasks.append(endpoint_loop(ctx, GLOBAL_ENDPOINT, "global", cfg.global_max_pages, cfg.global_min_interval_s))
            log.info(
                "global track %s; data in %s",
                "on" if cfg.global_track_enabled else "off",
                cfg.data_dir.resolve(),
            )
            await run_until_first_error(tasks)
    finally:
        state.close()

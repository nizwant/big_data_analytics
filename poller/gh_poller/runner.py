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
from .sink import DailyJsonl, EventSinks, KafkaSink, SinkError, make_record
from .snapshots import WatchlistFeed
from .state import State
from .watchlist import Watchlist, WatchlistError, load_watchlist, watchlist_from_rows

log = logging.getLogger("gh_poller")

GLOBAL_ENDPOINT = "/events"
# How long an event id stays in the dedup table. The global feed only shows the last few
# minutes, but org/repo timelines return events up to 30 days old, so their ids must live longer.
KEEP_S = {"global": 2 * 86400, "watchlist": 31 * 86400}
PRUNE_EVERY_S = 3600
WATCHLIST_CHECK_S = 60  # how often the watch-list (file or Kafka topic) is checked for a new version
# At start, how long to wait for the Kafka topic to be read to its end before polling begins.
SYNC_WAIT_S = 30
# After a failed delivery (Kafka down, disk full) all polling pauses this long. The events stay
# uncommitted, so the next poll of that timeline fetches them again.
SINK_RETRY_S = 60


@dataclass
class Context:
    cfg: Config
    client: httpx.AsyncClient
    state: State
    events_out: DailyJsonl | EventSinks
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
    # Order: deliver events first (Kafka confirms them), then commit state. A crash in between
    # re-sends a few events after restart (at-least-once); downstream deduplicates on event id.
    new = ctx.state.filter_new(res.events)
    unique = len({str(e.get("id")) for e in res.events})
    sink_error = None
    try:
        ctx.events_out.write(
            [
                make_record(e, track=track, endpoint=endpoint, fetched_at=fetched_at, packages_for=ctx.watchlist.packages_for)
                for e in reversed(new)
            ],
            day=day,
        )
    except SinkError as exc:
        sink_error = str(exc)
        log.error("events of %s not delivered: %s", endpoint, exc)
        ctx.pause.extend(now + SINK_RETRY_S, reason="output failed")
    # After a failed delivery nothing is committed: the ETag and the event ids stay as they were.
    if sink_error is None and res.status in (200, 304):
        ctx.state.commit_poll(
            endpoint,
            etag=res.etag,
            status=res.status,
            event_ids=[str(e.get("id")) for e in res.events],
            keep_s=KEEP_S[track],
            now=now,
        )
    elif sink_error is None and res.dead:
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
                "new_events": 0 if sink_error else len(new),
                "rate_remaining": res.rate_remaining,
                "rate_limited": res.pause_until is not None,
                "poll_interval_s": res.poll_interval_s,
                "next_poll_s": interval,
                # Seconds after its planned time that this poll started (scheduler falling behind).
                "late_s": round(late_s, 1) if late_s is not None else None,
                "delay_p50_s": round(statistics.median(delays), 1) if delays else None,
                "delay_max_s": round(max(delays), 1) if delays else None,
                "error": "; ".join(x for x in (res.error, sink_error) if x) or None,
            }
        ],
        day=day,
    )
    log.info(
        "%-32s status=%s new=%d remaining=%s next=%.0fs%s",
        endpoint,
        res.status,
        0 if sink_error else len(new),
        res.rate_remaining,
        interval,
        f" error={res.error}" if res.error else "",
    )
    return res, 0 if sink_error else len(new), interval


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
    apply_watchlist(ctx, scheduler, wl, str(path))
    return sig


def apply_watchlist(ctx: Context, scheduler: Scheduler, wl: Watchlist, origin: str) -> None:
    ctx.watchlist = wl
    added, removed, retiered = scheduler.update(wl.tiers, wl.order(), ctx.state.endpoint_history())
    tiers = list(wl.tiers.values())
    n2, every_h = tiers.count(2), scheduler.tier_interval(2) / 3600
    log.info(
        "watch-list %s: %d timelines (tier 1: %d, tier 2: %d every %g h%s); added %d, removed %d, re-tiered %d%s",
        origin,
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


class FileWatchlist:
    """watchlist.csv, applied again whenever its modification time or size changes."""

    synced = True

    def __init__(self, path: Path):
        self.path = path
        self._last: tuple[int, int] | None = None

    def describe(self) -> str:
        return str(self.path)

    def check(self, ctx: Context, scheduler: Scheduler) -> None:
        self._last = reload_watchlist(ctx, scheduler, self._last)

    def close(self) -> None:
        pass


class KafkaWatchlist:
    """The newest complete snapshot in github_repo_list, published by the Airflow DAG github_watchlist."""

    def __init__(self, feed: WatchlistFeed):
        self.feed = feed
        self.applied: str | None = None  # built_at of the snapshot in use

    @property
    def synced(self) -> bool:
        return self.feed.synced

    def describe(self) -> str:
        return f"Kafka topic {self.feed.topic}"

    def check(self, ctx: Context, scheduler: Scheduler) -> None:
        snapshot = self.feed.read()
        if snapshot is None or snapshot.built_at == self.applied:
            return
        self.applied = snapshot.built_at
        origin = f"{self.feed.topic} built at {snapshot.built_at}"
        try:
            wl = watchlist_from_rows(snapshot.rows, origin)
        except WatchlistError as exc:
            log.error("watch-list not loaded, keeping the current one: %s", exc)
            return
        apply_watchlist(ctx, scheduler, wl, origin)

    def close(self) -> None:
        self.feed.close()


WatchlistSource = FileWatchlist | KafkaWatchlist


def open_watchlist_source(cfg: Config) -> WatchlistSource:
    if cfg.watchlist_source == "kafka":
        return KafkaWatchlist(WatchlistFeed(cfg.kafka_bootstrap_servers or "", cfg.watchlist_topic))
    return FileWatchlist(cfg.watchlist_path)


async def first_check(ctx: Context, scheduler: Scheduler, source: WatchlistSource) -> None:
    """Load the watch-list before polling starts. Waits up to SYNC_WAIT_S for the Kafka topic to be
    read to its end, so an older snapshot is not applied just before the newest one."""
    deadline = ctx.clock.now() + SYNC_WAIT_S
    source.check(ctx, scheduler)
    while not source.synced and ctx.clock.now() < deadline:
        await ctx.clock.sleep(1)
        source.check(ctx, scheduler)


async def watchlist_loop(ctx: Context, scheduler: Scheduler, source: WatchlistSource) -> None:
    while True:
        await ctx.clock.sleep(WATCHLIST_CHECK_S)
        source.check(ctx, scheduler)
        if not scheduler.endpoints:
            log.info("still waiting for a watch-list in %s", source.describe())


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


async def run(cfg: Config, clock: Clock | None = None, watchlist_source: WatchlistSource | None = None) -> None:
    clock = clock or Clock()
    state = State(cfg.db_path)
    events_out = open_outputs(cfg)
    source = watchlist_source or open_watchlist_source(cfg)
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
                events_out=events_out,
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
            await first_check(ctx, scheduler, source)
            if not scheduler.endpoints:
                # No exit: the first run of the DAG github_watchlist may still be on its way.
                log.warning(
                    "no watch-list in %s yet; checking every %d s%s",
                    source.describe(),
                    WATCHLIST_CHECK_S,
                    ", polling the global track meanwhile" if cfg.global_track_enabled else "",
                )
            tasks = [prune_loop(state, clock), watchlist_loop(ctx, scheduler, source), scheduler.run()]
            if cfg.global_track_enabled:
                tasks.append(endpoint_loop(ctx, GLOBAL_ENDPOINT, "global", cfg.global_max_pages, cfg.global_min_interval_s))
            log.info(
                "global track %s; watch-list from %s; events to %s; state and logs in %s",
                "on" if cfg.global_track_enabled else "off",
                source.describe(),
                describe_outputs(cfg),
                cfg.data_dir.resolve(),
            )
            await run_until_first_error(tasks)
    finally:
        source.close()
        events_out.close()
        state.close()


def open_outputs(cfg: Config) -> EventSinks:
    sinks: list = []
    if cfg.kafka_bootstrap_servers:
        kafka = KafkaSink(cfg.kafka_bootstrap_servers, cfg.kafka_topic)
        kafka.check_topic()
        sinks.append(kafka)
    if cfg.events_jsonl:
        sinks.append(DailyJsonl(cfg.events_dir))
    return EventSinks(sinks)


def describe_outputs(cfg: Config) -> str:
    out = []
    if cfg.kafka_bootstrap_servers:
        out.append(f"Kafka topic {cfg.kafka_topic} at {cfg.kafka_bootstrap_servers}")
    if cfg.events_jsonl:
        out.append(f"JSONL in {cfg.events_dir.resolve()}")
    return " and ".join(out)

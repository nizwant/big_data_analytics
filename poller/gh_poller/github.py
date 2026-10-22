"""One poll of one GitHub Events API timeline: conditional request, pagination, error handling."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import httpx

API_VERSION = "2022-11-28"
PER_PAGE = 100
RETRY_DELAYS_S = (2, 4, 8)  # waits before the 2nd, 3rd and 4th attempt on 5xx or network errors
DEFAULT_SECONDARY_PAUSE_S = 60  # GitHub: without Retry-After, wait at least one minute
UNAUTHORIZED_HELP = "401 Unauthorized: GITHUB_TOKEN is invalid or expired. Polling stopped."


class Unauthorized(Exception):
    """HTTP 401. The token is bad for every request, so the process stops instead of backing off."""


def raise_if_unauthorized(resp: httpx.Response, what: str) -> None:
    if resp.status_code == 401:
        try:
            message = str(resp.json().get("message", ""))
        except ValueError:
            message = ""
        raise Unauthorized(f"{what}: HTTP 401 {message}".rstrip())


@dataclass
class PollResult:
    endpoint: str
    status: int | None = None  # HTTP status of page 1; None if no response arrived
    events: list[dict] = field(default_factory=list)  # raw events from all pages, API order (newest first)
    etag: str | None = None  # ETag to send next time; kept on 304 and errors, None after a partial poll
    poll_interval_s: int | None = None  # X-Poll-Interval header
    rate_remaining: int | None = None
    rate_reset: int | None = None  # epoch seconds
    requests: int = 0  # pages requested (retries not counted)
    pause_until: float | None = None  # epoch seconds; set when GitHub rate-limits us
    dead: bool = False  # 404/410: timeline no longer exists
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in (200, 304) and self.error is None


def build_headers(token: str, user_agent: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": user_agent,
    }


def _int(value: str | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def is_rate_limited(resp: httpx.Response) -> bool:
    """403/429 caused by rate limits, as opposed to a plain 'forbidden' on one timeline."""
    if resp.status_code == 429:
        return True
    if resp.status_code != 403:
        return False
    if resp.headers.get("retry-after") is not None or resp.headers.get("x-ratelimit-remaining") == "0":
        return True
    try:
        message = str(resp.json().get("message", ""))
    except ValueError:
        message = ""
    return "rate limit" in message.lower()


def pause_until(resp: httpx.Response, now: float) -> float:
    """Epoch time until which all polling should stop after a rate-limit response."""
    retry_after = _int(resp.headers.get("retry-after"))
    if retry_after is not None:
        return now + retry_after
    if resp.headers.get("x-ratelimit-remaining") == "0":
        reset = _int(resp.headers.get("x-ratelimit-reset"))
        if reset is not None:
            return float(reset) + 1
    return now + DEFAULT_SECONDARY_PAUSE_S


async def _get(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    params: dict | None,
    sleep: Callable[[float], Awaitable[None]],
    throttle: Callable[[], Awaitable[None]] | None = None,
) -> httpx.Response:
    """GET with retries on 5xx responses and network errors. `throttle` is awaited before every attempt."""
    attempts = (0, *RETRY_DELAYS_S)
    for i, delay in enumerate(attempts):
        if delay:
            await sleep(delay)
        if throttle is not None:
            await throttle()
        last = i == len(attempts) - 1
        try:
            resp = await client.get(url, headers=headers, params=params)
        except httpx.TransportError:
            if last:
                raise
            continue
        if resp.status_code >= 500 and not last:
            continue
        return resp
    raise AssertionError("unreachable")


async def poll(
    client: httpx.AsyncClient,
    api_url: str,
    endpoint: str,
    *,
    etag: str | None = None,
    is_seen: Callable[[str], bool] = lambda _id: False,
    max_pages: int = 3,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now: Callable[[], float] = time.time,
    throttle: Callable[[], Awaitable[None]] | None = None,
) -> PollResult:
    """Fetch new events from one timeline: page 1 with If-None-Match, then Link pages up to `max_pages`.

    Paging stops at a page with a seen event only when an ETag is stored: without one, an event
    written by the other track must not cut this timeline's history short.
    """
    res = PollResult(endpoint=endpoint, etag=etag)
    url = f"{api_url}{endpoint}"
    params: dict | None = {"per_page": PER_PAGE}
    headers = {"If-None-Match": etag} if etag else {}
    new_etag = etag

    for page in range(1, max_pages + 1):
        try:
            resp = await _get(client, url, headers, params, sleep, throttle)
        except httpx.TransportError as exc:
            res.error = f"{type(exc).__name__}: {exc}"
            break
        res.requests += 1
        remaining = _int(resp.headers.get("x-ratelimit-remaining"))
        if remaining is not None:
            res.rate_remaining = remaining
            res.rate_reset = _int(resp.headers.get("x-ratelimit-reset"))
        if page == 1:
            res.status = resp.status_code
            res.poll_interval_s = _int(resp.headers.get("x-poll-interval"))

        raise_if_unauthorized(resp, endpoint)
        if resp.status_code == 304:
            return res
        if resp.status_code in (404, 410):
            res.dead = True
            res.error = f"HTTP {resp.status_code}"
            return res
        if resp.status_code != 200:
            if is_rate_limited(resp):
                res.pause_until = pause_until(resp, now())
            res.error = f"HTTP {resp.status_code}"
            break
        try:
            batch = resp.json()
        except ValueError:
            res.error = "invalid JSON"
            break
        if not isinstance(batch, list):
            res.error = "unexpected response body"
            break

        if page == 1:
            new_etag = resp.headers.get("etag", etag)
        res.events.extend(batch)

        # Pages can hold fewer than PER_PAGE events even when more pages follow (GitHub filters
        # some events out), so the Link header, not the page size, says whether to continue.
        next_url = resp.links.get("next", {}).get("url")
        reached_known = etag is not None and any(is_seen(str(e.get("id"))) for e in batch)
        if not batch or not next_url or reached_known:
            break
        url, params, headers = next_url, None, {}

    # A later page failed: drop the ETag so the next poll fetches every page again (dedup skips repeats).
    if res.error is None:
        res.etag = new_etag
    elif res.status == 200:
        res.etag = None
    return res

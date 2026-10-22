import asyncio

import httpx
import pytest
import respx

from gh_poller.github import PER_PAGE, Unauthorized, build_headers, poll

API = "https://api.github.com"
FIXED_NOW = 1_800_000_000.0


def ev(i: int) -> dict:
    return {"id": str(i), "type": "PushEvent", "created_at": "2026-10-21T12:00:00Z"}


async def no_sleep(_seconds: float) -> None:
    return None


def run_poll(endpoint: str, **kwargs):
    kwargs.setdefault("sleep", no_sleep)
    kwargs.setdefault("now", lambda: FIXED_NOW)

    async def go():
        async with httpx.AsyncClient(headers=build_headers("test-token", "test")) as client:
            return await poll(client, API, endpoint, **kwargs)

    return asyncio.run(go())


@respx.mock
def test_200_returns_events_etag_and_headers():
    respx.get(f"{API}/orgs/r-lib/events").mock(
        return_value=httpx.Response(
            200,
            json=[ev(2), ev(1)],
            headers={"ETag": 'W/"abc"', "X-Poll-Interval": "60", "X-RateLimit-Remaining": "4999"},
        )
    )
    res = run_poll("/orgs/r-lib/events")
    assert res.ok
    assert [e["id"] for e in res.events] == ["2", "1"]
    assert res.etag == 'W/"abc"'
    assert res.poll_interval_s == 60
    assert res.rate_remaining == 4999
    assert res.requests == 1


@respx.mock
def test_304_sends_if_none_match_and_keeps_etag():
    route = respx.get(f"{API}/orgs/r-lib/events").mock(return_value=httpx.Response(304))
    res = run_poll("/orgs/r-lib/events", etag='W/"abc"')
    assert route.calls.last.request.headers["If-None-Match"] == 'W/"abc"'
    assert route.calls.last.request.headers["Authorization"] == "Bearer test-token"
    assert res.status == 304 and res.ok
    assert res.events == []
    assert res.etag == 'W/"abc"'


# Next-page links as GitHub sends them: /events links to itself, an organisation timeline links to
# /organizations/{id}/events (seen live for /orgs/r-lib/events), and every link also has rel="last".
ORG_PAGES = "/organizations/22618716/events"


def _paged(pages: dict[int, list[dict]], path: str = "/events"):
    """Side effect serving ?page=N with a Link header to the next and last page under `path`."""

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        headers = {"ETag": f'W/"p{page}"'}
        if page + 1 in pages:
            last = max(pages)
            headers["Link"] = (
                f'<{API}{path}?per_page={PER_PAGE}&page={page + 1}>; rel="next", '
                f'<{API}{path}?per_page={PER_PAGE}&page={last}>; rel="last"'
            )
        return httpx.Response(200, json=pages[page], headers=headers)

    return handler


@respx.mock
def test_pagination_stops_at_first_known_event():
    pages = {
        1: [ev(i) for i in range(300, 200, -1)],
        2: [ev(i) for i in range(200, 100, -1)],  # contains id 150, which we have seen
        3: [ev(i) for i in range(100, 0, -1)],
    }
    route = respx.get(f"{API}/events").mock(side_effect=_paged(pages))
    res = run_poll("/events", etag='W/"old"', is_seen=lambda i: i == "150")
    assert route.call_count == 2
    assert len(res.events) == 200
    assert res.etag == 'W/"p1"'  # ETag of page 1 is the one to send next time


@respx.mock
def test_first_poll_is_not_cut_short_by_event_seen_on_other_track():
    # No stored ETag yet. The timeline's newest event was already written from the global feed;
    # its older history on pages 2-3 must still be fetched.
    pages = {p: [ev(p * 1000 + i) for i in range(PER_PAGE)] for p in range(1, 4)}
    route = respx.get(url__regex=rf"^{API}(/orgs/r-lib/events|{ORG_PAGES})").mock(side_effect=_paged(pages, ORG_PAGES))
    res = run_poll("/orgs/r-lib/events", is_seen=lambda i: i == "1000")
    assert [c.request.url.path for c in route.calls] == ["/orgs/r-lib/events", ORG_PAGES, ORG_PAGES]  # follows Link
    assert len(res.events) == 300


@respx.mock
def test_short_page_with_next_link_continues():
    # Seen live: GitHub returned 96 events on page 1 of /orgs/ropensci/events with a next page.
    pages = {1: [ev(i) for i in range(1000, 904, -1)], 2: [ev(i) for i in range(904, 810, -1)]}
    route = respx.get(f"{API}/events").mock(side_effect=_paged(pages))
    res = run_poll("/events")
    assert route.call_count == 2
    assert len(res.events) == 96 + 94


@respx.mock
def test_pagination_respects_max_pages():
    pages = {p: [ev(p * 1000 + i) for i in range(PER_PAGE)] for p in range(1, 5)}
    route = respx.get(f"{API}/events").mock(side_effect=_paged(pages))
    res = run_poll("/events", max_pages=3)
    assert route.call_count == 3
    assert len(res.events) == 300


@respx.mock
def test_404_marks_endpoint_dead():
    respx.get(f"{API}/repos/gone/repo/events").mock(return_value=httpx.Response(404, json={"message": "Not Found"}))
    res = run_poll("/repos/gone/repo/events")
    assert res.dead
    assert res.pause_until is None


@respx.mock
def test_primary_rate_limit_pauses_until_reset():
    respx.get(f"{API}/events").mock(
        return_value=httpx.Response(
            403,
            json={"message": "API rate limit exceeded"},
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1800000500"},
        )
    )
    res = run_poll("/events", etag='W/"old"')
    assert res.pause_until == 1_800_000_501
    assert res.error == "HTTP 403"
    assert res.etag == 'W/"old"'


@respx.mock
def test_secondary_rate_limit_uses_retry_after():
    respx.get(f"{API}/events").mock(return_value=httpx.Response(429, headers={"Retry-After": "30"}))
    res = run_poll("/events")
    assert res.pause_until == FIXED_NOW + 30


@respx.mock
def test_secondary_rate_limit_without_retry_after_pauses_a_minute():
    # GitHub docs: a secondary limit may come as a plain 403 with only this message and quota left;
    # without Retry-After, wait at least one minute.
    respx.get(f"{API}/events").mock(
        return_value=httpx.Response(
            403,
            json={"message": "You have exceeded a secondary rate limit. Please wait a few minutes before you try again."},
            headers={"X-RateLimit-Remaining": "4000"},
        )
    )
    res = run_poll("/events")
    assert res.pause_until == FIXED_NOW + 60
    assert res.error == "HTTP 403"


@respx.mock
def test_plain_forbidden_does_not_pause_everyone():
    respx.get(f"{API}/orgs/private-org/events").mock(
        return_value=httpx.Response(403, json={"message": "Resource not accessible"}, headers={"X-RateLimit-Remaining": "4000"})
    )
    res = run_poll("/orgs/private-org/events")
    assert res.pause_until is None
    assert res.error == "HTTP 403"
    assert not res.dead


@respx.mock
def test_5xx_is_retried():
    route = respx.get(f"{API}/events").mock(
        side_effect=[httpx.Response(502), httpx.Response(503), httpx.Response(200, json=[ev(1)])]
    )
    res = run_poll("/events")
    assert route.call_count == 3
    assert res.ok and len(res.events) == 1


@respx.mock
def test_network_error_after_retries_is_reported():
    respx.get(f"{API}/events").mock(side_effect=httpx.ConnectError("boom"))
    res = run_poll("/events", etag='W/"old"')
    assert res.status is None
    assert res.error.startswith("ConnectError")
    assert res.etag == 'W/"old"'


@respx.mock
def test_error_on_later_page_returns_page_one_and_drops_etag():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") == "2":
            return httpx.Response(500)
        return httpx.Response(
            200,
            json=[ev(i) for i in range(PER_PAGE)],
            headers={"ETag": 'W/"new"', "Link": f'<{API}/events?per_page={PER_PAGE}&page=2>; rel="next"'},
        )

    respx.get(f"{API}/events").mock(side_effect=handler)
    res = run_poll("/events", etag='W/"old"')
    assert res.status == 200
    assert len(res.events) == PER_PAGE
    assert res.error == "HTTP 500"
    assert res.etag is None  # next poll fetches every page again (test_runner checks the recovery)


@respx.mock
def test_network_error_is_retried_three_times_with_backoff():
    sleeps: list[float] = []

    async def record(seconds: float) -> None:
        sleeps.append(seconds)

    route = respx.get(f"{API}/events").mock(side_effect=httpx.ReadTimeout("slow"))
    res = run_poll("/events", sleep=record)
    assert route.call_count == 4  # first try + 3 retries
    assert sleeps == [2, 4, 8]
    assert res.status is None and res.requests == 0
    assert res.error.startswith("ReadTimeout")


@respx.mock
def test_network_error_then_success():
    sleeps: list[float] = []

    async def record(seconds: float) -> None:
        sleeps.append(seconds)

    route = respx.get(f"{API}/events").mock(
        side_effect=[httpx.ConnectError("boom"), httpx.ConnectError("boom"), httpx.Response(200, json=[ev(1)])]
    )
    res = run_poll("/events", sleep=record)
    assert route.call_count == 3 and sleeps == [2, 4]
    assert res.ok and res.requests == 1 and len(res.events) == 1


@respx.mock
def test_401_raises_unauthorized_on_any_page():
    respx.get(f"{API}/orgs/r-lib/events").mock(return_value=httpx.Response(401, json={"message": "Bad credentials"}))
    with pytest.raises(Unauthorized, match="/orgs/r-lib/events: HTTP 401 Bad credentials"):
        run_poll("/orgs/r-lib/events", etag='W/"old"')

    def page2_unauthorized(request):
        if request.url.params.get("page") == "2":
            return httpx.Response(401)
        return httpx.Response(200, json=[ev(1)], headers={"Link": f'<{API}/events?per_page=100&page=2>; rel="next"'})

    respx.get(f"{API}/events").mock(side_effect=page2_unauthorized)
    with pytest.raises(Unauthorized):
        run_poll("/events")

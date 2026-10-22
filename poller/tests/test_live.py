"""Checks against the real GitHub API that the mocked tests rest on. About 7 requests.

Run only when GITHUB_TOKEN is set, in the environment or in poller/.env.
"""

import asyncio
import os
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from gh_poller.config import CRANTOGIT_URL
from gh_poller.github import Unauthorized, build_headers, poll
from gh_poller.watchlist import fetch_account, parse_crantogit, read_source

try:
    from dotenv import dotenv_values
except ImportError:
    dotenv_values = None

API = "https://api.github.com"
ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def _token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token and dotenv_values is not None and ENV_FILE.exists():
        token = dotenv_values(ENV_FILE).get("GITHUB_TOKEN") or ""
    return token.strip()


TOKEN = _token()
pytestmark = [pytest.mark.live, pytest.mark.skipif(not TOKEN, reason="needs GITHUB_TOKEN")]


def with_client(fn, token=TOKEN):
    async def go():
        async with httpx.AsyncClient(headers=build_headers(token, "bda-cran-poller-test"), timeout=30) as client:
            return await fn(client)

    return asyncio.run(go())


def test_timeline_events_and_304_on_its_etag():
    async def go(client):
        first = await poll(client, API, "/orgs/r-lib/events", max_pages=1)
        return first, await poll(client, API, "/orgs/r-lib/events", etag=first.etag, max_pages=1)

    first, second = with_client(go)
    assert first.status == 200 and first.etag and first.poll_interval_s >= 60
    assert first.events
    for e in first.events:  # the fields the poller and the record format rely on
        assert isinstance(e["id"], str) and e["type"].endswith("Event") and "/" in e["repo"]["name"]
        datetime.strptime(e["created_at"], "%Y-%m-%dT%H:%M:%SZ")
    assert second.status == 304 and second.events == []


def test_global_feed_follows_the_link_header():
    res = with_client(lambda client: poll(client, API, "/events", max_pages=2))
    assert res.error is None and res.requests == 2 and res.events


def test_account_lookup_gives_type_and_public_repos():
    org, user = with_client(lambda c: asyncio.gather(fetch_account(c, API, "r-lib"), fetch_account(c, API, "hadley")))
    assert org[0].type == "Organization" and org[0].public_repos > 0
    assert user[0].type == "User"


def test_bad_token_raises_unauthorized():
    with pytest.raises(Unauthorized, match="Bad credentials"):
        with_client(lambda client: poll(client, API, "/events", max_pages=1), token="invalid")


def test_crantogit_parses_into_github_packages():
    packages = parse_crantogit(asyncio.run(read_source(CRANTOGIT_URL)))
    assert len(packages) > 10_000
    assert any((p.package, p.owner, p.repo) == ("dplyr", "tidyverse", "dplyr") for p in packages)

"""Watch-list: build watchlist.csv from crantogit.csv (python -m gh_poller.watchlist) and load it.

Endpoint per package:
- owner in TIER1_ORGS -> /orgs/{owner}/events, tier 1
- organisation whose R repos are >= ORG_MIN_R_SHARE of its public repos -> /orgs/{owner}/events, tier 2
- anything else -> /repos/{owner}/{repo}/events, tier 2
Not /users/{name}/events/public: it lists what the user did anywhere, not events in their repos.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from .config import Config, load_config
from .github import Unauthorized, _get, _int, build_headers, is_rate_limited, raise_if_unauthorized

COLUMNS = ["package", "owner", "repo", "endpoint", "tier"]
CRAN_MIRROR_ORG = "cran"  # github.com/cran: read-only mirror with one repo per CRAN package
# Tier 1: the CRAN mirror and large R organisations.
TIER1_ORGS = (
    "cran", "tidyverse", "r-lib", "rstudio", "posit-dev", "ropensci", "tidymodels",
    "mlr-org", "r-spatial", "easystats", "r-dbi", "rdatatable",
)
QUOTA_RESERVE = 1000  # stop looking up accounts when X-RateLimit-Remaining falls below this


# --- Build ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Package:
    package: str
    owner: str  # lower case: GitHub names are case-insensitive, crantogit.csv is lower case anyway
    repo: str


@dataclass(frozen=True)
class Row:
    package: str
    owner: str
    repo: str
    endpoint: str
    tier: int


@dataclass(frozen=True)
class Account:
    type: str  # "Organization", "User", "Bot", or "missing" when GET /users/{name} returned 404
    public_repos: int | None = None


class LookupStopped(Exception):
    """Account lookups stopped early; running the build again continues where it stopped."""


def parse_crantogit(text: str) -> list[Package]:
    """Packages whose repository is on github.com and that no other registry (bioc, archived) claims."""
    packages: list[Package] = []
    for row in csv.DictReader(io.StringIO(text)):
        if (row.get("registry") or "").strip():
            continue
        url = urlsplit((row.get("url") or "").strip())
        parts = [p for p in url.path.split("/") if p]
        name = (row.get("package") or "").strip()
        if url.netloc.lower() not in ("github.com", "www.github.com") or len(parts) < 2 or not name:
            continue
        owner, repo = parts[0].lower(), parts[1].lower().removesuffix(".git")
        packages.append(Package(name, owner, repo))
    return packages


def repos_by_owner(packages: list[Package]) -> dict[str, set[str]]:
    repos: dict[str, set[str]] = defaultdict(set)
    for p in packages:
        repos[p.owner].add(p.repo)
    return repos


def owners_to_look_up(packages: list[Package], tier1: tuple[str, ...] = TIER1_ORGS) -> list[str]:
    """Owners whose account type decides the endpoint: two or more repos and not a tier-1 organisation."""
    return sorted(o for o, repos in repos_by_owner(packages).items() if len(repos) >= 2 and o not in tier1)


def uses_org_timeline(account: Account | None, r_repos: int, min_r_share: float) -> bool:
    if account is None or account.type != "Organization":
        return False
    if min_r_share <= 0 or not account.public_repos:
        return True
    return r_repos / account.public_repos >= min_r_share


def plan_endpoints(
    packages: list[Package],
    accounts: dict[str, Account],
    *,
    org_min_r_share: float,
    tier1: tuple[str, ...] = TIER1_ORGS,
) -> list[Row]:
    repos = repos_by_owner(packages)
    rows: list[Row] = []
    for p in packages:
        if p.owner in tier1:
            rows.append(Row(p.package, p.owner, p.repo, f"/orgs/{p.owner}/events", 1))
        elif len(repos[p.owner]) >= 2 and uses_org_timeline(accounts.get(p.owner), len(repos[p.owner]), org_min_r_share):
            rows.append(Row(p.package, p.owner, p.repo, f"/orgs/{p.owner}/events", 2))
        else:
            rows.append(Row(p.package, p.owner, p.repo, f"/repos/{p.owner}/{p.repo}/events", 2))
    # Tier-1 organisations are polled even without a package row (always the case for the CRAN mirror).
    present = {r.owner for r in rows}
    rows += [Row("", org, "", f"/orgs/{org}/events", 1) for org in tier1 if org not in present]
    return sorted(rows, key=lambda r: (r.tier, r.endpoint, r.package))


class AccountCache:
    """Account types from GET /users/{name}, one CSV row per lookup, so a stopped build can resume."""

    FIELDS = ["owner", "type", "public_repos", "checked_at"]

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.accounts: dict[str, Account] = {}
        if self.path.exists():
            with open(self.path, encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    # A line cut short by a crash has no complete timestamp; that owner is looked up again.
                    if row.get("owner") and row.get("type") and len(row.get("checked_at") or "") == 20:
                        self.accounts[row["owner"].lower()] = Account(row["type"], _int(row.get("public_repos") or None))

    def add(self, owner: str, account: Account) -> None:
        new_file = not self.path.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            if new_file:
                writer.writerow(self.FIELDS)
            public = "" if account.public_repos is None else account.public_repos
            writer.writerow([owner, account.type, public, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())])
        self.accounts[owner] = account


def _reset_text(reset: int | None) -> str:
    return time.strftime("%H:%M:%S UTC", time.gmtime(reset)) if reset else "unknown"


async def fetch_account(client: httpx.AsyncClient, api_url: str, owner: str) -> tuple[Account, int | None, int | None]:
    """GET /users/{owner} -> (account, X-RateLimit-Remaining, X-RateLimit-Reset)."""
    try:
        resp = await _get(client, f"{api_url}/users/{owner}", {}, None, asyncio.sleep)
    except httpx.TransportError as exc:
        raise LookupStopped(f"GET /users/{owner}: {type(exc).__name__}: {exc}") from exc
    raise_if_unauthorized(resp, f"GET /users/{owner}")
    remaining = _int(resp.headers.get("x-ratelimit-remaining"))
    reset = _int(resp.headers.get("x-ratelimit-reset"))
    if resp.status_code == 200:
        body = resp.json()
        return Account(str(body.get("type")), _int(str(body.get("public_repos")))), remaining, reset
    if resp.status_code in (404, 410):
        return Account("missing"), remaining, reset
    if is_rate_limited(resp):
        raise LookupStopped(f"rate limited by GitHub; the quota resets at {_reset_text(reset)}")
    raise LookupStopped(f"GET /users/{owner}: HTTP {resp.status_code}")


async def resolve_accounts(
    client: httpx.AsyncClient, api_url: str, owners: list[str], cache: AccountCache, reserve: int = QUOTA_RESERVE
) -> int:
    """Look up owners missing from the cache, one request at a time. Returns the number looked up."""
    todo = [o for o in owners if o not in cache.accounts]
    for i, owner in enumerate(todo, start=1):
        account, remaining, reset = await fetch_account(client, api_url, owner)
        cache.add(owner, account)
        if remaining is not None and remaining < reserve and i < len(todo):
            raise LookupStopped(
                f"X-RateLimit-Remaining is {remaining} (< {reserve}), left for the poller; "
                f"the quota resets at {_reset_text(reset)}"
            )
    return len(todo)


def write_watchlist(rows: list[Row], path: str | Path) -> None:
    """Write via a temporary file and rename, so the poller never reads a half-written list."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows([r.package, r.owner, r.repo, r.endpoint, r.tier] for r in rows)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


async def read_source(source: str) -> str:
    if source.startswith(("http://", "https://")):
        # A separate client without the token: it is not an API request.
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as plain:
            resp = await plain.get(source)
            resp.raise_for_status()
            return resp.text
    return Path(source).read_text(encoding="utf-8")


def account_cache_path(cfg: Config) -> Path:
    return cfg.data_dir / "account_types.csv"


async def build(cfg: Config) -> list[str]:
    """crantogit.csv -> watchlist.csv. Returns summary lines; raises LookupStopped if types are missing."""
    packages = parse_crantogit(await read_source(cfg.crantogit_url))
    cache = AccountCache(account_cache_path(cfg))
    owners = owners_to_look_up(packages)
    async with httpx.AsyncClient(
        headers=build_headers(cfg.github_token, cfg.user_agent), timeout=httpx.Timeout(30.0), follow_redirects=True
    ) as client:
        try:
            looked_up = await resolve_accounts(client, cfg.api_url, owners, cache)
        except LookupStopped as exc:
            known = sum(o in cache.accounts for o in owners)
            raise LookupStopped(f"{exc}. Account types known for {known} of {len(owners)} owners; run again to continue.") from exc
    rows = plan_endpoints(packages, cache.accounts, org_min_r_share=cfg.org_min_r_share)
    write_watchlist(rows, cfg.watchlist_path)
    return summarize(packages, cache.accounts, owners, looked_up, rows, cfg)


def summarize(packages, accounts, owners, looked_up, rows, cfg) -> list[str]:
    repos = repos_by_owner(packages)
    types = Counter(accounts[o].type for o in owners)
    endpoints = {r.endpoint: r.tier for r in rows}
    per_tier = Counter(endpoints.values())
    split = [
        (o, len(repos[o]), accounts[o].public_repos)
        for o in owners
        if accounts[o].type == "Organization" and not uses_org_timeline(accounts[o], len(repos[o]), cfg.org_min_r_share)
    ]
    lines = [
        f"packages {len(packages)}, repos {sum(len(r) for r in repos.values())}, owners {len(repos)}",
        f"owners with >= 2 repos looked up: {len(owners)} ({looked_up} now, {len(owners) - looked_up} from cache): "
        + ", ".join(f"{t} {n}" for t, n in types.most_common()),
        f"endpoints {len(endpoints)}: tier 1 {per_tier[1]}, tier 2 {per_tier[2]} "
        f"(/orgs {sum(e.startswith('/orgs/') for e in endpoints)}, /repos {sum(e.startswith('/repos/') for e in endpoints)})",
    ]
    if split:
        split.sort(key=lambda x: -(x[2] or 0))
        lines.append(
            f"organisations polled per repo (R repos < {cfg.org_min_r_share:.0%} of public repos): {len(split)}, e.g. "
            + ", ".join(f"{o} {n}/{p}" for o, n, p in split[:8])
        )
    lines.append(f"written {cfg.watchlist_path}")
    return lines


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    cfg = load_config()
    try:
        lines = asyncio.run(build(cfg))
    except LookupStopped as exc:
        print(f"watch-list not written: {exc}", file=sys.stderr)
        return 1
    except Unauthorized as exc:
        raise SystemExit(f"401 Unauthorized: GITHUB_TOKEN is invalid or expired. Watch-list not written. ({exc})") from None
    print("\n".join(lines))
    return 0


# --- Load (used by the poller) ----------------------------------------------------------------


class WatchlistError(ValueError):
    pass


@dataclass
class Watchlist:
    tiers: dict[str, int] = field(default_factory=dict)  # endpoint -> tier
    packages: dict[str, list[str]] = field(default_factory=dict)  # "owner/repo" (lower case) -> packages
    weight: dict[str, int] = field(default_factory=dict)  # endpoint -> number of packages it covers
    skipped_rows: int = 0

    def packages_for(self, repo_name: str | None) -> list[str]:
        """Packages developed in the repository `owner/repo` (an event's repo.name)."""
        if not repo_name:
            return []
        found = self.packages.get(repo_name.lower())
        if found:
            return list(found)
        owner, _, repo = repo_name.partition("/")
        if owner.lower() == CRAN_MIRROR_ORG and repo:  # mirror repo cran/<package>
            return [repo]
        return []

    def order(self) -> list[str]:
        """Endpoints in the order of their first poll: tier 1 first, then those covering more packages."""
        return sorted(self.tiers, key=lambda e: (self.tiers[e], -self.weight.get(e, 0), e))


def load_watchlist(path: str | Path) -> Watchlist:
    wl = Watchlist()
    with open(path, encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if [f.strip() for f in reader.fieldnames or []] != COLUMNS:
            raise WatchlistError(f"{path}: expected the header {','.join(COLUMNS)}")
        for row in reader:
            value = {k: (row.get(k) or "").strip() for k in COLUMNS}
            endpoint = value["endpoint"]
            if value["tier"] not in ("1", "2") or not endpoint.startswith("/") or not endpoint.endswith("/events"):
                wl.skipped_rows += 1
                continue
            tier = int(value["tier"])
            wl.tiers[endpoint] = min(tier, wl.tiers.get(endpoint, tier))
            if value["package"]:
                wl.weight[endpoint] = wl.weight.get(endpoint, 0) + 1
                if value["owner"] and value["repo"]:
                    key = f"{value['owner']}/{value['repo']}".lower()
                    if value["package"] not in wl.packages.setdefault(key, []):
                        wl.packages[key].append(value["package"])
    if not wl.tiers:
        raise WatchlistError(f"{path}: no valid rows")
    return wl


if __name__ == "__main__":
    sys.exit(main())

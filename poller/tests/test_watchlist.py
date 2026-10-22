import asyncio
import csv
import dataclasses

import httpx
import pytest
import respx

from gh_poller.watchlist import (
    Account,
    AccountCache,
    LookupStopped,
    Row,
    WatchlistError,
    build,
    load_watchlist,
    main,
    parse_crantogit,
    write_watchlist,
)

from helpers import API, make_cfg

CRANTOGIT = """package,url,subdir,registry
solo,https://github.com/Alice/Solo,,
mono1,https://github.com/mono/bigrepo,pkg1,
mono2,https://github.com/mono/bigrepo,pkg2,
ua,https://github.com/bob/ua,,
ub,https://github.com/bob/ub.git,,
oa,https://github.com/rorg/oa,,
ob,https://github.com/rorg/ob/,,
ma,https://github.com/megacorp/ma,,
mb,https://github.com/megacorp/mb,,
ga,https://github.com/ghost/ga,,
gb,https://github.com/ghost/gb,,
dplyr,https://github.com/tidyverse/dplyr,,
tidyr,https://github.com/tidyverse/tidyr,,
onlygl,https://gitlab.com/x/onlygl,,
bio,https://github.com/bioc-dev/bio,,bioc
old,https://github.com/gone/old,,archived
"""

# GET /users/{name}; any other name answers 404 (deleted or renamed account).
ACCOUNTS = {
    "bob": {"type": "User", "public_repos": 40},
    "rorg": {"type": "Organization", "public_repos": 3},  # 2 of 3 repos are R packages
    "megacorp": {"type": "Organization", "public_repos": 500},  # 2 of 500
}
USERS = rf"^{API}/users/[^/]+$"
RESET = "1800000000"


def users_handler(remaining: str = "4000"):
    def handler(request: httpx.Request) -> httpx.Response:
        name = request.url.path.rsplit("/", 1)[-1]
        headers = {"X-RateLimit-Remaining": remaining, "X-RateLimit-Reset": RESET}
        if name in ACCOUNTS:
            return httpx.Response(200, json={"login": name, **ACCOUNTS[name]}, headers=headers)
        return httpx.Response(404, json={"message": "Not Found"}, headers=headers)

    return handler


def build_cfg(tmp_path, **changes):
    source = tmp_path / "crantogit.csv"
    source.write_text(CRANTOGIT, encoding="utf-8")
    cfg = make_cfg(tmp_path)
    return dataclasses.replace(cfg, crantogit_url=str(source), watchlist_path=tmp_path / "out" / "watchlist.csv", **changes)


def read_rows(path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def endpoints_by_package(path) -> dict[str, tuple[str, str]]:
    return {r["package"]: (r["endpoint"], r["tier"]) for r in read_rows(path) if r["package"]}


def looked_up(route) -> list[str]:
    return [c.request.url.path.rsplit("/", 1)[-1] for c in route.calls]


def test_parse_crantogit_keeps_github_packages_without_registry():
    packages = parse_crantogit(CRANTOGIT)
    by_name = {p.package: (p.owner, p.repo) for p in packages}
    assert len(packages) == 13
    assert not {"onlygl", "bio", "old"} & set(by_name)  # GitLab, Bioconductor, archived
    assert by_name["solo"] == ("alice", "solo")  # lower case
    assert by_name["ub"] == ("bob", "ub")  # .git suffix
    assert by_name["ob"] == ("rorg", "ob")  # trailing slash


@respx.mock
def test_build_chooses_one_endpoint_per_owner(tmp_path):
    route = respx.get(url__regex=USERS).mock(side_effect=users_handler())
    cfg = build_cfg(tmp_path)
    lines = asyncio.run(build(cfg))

    assert endpoints_by_package(cfg.watchlist_path) == {
        "solo": ("/repos/alice/solo/events", "2"),  # one repo
        "mono1": ("/repos/mono/bigrepo/events", "2"),  # two packages, one repo
        "mono2": ("/repos/mono/bigrepo/events", "2"),
        "ua": ("/repos/bob/ua/events", "2"),  # User: per repo, never /users/bob/events/public
        "ub": ("/repos/bob/ub/events", "2"),
        "oa": ("/orgs/rorg/events", "2"),  # organisation made mostly of R repos
        "ob": ("/orgs/rorg/events", "2"),
        "ma": ("/repos/megacorp/ma/events", "2"),  # large organisation with few R repos
        "mb": ("/repos/megacorp/mb/events", "2"),
        "ga": ("/repos/ghost/ga/events", "2"),  # account not found
        "gb": ("/repos/ghost/gb/events", "2"),
        "dplyr": ("/orgs/tidyverse/events", "1"),  # tier-1 organisation, no lookup
        "tidyr": ("/orgs/tidyverse/events", "1"),
    }
    rows = read_rows(cfg.watchlist_path)
    assert list(rows[0]) == ["package", "owner", "repo", "endpoint", "tier"]
    tier1 = {r["endpoint"] for r in rows if r["tier"] == "1"}
    assert {"/orgs/cran/events", "/orgs/r-lib/events", "/orgs/tidyverse/events"} <= tier1
    assert not any("/users/" in r["endpoint"] for r in rows)
    assert looked_up(route) == ["bob", "ghost", "megacorp", "rorg"]  # only owners with >= 2 repos
    assert any("organisations polled per repo" in line and "megacorp 2/500" in line for line in lines)


@respx.mock
def test_org_share_rule_can_be_switched_off(tmp_path):
    respx.get(url__regex=USERS).mock(side_effect=users_handler())
    cfg = build_cfg(tmp_path, org_min_r_share=0.0)
    asyncio.run(build(cfg))
    assert endpoints_by_package(cfg.watchlist_path)["ma"] == ("/orgs/megacorp/events", "2")


@respx.mock
def test_build_resumes_after_a_rate_limit(tmp_path):
    cfg = build_cfg(tmp_path)
    ok = users_handler()
    answered: list[str] = []

    def limited_after_first(request: httpx.Request) -> httpx.Response:
        answered.append(request.url.path)
        if len(answered) == 1:
            return ok(request)
        return httpx.Response(
            403, json={"message": "API rate limit exceeded"},
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": RESET},
        )

    route = respx.get(url__regex=USERS).mock(side_effect=limited_after_first)
    with pytest.raises(LookupStopped, match="known for 1 of 4 owners"):
        asyncio.run(build(cfg))
    assert not cfg.watchlist_path.exists()

    assert looked_up(route) == ["bob", "ghost"]
    route.side_effect = ok
    asyncio.run(build(cfg))
    assert looked_up(route)[2:] == ["ghost", "megacorp", "rorg"]  # bob came from the cache
    assert endpoints_by_package(cfg.watchlist_path)["ua"] == ("/repos/bob/ua/events", "2")


@respx.mock
def test_build_leaves_quota_for_the_poller(tmp_path):
    route = respx.get(url__regex=USERS).mock(side_effect=users_handler(remaining="900"))
    cfg = build_cfg(tmp_path)
    with pytest.raises(LookupStopped, match="X-RateLimit-Remaining is 900"):
        asyncio.run(build(cfg))
    assert route.call_count == 1
    assert list(AccountCache(cfg.data_dir / "account_types.csv").accounts) == ["bob"]


@respx.mock
def test_crantogit_is_downloaded_without_the_token(tmp_path):
    url = "https://raw.example.org/crantogit.csv"
    download = respx.get(url).mock(return_value=httpx.Response(200, text=CRANTOGIT))
    users = respx.get(url__regex=USERS).mock(side_effect=users_handler())
    cfg = dataclasses.replace(build_cfg(tmp_path), crantogit_url=url)
    asyncio.run(build(cfg))
    assert "authorization" not in download.calls.last.request.headers
    assert users.calls.last.request.headers["authorization"] == "Bearer t"


def test_account_cache_ignores_a_line_cut_short(tmp_path):
    path = tmp_path / "account_types.csv"
    AccountCache(path).add("bob", Account("User", 40))
    with open(path, "a", encoding="utf-8") as fh:
        fh.write("rorg,Organization,3,2026-10-2")  # crash while writing
    assert AccountCache(path).accounts == {"bob": Account("User", 40)}


def test_load_watchlist_tiers_and_package_lookup(tmp_path):
    path = tmp_path / "watchlist.csv"
    path.write_text(
        "﻿package,owner,repo,endpoint,tier\n"  # a BOM, as Excel writes it
        "dplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\n"
        "tidyr,tidyverse,tidyr,/orgs/tidyverse/events,1\n"
        ",cran,,/orgs/cran/events,1\n"
        "mono1,mono,bigrepo,/repos/mono/bigrepo/events,2\n"
        "mono2,mono,bigrepo,/repos/mono/bigrepo/events,2\n"
        "x1,x,x,/repos/x/x/events,2\n"
        "x2,x,x,/repos/x/x/events,1\n"  # same endpoint in two tiers: the more frequent wins
        "bad1,b,b,/repos/b/b/events,3\n"
        "bad2,b,b,not-an-endpoint,2\n",
        encoding="utf-8",
    )
    wl = load_watchlist(path)
    assert wl.tiers == {
        "/orgs/tidyverse/events": 1,
        "/orgs/cran/events": 1,
        "/repos/mono/bigrepo/events": 2,
        "/repos/x/x/events": 1,
    }
    assert wl.skipped_rows == 2
    assert wl.packages_for("Tidyverse/DPLYR") == ["dplyr"]  # repo.name case does not matter
    assert wl.packages_for("mono/bigrepo") == ["mono1", "mono2"]
    assert wl.packages_for("cran/RCurl") == ["RCurl"]  # CRAN mirror: repo name is the package
    assert wl.packages_for("someone/else") == [] and wl.packages_for(None) == []
    assert wl.order() == [
        "/orgs/tidyverse/events", "/repos/x/x/events", "/orgs/cran/events", "/repos/mono/bigrepo/events"
    ]


@pytest.mark.parametrize("content", ["endpoint,tier\n/orgs/a/events,1\n", "package,owner,repo,endpoint,tier\n"],
                         ids=["wrong-header", "no-rows"])
def test_invalid_watchlist_is_rejected(tmp_path, content):
    path = tmp_path / "watchlist.csv"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(WatchlistError):
        load_watchlist(path)


def test_write_watchlist_replaces_the_file_in_one_step(tmp_path):
    path = tmp_path / "watchlist.csv"
    path.write_text("old", encoding="utf-8")
    write_watchlist([Row("dplyr", "tidyverse", "dplyr", "/orgs/tidyverse/events", 1)], path)
    assert path.read_text(encoding="utf-8") == "package,owner,repo,endpoint,tier\ndplyr,tidyverse,dplyr,/orgs/tidyverse/events,1\n"
    assert list(tmp_path.iterdir()) == [path]  # no temporary file left
    assert load_watchlist(path).tiers == {"/orgs/tidyverse/events": 1}


@respx.mock
def test_401_ends_the_build_with_a_clear_message(tmp_path, monkeypatch):
    source = tmp_path / "crantogit.csv"
    source.write_text(CRANTOGIT, encoding="utf-8")
    monkeypatch.chdir(tmp_path)  # no .env here: the test must not read a real token
    monkeypatch.setenv("GITHUB_TOKEN", "expired")
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("CRANTOGIT_URL", str(source))
    for name in ("WATCHLIST_PATH", "GITHUB_API_URL"):
        monkeypatch.delenv(name, raising=False)
    respx.get(url__regex=USERS).mock(return_value=httpx.Response(401, json={"message": "Bad credentials"}))
    with pytest.raises(SystemExit, match="401 Unauthorized.*Watch-list not written"):
        main()
    assert not (tmp_path / "data" / "watchlist.csv").exists()


@respx.mock
def test_rebuild_takes_type_and_public_repos_from_the_cache(tmp_path):
    route = respx.get(url__regex=USERS).mock(side_effect=users_handler())
    cfg = build_cfg(tmp_path)
    asyncio.run(build(cfg))
    first = read_rows(cfg.watchlist_path)
    assert AccountCache(cfg.data_dir / "account_types.csv").accounts["megacorp"] == Account("Organization", 500)

    asyncio.run(build(cfg))
    assert route.call_count == 4  # no request in the second build
    assert read_rows(cfg.watchlist_path) == first  # the share rule decided megacorp from the cached public_repos

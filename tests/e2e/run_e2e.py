"""End-to-end functional and quality tests of the data acquisition and pre-processing pipeline.

Run against a started dev stack (deploy/up.sh, deploy/start_logs.sh, deploy/start_github.sh), with the venv of
tests/e2e/requirements.txt:

    python tests/e2e/run_e2e.py

Every test case writes a log to deliverables/test_logs/<ID>.log; results.json feeds the LaTeX test document
(tests/e2e/make_tex.py). The run injects clearly marked test records (documentation IP range 203.0.113.0/24,
package names e2etest.<tag>) next to the live data, so it can run on a stack that is processing real events.
"""

from __future__ import annotations

import json
import os
import re
import secrets
import subprocess
import sys
import time
from collections import Counter
from datetime import timedelta

import pandas as pd

import lib
from lib import REPO, Case, compose, compose_text, execute, iso, log_ts, utcnow, wait_for

TAG = secrets.token_hex(3)  # marks the records of this run
PKG = f"e2etest.{TAG}"
STATE: dict = {}
POLLER_ARGS = ("--profile", "github")
ALLOWED_METHODS = {"GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "CONNECT", "TRACE"}
COMMON_LOG = re.compile(r'^(\S+) \S+ \S+ \[(\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}) \+0000\] "(\S+) (\S+) ([^"]+)" (\d{3}) (\d+|-)$')


def line(ip, ts, method, path, status, size, proto="HTTP/1.1", offset="+0000"):
    return f'{ip} - - [{log_ts(ts, offset)}] "{method} {path} {proto}" {status} {size}'


def gh_event(event_id, ts, *, etype="ReleaseEvent", track="watchlist", repo="test-org/e2e-repo", org="test-org",
             payload=None, repo_name=True, created_at=None):
    event = {
        "id": str(event_id), "type": etype, "actor": {"id": 1001, "login": "e2e-tester"},
        "repo": {"id": 4242, **({"name": repo} if repo_name else {})},
        "payload": payload if payload is not None else {"action": "published", "release": {"tag_name": "v1.2.3"}},
        "public": True, "created_at": created_at or iso(ts),
    }
    if org:
        event["org"] = {"id": 7, "login": org}
    return {"fetched_at": iso(ts), "track": track, "endpoint": "/orgs/test-org/events", "packages": [PKG], "event": event}


# =================================================================================================
# Functional tests: data acquisition
# =================================================================================================


def fa01(t: Case):
    before = lib.end_offsets("cran_mirror_logs")
    time.sleep(10)
    after = lib.end_offsets("cran_mirror_logs")
    new = sum(after.values()) - sum(before.values())
    t.out(f"end offsets before: {before}")
    t.out(f"end offsets after 10 s: {after}")
    t.check(new >= 20, f"{new} new messages in 10 s (generator rate is about 4 per second)")
    sample = [m.value.decode() for m in lib.consume_all("cran_mirror_logs")[:200]]
    ok = [bool(COMMON_LOG.match(s)) for s in sample]
    t.out("first sample lines:")
    for s in sample[:3]:
        t.out("  " + s)
    t.check(all(ok), f"{sum(ok)}/{len(sample)} sampled messages match the common log format")
    t.actual = f"{new} messages in 10 s; {sum(ok)}/{len(sample)} sampled lines are valid common-log lines."


def fa02(t: Case):
    topics = ["cran_mirror_logs", "cran_mirror_logs_clean", "cran_mirror_package_downloads", "github_repo_updates",
              "github_repo_updates_clean", "github_repo_list"]
    text = compose_text("exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:19092",
                        "--describe")
    t.out("\n".join(l for l in text.splitlines() if l.startswith("Topic:")))
    for name in topics:
        m = re.search(rf"Topic: {name}\s.*?PartitionCount: (\d+)", text)
        t.check(m and int(m.group(1)) == 3, f"topic {name} exists with 3 partitions")
    subjects = lib.registry_get("/subjects")
    t.out(f"registry subjects: {subjects}")
    for s in ("cran_mirror_logs_clean", "cran_mirror_package_downloads", "github_repo_updates_clean"):
        t.check(f"{s}-value" in subjects, f"Avro subject {s}-value is registered")
    dirs = compose_text("exec", "-T", "namenode", "hdfs", "dfs", "-ls", "/data/raw", "/data/clean")
    t.out(dirs)
    listed = {l.split()[-1] for l in dirs.splitlines() if l.startswith("d")}  # exact paths, not substrings
    for d in ("raw/cran_mirror_logs", "raw/cran_mirror_logs_bad", "raw/github_repo_updates", "raw/github_repo_updates_bad",
              "raw/github_repo_list", "clean/cran_mirror_logs", "clean/cran_mirror_package_downloads",
              "clean/github_repo_updates"):
        t.check(f"/data/{d}" in listed, f"HDFS directory /data/{d} exists")
    t.actual = "6 topics (3 partitions), 3 Avro subjects and 8 HDFS data directories exist."


def pytest_run(t: Case, marker: str) -> str:
    p = subprocess.run([sys.executable, "-m", "pytest", "-m", marker, "-v", "--no-header", "-p", "no:cacheprovider"],
                       cwd=REPO / "poller", capture_output=True, text=True, timeout=900)
    t.out(p.stdout.strip())
    if p.stderr.strip():
        t.out(p.stderr[-800:])
    summary = [l for l in p.stdout.splitlines() if re.search(r"\d+ (passed|failed|skipped|error)", l)]
    return summary[-1].strip(" =") if summary else "no summary"


def fa03(t: Case):
    summary = pytest_run(t, "not live")
    t.check("failed" not in summary and "error" not in summary and "passed" in summary, f"pytest: {summary}")
    t.actual = f"pytest -m 'not live': {summary}."


def fa04(t: Case):
    if not os.environ.get("GITHUB_TOKEN"):
        t.check(False, "GITHUB_TOKEN is not set, live tests cannot run")
        return
    summary = pytest_run(t, "live")
    t.check("failed" not in summary and "error" not in summary and "passed" in summary, f"pytest: {summary}")
    t.actual = f"pytest -m live (real GitHub API): {summary}."


def fa05(t: Case):
    runs = json.loads(compose_text("exec", "-T", "airflow", "airflow", "dags", "list-runs", "github_watchlist", "--output", "json")
                      or "[]")
    t.out("runs of github_watchlist: " + json.dumps([{k: r.get(k) for k in ("run_id", "state", "run_after")} for r in runs[:5]]))
    t.check(bool(runs) and runs[0]["state"] == "success", "the newest run of the DAG github_watchlist succeeded")
    landed = compose_text("exec", "-T", "namenode", "hdfs", "dfs", "-ls", "-R", "/data/raw/crantogit", check=False)
    files = [l.split()[-1] for l in landed.splitlines() if l.endswith("/crantogit.csv")]
    t.out("landed crantogit.csv files: " + ", ".join(files))
    t.check(bool(files), "the DAG landed crantogit.csv in /data/raw/crantogit/dt=...")

    # The newest complete build, derived independently of the poller's code: rows grouped by built_at,
    # complete once rows_in_snapshot distinct rows have arrived.
    cols = ["package", "owner", "repo", "endpoint", "tier"]
    rows = [json.loads(m.value) for m in lib.consume_all("github_repo_list")]
    builds: dict[str, dict] = {}
    for r in rows:
        if "rows_in_snapshot" in r:
            b = builds.setdefault(r["built_at"], {"expected": r["rows_in_snapshot"], "rows": {}})
            b["rows"][tuple(str(r[c]) for c in cols)] = r
    complete = sorted(k for k, b in builds.items() if len(b["rows"]) == b["expected"])
    t.out(f"github_repo_list: {len(rows)} messages, {len(builds)} builds, {len(complete)} complete")
    t.check(bool(complete), "github_repo_list holds a complete watch-list build")
    if not complete:
        return
    last = complete[-1]
    wl = pd.DataFrame(list(builds[last]["rows"].values()))[cols]
    t.out(f"newest complete build: {last}, {len(wl)} rows; example message: {json.dumps(next(iter(builds[last]['rows'].values())))}")
    t.df(wl, n=5)
    t.out(f"tier counts: {wl['tier'].value_counts().to_dict()}")
    t.check(set(wl["tier"]) <= {1, 2}, "every row has tier 1 or 2")
    t.check(wl["endpoint"].str.match(r"^/(orgs|repos)/[^/]+(/[^/]+)?/events$").all(), "every endpoint is an /orgs/... or /repos/... timeline")
    poller_log = compose_text(*POLLER_ARGS, "logs", "github-poller")
    t.check(f"built at {last}" in poller_log, f"the poller applied the build of {last}")
    t.actual = (f"DAG run {runs[0]['state']}; newest complete build {last}: {len(wl)} rows "
                f"({wl['tier'].value_counts().to_dict()}), applied by the poller.")


def fa06(t: Case):
    msgs = [m for m in lib.consume_all("github_repo_updates") if m.value.startswith(b"{")]
    recs = [(m, json.loads(m.value)) for m in msgs]
    t.out(f"{len(recs)} messages in github_repo_updates")
    t.check(len(recs) > 0, "the poller has produced events")
    keys_ok = all(m.key and m.key.decode() == r["event"]["repo"]["name"] for m, r in recs)
    t.check(keys_ok, "the message key equals event.repo.name for every message (events of one repository stay ordered)")
    t.check(all({"fetched_at", "track", "endpoint", "packages", "event"} <= set(r) for _, r in recs),
            "every message has fetched_at, track, endpoint, packages and event")
    t.check(all(r["track"] in ("watchlist", "global") for _, r in recs), "track is watchlist or global")
    types = Counter(r["event"]["type"] for _, r in recs)
    t.out(f"event types: {dict(types.most_common(8))}")
    with_pkg = sum(1 for _, r in recs if r["packages"])
    t.out(f"events with at least one CRAN package: {with_pkg}")
    t.check(with_pkg > 0, "events are matched to CRAN packages through the watch-list")
    sample = next(r for _, r in recs if r["packages"])
    t.out("example (payload shortened): " + json.dumps({**sample, "event": {k: v for k, v in sample["event"].items() if k != "payload"}})[:600])
    t.actual = f"{len(recs)} events in Kafka, keys correct, {with_pkg} matched to CRAN packages."


def fa07(t: Case):
    raw = compose_text(*POLLER_ARGS, "exec", "-T", "github-poller", "sh", "-c", "cat /data/logs/requests-*.jsonl")
    log = pd.DataFrame([json.loads(l) for l in raw.splitlines() if l.strip()])
    t.df(log, ["ts", "endpoint", "tier", "status", "requests", "events", "new_events", "rate_remaining", "next_poll_s"], n=6)
    counts = log["status"].value_counts().to_dict()
    t.out(f"polls by HTTP status: {counts}")
    t.out(f"polls: {len(log)}, last known quota remaining: {log['rate_remaining'].dropna().iloc[-1]}")
    t.check(counts.get(200, 0) > 0, "full responses (200) were received")
    t.check(counts.get(304, 0) > 0, f"conditional requests were answered with 304 Not Modified ({counts.get(304, 0)} polls, no quota used)")
    t.check(not log["rate_limited"].fillna(False).any(), "the poller never hit a GitHub rate limit")
    t.check(log["rate_remaining"].dropna().min() > 1000, "the remaining quota stayed above the 1000 request reserve")
    t.actual = f"{len(log)} polls, statuses {counts}; no rate limit; minimum remaining quota {int(log['rate_remaining'].dropna().min())}."


def raw_gh_ids() -> list[str]:
    """Event ids of the poller's records; non-JSON records (the injected invalid test record) are skipped."""
    ids = []
    for m in lib.consume_all("github_repo_updates"):
        try:
            ids.append(json.loads(m.value)["event"]["id"])
        except (ValueError, KeyError, TypeError):
            pass
    return ids


def fa08_prepare():
    # Records injected by earlier runs of this harness are not the poller's, so they are left out here and in fa08
    ids = [i for i in raw_gh_ids() if not i.startswith("e2e-")]
    STATE["gh_ids_before"] = set(ids)
    STATE["gh_dups_before"] = len(ids) - len(set(ids))
    STATE["restart_at"] = time.time()
    compose(*POLLER_ARGS, "restart", "github-poller")


def fa08(t: Case):
    ids = raw_gh_ids()
    # Events injected by this run (FP-04/FP-05) are produced by the test, not by the poller
    ids = [i for i in ids if not i.startswith("e2e-")]
    dup = len(ids) - len(set(ids))
    t.out(f"before restart: {len(STATE['gh_ids_before'])} distinct ids, {STATE['gh_dups_before']} duplicates")
    t.out(f"after restart ({time.time() - STATE['restart_at']:.0f} s later): {len(ids)} messages, {len(set(ids))} distinct ids")
    raw = compose_text(*POLLER_ARGS, "exec", "-T", "github-poller", "sh", "-c", "cat /data/logs/requests-*.jsonl")
    log = pd.DataFrame([json.loads(l) for l in raw.splitlines() if l.strip()])
    log["ts"] = pd.to_datetime(log["ts"], utc=True)
    restarted = pd.Timestamp(STATE["restart_at"], unit="s", tz="UTC")
    after = log[log["ts"] > restarted]
    counts = after["status"].value_counts().to_dict()
    t.out(f"polls after the restart ({restarted:%H:%M:%S} UTC): {len(after)}, by status: {counts}")
    t.check(len(after) > 0, f"the poller polled again after the restart ({len(after)} polls), so the duplicate check below is meaningful")
    t.check(counts.get(304, 0) > 0, f"{counts.get(304, 0)} polls after the restart got 304 Not Modified: the stored ETags survived the restart")
    t.check(STATE["gh_ids_before"] <= set(ids), "all events from before the restart are still in the topic")
    t.check(dup == 0, f"{dup} duplicate event ids in github_repo_updates after the restart (written event ids kept in SQLite)")
    t.actual = f"{len(after)} polls after the restart ({counts}); {len(set(ids))} distinct events, {dup} duplicates."


# =================================================================================================
# Functional tests: pre-processing (Flink jobs)
# =================================================================================================


def inject():
    now = utcnow()
    ips = {f"v{i}": f"203.0.113.{10 + i}" for i in range(1, 7)} | {f"i{i}": f"203.0.113.{20 + i}" for i in range(1, 7)}
    pkg2 = f"{PKG}x"
    valid = {
        "v1": line(ips["v1"], now, "GET", f"/src/contrib/{PKG}_1.2.3-4.tar.gz", 200, 12345),
        "v2": line(ips["v2"], now, "GET", f"/bin/macosx/big-sur-arm64/contrib/4.5/{PKG}_1.2.3-4.tgz", 200, 23456),
        "v3": line(ips["v3"], now, "GET", f"/bin/windows/contrib/4.5/{PKG}_1.2.3-4.zip", 200, 34567),
        "v4": line(ips["v4"], now, "GET", f"/bin/windows/contrib/4.5/{pkg2}_9.9.9.zip", 404, 196),
        "v5": line(ips["v5"], now, "GET", "/src/contrib/PACKAGES.rds", 200, 2048),
        "v6": line(ips["v6"], now, "GET", f"/web/packages/{PKG}/index.html", 304, "-"),
    }
    invalid = {
        "i1": f"this is not a log line {PKG}",
        "i2": line(ips["i2"], now, "GET", f"/src/contrib/{PKG}_0.0.1.tar.gz", 999, 10),
        "i3": line(ips["i3"], now, "FOO", f"/src/contrib/{PKG}_0.0.1.tar.gz", 200, 10),
        "i4": line(ips["i4"], now + timedelta(days=1), "GET", f"/src/contrib/{PKG}_0.0.1.tar.gz", 200, 10),
        "i5": line(ips["i5"], now - timedelta(days=30), "GET", f"/src/contrib/{PKG}_0.0.1.tar.gz", 200, 10),
        "i6": line(ips["i6"], now, "GET", f"/src/contrib/{PKG}_0.0.1.tar.gz", 200, 10, offset="+0200"),
    }
    expected_reason = {"i1": "unparseable", "i2": "invalid_status", "i3": "invalid_method", "i4": "future_timestamp",
                       "i5": "stale_timestamp", "i6": "unparseable"}
    lib.produce("cran_mirror_logs", [(None, l) for l in (*valid.values(), *invalid.values())])

    base = gh_event(f"e2e-{TAG}-1", now)
    push = gh_event(f"e2e-{TAG}-2", now, etype="PushEvent", org=None, payload={"ref": "refs/heads/main", "size": 1},
                    repo="someone/e2e-repo2")
    gh_bad = {
        "stale": gh_event(f"e2e-{TAG}-s", now - timedelta(days=30)),
        "track": gh_event(f"e2e-{TAG}-t", now, track="other"),
        "nojson": f"e2e-{TAG}-n this is not json",
        "norepo": gh_event(f"e2e-{TAG}-m", now, repo_name=False),
        "badts": gh_event(f"e2e-{TAG}-b", now, created_at="yesterday-ish"),
    }
    gh_reason = {"stale": "stale_timestamp", "track": "invalid_track", "nojson": "unparseable",
                 "norepo": "missing_field", "badts": "invalid_timestamp"}
    records = [(base["event"]["repo"]["name"], json.dumps(base)), (base["event"]["repo"]["name"], json.dumps(base)),
               (push["event"]["repo"]["name"], json.dumps(push))]
    for k, v in gh_bad.items():
        records.append(("e2e/bad", v if isinstance(v, str) else json.dumps(v)))
    lib.produce("github_repo_updates", records)
    STATE.update(now=now, ips=ips, pkg2=pkg2, valid=valid, invalid=invalid, expected_reason=expected_reason,
                 base=base, push=push, gh_reason=gh_reason, injected_at=time.time())


def find_clean():
    return [r for r in lib.avro_records("cran_mirror_logs_clean") if r["ip"].startswith("203.0.113.")]


def fp01(t: Case):
    ips = STATE["ips"]
    found = wait_for(lambda: (lambda rs: rs if len({r["ip"] for r in rs if r["ip"] in (ips[f"v{i}"] for i in range(1, 7))}) >= 6 else None)(find_clean()),
                     120, 5, "the six valid test lines in cran_mirror_logs_clean")
    by_ip = {r["ip"]: r for r in found}
    t.out(pd.DataFrame([by_ip[ips[f"v{i}"]] for i in range(1, 7)])[["event_ts", "ip", "http_method", "path", "status", "bytes_sent"]].to_string())
    v1 = by_ip[ips["v1"]]
    t.check(v1["http_method"] == "GET" and v1["status"] == 200 and v1["bytes_sent"] == 12345, "v1: method, status and size are parsed")
    t.check(v1["path"] == f"/src/contrib/{PKG}_1.2.3-4.tar.gz" and v1["protocol"] == "HTTP/1.1", "v1: path and protocol are parsed")
    t.check(v1["event_ts"].replace(microsecond=0) == STATE["now"], "v1: event_ts is the time from the log line (UTC)")
    t.check(v1["arrival_ts"] >= v1["event_ts"] - timedelta(seconds=2), "v1: arrival_ts (Kafka timestamp) is set separately from event_ts")
    t.check(by_ip[ips["v6"]]["bytes_sent"] is None, "v6: '-' as size becomes null")
    t.check(by_ip[ips["v4"]]["status"] == 404 and by_ip[ips["v5"]]["path"] == "/src/contrib/PACKAGES.rds",
            "404 and non-package requests are kept in the clean topic (needed for bot detection)")
    t.actual = "All 6 valid test lines reached cran_mirror_logs_clean with correct fields; '-' size became null."


def fp02(t: Case):
    ips, exp = STATE["ips"], STATE["expected_reason"]

    def bad_rows():
        df = lib.read_parquet("/data/raw/cran_mirror_logs_bad")
        return df[df["raw_line"].str.contains(PKG, regex=False)] if len(df) else None

    df = wait_for(lambda: (lambda d: d if d is not None and len(d) >= 6 else None)(bad_rows()), 240, 15,
                  "the rejected test lines in /data/raw/cran_mirror_logs_bad")
    t.df(df, ["arrival_ts", "raw_line", "reject_reason", "dt", "hr"], n=8)
    for k, reason in exp.items():
        row = df[df["raw_line"] == STATE["invalid"][k]]
        t.check(len(row) == 1 and row.iloc[0]["reject_reason"] == reason,
                f"{k}: rejected as {reason} (got {row.iloc[0]['reject_reason'] if len(row) else 'not found'})")
    clean = {r["ip"] for r in find_clean()}
    t.check(not any(ips[k] in clean for k in exp if k != "i1"), "no rejected line reached the clean topic")
    t.actual = "6/6 invalid lines were rejected with the expected reject_reason and none reached the clean topic."


def fp03(t: Case):
    ips = STATE["ips"]
    marker = {ips[f"v{i}"] for i in range(1, 7)}
    found = wait_for(lambda: (lambda rs: rs if len(rs) >= 4 else None)([r for r in lib.avro_records("cran_mirror_package_downloads") if r["ip"] in marker]),
                     120, 5, "the test downloads in cran_mirror_package_downloads")
    df = pd.DataFrame(found).sort_values("ip")
    t.df(df, ["event_ts", "ip", "package_name", "package_version", "artifact_type", "target_system", "outcome"], n=6)
    by_ip = {r["ip"]: r for r in found}
    exp = {"v1": (PKG, "1.2.3-4", "tar.gz", "source", "SUCCESS"), "v2": (PKG, "1.2.3-4", "tgz", "macos", "SUCCESS"),
           "v3": (PKG, "1.2.3-4", "zip", "windows", "SUCCESS"), "v4": (STATE["pkg2"], "9.9.9", "zip", "windows", "FAILURE")}
    for k, want in exp.items():
        r = by_ip.get(ips[k])
        got = (r["package_name"], r["package_version"], r["artifact_type"], r["target_system"], r["outcome"]) if r else None
        t.check(got == want, f"{k}: {want[2]} -> package {want[0]} {want[1]}, {want[3]}, {want[4]}")
    t.check(ips["v5"] not in by_ip and ips["v6"] not in by_ip, "PACKAGES.rds and an HTML page are not downloads (v5, v6 are skipped)")
    t.check(all(r["_key"] == r["package_name"] for r in found), "the Kafka key equals package_name")
    t.actual = "4/4 package requests were extracted correctly (source, macos, windows, 404 as FAILURE); 2 non-downloads skipped."


def gh_clean(prefix=None):
    return [r for r in lib.avro_records("github_repo_updates_clean") if r["event_id"].startswith(prefix or f"e2e-{TAG}")]


def fp04(t: Case):
    found = wait_for(lambda: (lambda rs: rs if len({r["event_id"] for r in rs}) >= 2 else None)(gh_clean()), 120, 5,
                     "the injected GitHub events in github_repo_updates_clean")
    df = pd.DataFrame(found)
    t.df(df, ["event_id", "event_type", "event_ts", "track", "repo_name", "org_login", "packages", "ref", "release_tag"], n=4)
    one = [r for r in found if r["event_id"] == f"e2e-{TAG}-1"]
    t.check(len(one) == 1, f"event e2e-{TAG}-1 was produced twice and appears {len(one)} time in the clean topic (deduplication)")
    r = one[0]
    t.check(r["event_type"] == "ReleaseEvent" and r["release_tag"] == "v1.2.3", "ReleaseEvent: release_tag is extracted from payload.release.tag_name")
    t.check(r["packages"] == [PKG] and r["org_login"] == "test-org" and r["repo_name"] == "test-org/e2e-repo",
            "packages, org_login and repo_name are carried over")
    t.check(r["event_ts"].replace(microsecond=0) == STATE["now"] and r["track"] == "watchlist", "event_ts is GitHub's created_at; track is kept")
    t.check(r["_key"] == r["repo_name"], "the Kafka key equals repo_name")
    p = [x for x in found if x["event_id"] == f"e2e-{TAG}-2"][0]
    t.check(p["event_type"] == "PushEvent" and p["ref"] == "refs/heads/main" and p["org_login"] is None,
            "PushEvent: ref is extracted and a missing organisation becomes null")
    t.check(json.loads(r["payload_json"])["release"]["tag_name"] == "v1.2.3", "payload_json keeps the original payload as JSON text")
    t.actual = "Duplicate event kept once; release_tag, ref, packages and nullable org_login mapped correctly."


def fp05(t: Case):
    exp = STATE["gh_reason"]

    def rows():
        df = lib.read_parquet("/data/raw/github_repo_updates_bad")
        if not len(df):
            return None
        col = [c for c in df.columns if c not in ("arrival_ts", "reject_reason", "dt", "hr", "_file")][0]
        sel = df[df[col].astype(str).str.contains(f"e2e-{TAG}", regex=False)]
        sel = sel.assign(line=sel[col])
        return sel if len(sel) >= 5 else None

    df = wait_for(rows, 240, 15, "the rejected test events in /data/raw/github_repo_updates_bad")
    t.df(df.assign(line=df["line"].str.slice(0, 60)), ["arrival_ts", "line", "reject_reason", "dt", "hr"], n=6)
    for k, reason in exp.items():
        suffix = {"stale": "-s", "track": "-t", "nojson": "-n", "norepo": "-m", "badts": "-b"}[k]
        row = df[df["line"].str.contains(f"e2e-{TAG}{suffix}", regex=False)]
        t.check(len(row) == 1 and row.iloc[0]["reject_reason"] == reason,
                f"{k}: rejected as {reason} (got {row.iloc[0]['reject_reason'] if len(row) else 'not found'})")
    clean_ids = {r["event_id"] for r in gh_clean()}
    t.check(not any(i.endswith(("-s", "-t", "-n", "-m", "-b")) for i in clean_ids),
            "none of the rejected events reached the clean topic")
    t.actual = "5/5 invalid GitHub events were rejected with the expected reject_reason."


def fp06(t: Case):
    cran_raw = wait_for(lambda: (lambda ls: ls if all(l in set(ls) for l in (*STATE["valid"].values(), *STATE["invalid"].values())) else None)(
        lib.read_text_lines("/data/raw/cran_mirror_logs")), 240, 15, "the injected lines in /data/raw/cran_mirror_logs")
    s = set(cran_raw)
    for k, l in {**STATE["valid"], **STATE["invalid"]}.items():
        t.check(l in s, f"{k}: the line is stored unchanged in /data/raw/cran_mirror_logs")
    gh_raw = lib.read_text_lines("/data/raw/github_repo_updates")
    mine = [l for l in gh_raw if f"e2e-{TAG}" in l]
    t.out(f"/data/raw/github_repo_updates: {len(mine)} lines contain this run's event ids (expected 8: 1 duplicate + 7 distinct)")
    t.check(len(mine) == 8, "all 8 produced GitHub records (including the duplicate and the invalid ones) are stored raw")
    t.actual = "All 12 injected log lines and 8 injected GitHub records are stored unchanged in the raw zone."


# =================================================================================================
# Quality tests: transformation
# =================================================================================================


def wait_for_flink_lag():
    groups = ["flink-cran-logs-ingest", "flink-cran-package-downloads", "flink-github-events-ingest"]

    def lag_zero():
        total = 0
        for g in groups:
            txt = compose_text("exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server",
                               "kafka:19092", "--describe", "--group", g, check=False)
            for l in txt.splitlines():
                p = l.split()
                if len(p) >= 6 and p[5].isdigit():
                    total += int(p[5])
        return total == 0

    wait_for(lag_zero, 300, 10, "Flink consumer lag 0")


def q01(t: Case):
    specs = {"cran_mirror_logs_clean": ("CranMirrorLogClean", "/data/clean/cran_mirror_logs"),
             "cran_mirror_package_downloads": ("CranMirrorPackageDownload", "/data/clean/cran_mirror_package_downloads"),
             "github_repo_updates_clean": ("GithubRepoUpdateClean", "/data/clean/github_repo_updates")}
    for topic, (_, hdfs) in specs.items():
        latest = lib.registry_get(f"/subjects/{topic}-value/versions/latest")
        fields = [f["name"] for f in json.loads(latest["schema"])["fields"]]
        recs = lib.avro_records(topic)
        t.check(len(recs) > 0, f"{topic}: {len(recs)} messages decode with the registered schema (version {latest['version']})")
        t.check(all(set(fields) <= set(r) for r in recs), f"{topic}: every record has all {len(fields)} schema fields")
        df = lib.read_parquet(hdfs)
        t.check(set(fields) <= set(df.columns), f"{topic}: Parquet columns match the Avro fields")
        t.out(f"  Parquet dtypes: {dict(df[fields].dtypes.astype(str))}")
        nulls = {f: int(df[f].isna().sum()) for f in fields if df[f].isna().any()}
        t.out(f"  columns with nulls (nullable in the schema): {nulls}")
    logs = pd.DataFrame(lib.avro_records("cran_mirror_logs_clean"))
    t.check(logs["status"].between(100, 599).all(), "cran_mirror_logs_clean: status is within 100-599")
    t.check(logs["http_method"].isin(ALLOWED_METHODS).all(), "cran_mirror_logs_clean: http_method is an allowed method")
    dl = pd.DataFrame(lib.avro_records("cran_mirror_package_downloads"))
    t.check(dl["artifact_type"].isin({"tar.gz", "tgz", "zip"}).all() and dl["target_system"].isin({"source", "macos", "windows"}).all()
            and dl["outcome"].isin({"SUCCESS", "FAILURE"}).all(), "cran_mirror_package_downloads: artifact_type, target_system and outcome use the allowed values")
    gh = pd.DataFrame(lib.avro_records("github_repo_updates_clean"))
    t.check(gh["track"].isin({"global", "watchlist"}).all() and gh["event_id"].notna().all() and gh["repo_name"].notna().all(),
            "github_repo_updates_clean: track is valid, event_id and repo_name are never null")
    t.actual = "All three clean topics decode with the registry schema; Parquet columns match; value constraints hold."


def q02(t: Case):
    gh = pd.DataFrame(lib.avro_records("github_repo_updates_clean"))
    dups = int(gh["event_id"].duplicated().sum())
    t.out(f"github_repo_updates_clean topic: {len(gh)} records, {gh['event_id'].nunique()} distinct event ids")
    t.check(dups == 0, f"{dups} duplicate event ids in the clean topic")
    pq = lib.read_parquet("/data/clean/github_repo_updates")
    pdups = int(pq["event_id"].duplicated().sum())
    t.out(f"github_repo_updates Parquet: {len(pq)} rows, {pq['event_id'].nunique()} distinct event ids")
    t.check(pdups == 0, f"{pdups} duplicate event ids in the Parquet files")
    raw_ids = raw_gh_ids()
    t.out(f"raw topic: {len(raw_ids)} records with a parseable id, {len(set(raw_ids))} distinct (repeats removed by the job: {len(raw_ids) - len(set(raw_ids))})")
    t.actual = f"{len(gh)} clean events, 0 duplicates (topic and Parquet); {len(raw_ids) - len(set(raw_ids))} repeated raw records were dropped."


def q03(t: Case):
    raw_topic = sum(lib.end_offsets("cran_mirror_logs").values())
    raw_hdfs = len(lib.read_text_lines("/data/raw/cran_mirror_logs"))
    clean_topic = len(lib.consume_all("cran_mirror_logs_clean"))
    clean_pq = len(lib.read_parquet("/data/clean/cran_mirror_logs"))
    bad_pq = len(lib.read_parquet("/data/raw/cran_mirror_logs_bad"))
    t.out(pd.DataFrame({"count": [raw_topic, raw_hdfs, clean_topic, clean_pq, bad_pq]},
                       index=["cran_mirror_logs (Kafka)", "raw text (HDFS)", "cran_mirror_logs_clean (Kafka)",
                              "clean Parquet (HDFS)", "bad Parquet (HDFS)"]).to_string())
    t.check(raw_topic == raw_hdfs, f"raw topic = raw HDFS lines ({raw_topic} = {raw_hdfs}): nothing lost on the way to the raw zone")
    t.check(clean_topic == clean_pq, f"clean topic = clean Parquet rows ({clean_topic} = {clean_pq})")
    t.check(raw_topic == clean_pq + bad_pq, f"raw = clean + rejected ({raw_topic} = {clean_pq} + {bad_pq}): every line is accounted for")
    t.actual = f"{raw_topic} raw lines = {clean_pq} clean + {bad_pq} rejected; Kafka and HDFS counts agree."


def q04(t: Case):
    checks = [("clean CRAN logs", "/data/clean/cran_mirror_logs", "event_ts"),
              ("package downloads", "/data/clean/cran_mirror_package_downloads", "event_ts"),
              ("clean GitHub events", "/data/clean/github_repo_updates", "event_ts")]
    for name, path, col in checks:
        df = lib.read_parquet(path)
        ts = pd.to_datetime(df[col], utc=True)
        ok = ((ts.dt.strftime("%Y-%m-%d") == df["dt"]) & (ts.dt.strftime("%H") == df["hr"])).all()
        t.out(f"{name}: {len(df)} rows in {df[['dt', 'hr']].drop_duplicates().shape[0]} dt/hr partitions")
        t.check(ok, f"{name}: every row is in the dt/hr directory of its event_ts (UTC)")
    bad = lib.read_parquet("/data/raw/cran_mirror_logs_bad")
    ts = pd.to_datetime(bad["arrival_ts"], utc=True)
    t.check(((ts.dt.strftime("%Y-%m-%d") == bad["dt"]) & (ts.dt.strftime("%H") == bad["hr"])).all(),
            "rejected CRAN lines are partitioned by arrival time")
    t.actual = "All Parquet rows are in the partition matching their event time (rejected lines: arrival time)."


def q05(t: Case):
    for topic, keyf in (("cran_mirror_package_downloads", "package_name"), ("github_repo_updates_clean", "repo_name")):
        df = pd.DataFrame(lib.avro_records(topic))
        parts = df.groupby(keyf)["_partition"].nunique()
        t.out(f"{topic}: {len(df)} records, {df[keyf].nunique()} distinct {keyf} values, partitions used: {sorted(df['_partition'].unique())}")
        t.check((parts == 1).all(), f"{topic}: every {keyf} is in exactly one partition")
        t.check((df["_key"] == df[keyf]).all(), f"{topic}: the message key equals {keyf}")
    t.actual = "Records with the same package_name / repo_name always go to the same partition, so their order is kept."


def q06(t: Case):
    bad = lib.read_parquet("/data/raw/cran_mirror_logs_bad")
    reasons = Counter(bad["reject_reason"])
    t.out(f"CRAN rejects by reason: {dict(reasons)}")
    mine = bad[bad["raw_line"].str.contains(PKG, regex=False)]
    others = bad[~bad["raw_line"].str.contains("e2etest.", regex=False)]
    exp = Counter(STATE["expected_reason"].values())
    t.check(len(others) == 0, f"the generator produced only valid lines: {len(others)} rejects that are not test lines")
    t.check(Counter(mine["reject_reason"]) == exp, f"the {len(mine)} rejected test lines have exactly the injected reasons {dict(exp)}")
    gb = lib.read_parquet("/data/raw/github_repo_updates_bad")
    greasons = Counter(gb["reject_reason"])
    clean = len(lib.read_parquet("/data/clean/github_repo_updates"))
    t.out(f"GitHub rejects by reason: {dict(greasons)}; clean events: {clean}")
    allowed = {"unparseable", "invalid_timestamp", "invalid_track", "stale_timestamp", "missing_field"}
    t.check(set(greasons) <= allowed, "GitHub reject reasons are within the documented set")
    t.check(greasons.get("stale_timestamp", 0) >= 1, "stale history (older than 7 days) returned on the first poll is rejected, not loaded")
    rate = len(gb) / max(1, len(gb) + clean)
    t.out(f"GitHub reject rate (mostly stale history of the first poll): {rate:.1%}")
    t.actual = f"CRAN: {len(bad)} rejects, all of them injected test lines. GitHub: {len(gb)} rejects ({dict(greasons)}), {clean} clean events."


def q07(t: Case):
    logs = lib.read_parquet("/data/clean/cran_mirror_logs")
    ev, ar = pd.to_datetime(logs["event_ts"], utc=True), pd.to_datetime(logs["arrival_ts"], utc=True)
    t.out(f"CRAN: event_ts - arrival_ts ranges from {(ev - ar).min()} to {(ev - ar).max()}")
    t.check(((ev - ar) <= timedelta(minutes=5)).all() and ((ar - ev) <= timedelta(days=7)).all(),
            "CRAN clean records: event_ts is at most 5 minutes after and 7 days before arrival_ts")
    gh = lib.read_parquet("/data/clean/github_repo_updates")
    e, f = pd.to_datetime(gh["event_ts"], utc=True), pd.to_datetime(gh["fetched_ts"], utc=True)
    t.out(f"GitHub: fetched_ts - event_ts ranges from {(f - e).min()} to {(f - e).max()}")
    t.check(((f - e) <= timedelta(days=7, minutes=5)).all(), "GitHub clean records are at most about 7 days old when fetched")
    t.check(((e - f) <= timedelta(minutes=5)).all(), "GitHub clean records are not in the future")
    t.actual = "All clean records satisfy the event-time bounds enforced by the ingest jobs."


# =================================================================================================
# Example data (pandas)
# =================================================================================================


def examples():
    out = []

    def add(title, df, cols=None, n=5, note=""):
        out.append(f"=== {title}")
        if note:
            out.append(note)
        shown = df[cols] if cols else df
        out.append(f">>> df.shape\n{df.shape}")
        out.append(f">>> df.dtypes\n{df.dtypes.astype(str).to_string()}")
        out.append(f">>> df.head({n})\n{shown.head(n).to_string(max_colwidth=36)}")
        out.append("")

    add("Clean CRAN access log (/data/clean/cran_mirror_logs)", lib.read_parquet("/data/clean/cran_mirror_logs").drop(columns=["_file"]),
        ["event_ts", "ip", "http_method", "path", "status", "bytes_sent", "dt", "hr"])
    dl = lib.read_parquet("/data/clean/cran_mirror_package_downloads").drop(columns=["_file"])
    add("Package downloads (/data/clean/cran_mirror_package_downloads)", dl,
        ["event_ts", "ip", "package_name", "package_version", "artifact_type", "target_system", "outcome"])
    out.append(f">>> df.groupby(['target_system', 'outcome']).size()\n{dl.groupby(['target_system', 'outcome']).size().to_string()}\n")
    gh = lib.read_parquet("/data/clean/github_repo_updates").drop(columns=["_file"])
    add("Clean GitHub events (/data/clean/github_repo_updates)", gh,
        ["event_ts", "event_type", "repo_name", "actor_login", "track", "packages", "release_tag"])
    out.append(f">>> df['event_type'].value_counts().head(6)\n{gh['event_type'].value_counts().head(6).to_string()}\n")
    add("Rejected CRAN lines (/data/raw/cran_mirror_logs_bad)", lib.read_parquet("/data/raw/cran_mirror_logs_bad").drop(columns=["_file"]),
        ["arrival_ts", "raw_line", "reject_reason", "dt", "hr"], n=6)
    gb = lib.read_parquet("/data/raw/github_repo_updates_bad").drop(columns=["_file"])
    gb = gb.assign(**{gb.columns[1]: gb[gb.columns[1]].astype(str).str.slice(0, 50)})
    add("Rejected GitHub records (/data/raw/github_repo_updates_bad)", gb, n=6)
    (lib.LOG_DIR / "DATA-EXAMPLES.log").write_text("\n".join(out))


# =================================================================================================


CASES = {
    "FA-01": Case("FA-01", "Acquisition", "Log source (log generator; Filebeat later) delivers log lines to Kafka",
                  "Check that the current CRAN log source, the log generator that simulates the mirror until Filebeat is deployed, "
                  "delivers well-formed access-log lines to the raw topic. It tests the interface (topic, line format) that Filebeat must also meet, not Filebeat itself.",
                  ["Read the end offsets of cran_mirror_logs, wait 10 s, read them again.",
                   "Read a sample of messages and match each against the Apache common log format."],
                  "At least 20 new messages in 10 s; every sampled message is a valid common-log line."),
    "FA-02": Case("FA-02", "Acquisition", "Platform initialisation: topics, schemas, HDFS directories",
                  "Check that deploy/init.sh created everything the acquisition modules write to.",
                  ["Describe the Kafka topics.", "List the Schema Registry subjects.", "List the HDFS raw and clean directories."],
                  "Six topics with 3 partitions, three Avro subjects and eight HDFS directories exist."),
    "FA-03": Case("FA-03", "Acquisition", "GitHub poller unit tests (mocked HTTP)",
                  "Check the poller logic offline: ETags, rate limits, scheduling, retries, watch-list, Kafka sink, state.",
                  ["Run pytest -m 'not live' in poller/."], "All tests pass."),
    "FA-04": Case("FA-04", "Acquisition", "GitHub poller live tests (real API)",
                  "Check the poller against the real GitHub Events API with the project token.",
                  ["Run pytest -m live in poller/ with GITHUB_TOKEN set."], "All live tests pass."),
    "FA-05": Case("FA-05", "Acquisition", "Watch-list build by the Airflow DAG and handoff to the poller via Kafka",
                  "Check that the DAG github_watchlist landed crantogit.csv in HDFS and published a complete watch-list "
                  "to github_repo_list, and that the poller applied it.",
                  ["Read the runs of the DAG github_watchlist.", "List the landed crantogit.csv files in HDFS.",
                   "Read github_repo_list, group the rows by built_at and take the newest build with all its "
                   "rows_in_snapshot rows as a DataFrame.", "Validate tiers and endpoints.",
                   "Search the poller's log for that build."],
                  "The newest DAG run succeeded and the raw file exists; the newest complete build is a valid "
                  "watch-list, and the poller applied it."),
    "FA-06": Case("FA-06", "Acquisition", "GitHub events reach Kafka with the documented format",
                  "Check the structure, keys and package matching of the events written by the poller.",
                  ["Read all messages of github_repo_updates.", "Check the keys, mandatory fields, track values and package matching."],
                  "Key = repository name; every message has the documented fields; events are matched to CRAN packages."),
    "FA-07": Case("FA-07", "Acquisition", "API limit handling: conditional requests and quota",
                  "Check from the poller's own request log that ETags are used and the quota is respected.",
                  ["Read the request log of the poller (requests-*.jsonl) as a DataFrame.", "Count polls by HTTP status.",
                   "Check rate-limit flags and the remaining quota."],
                  "Both 200 and 304 responses occur; no rate limit; remaining quota stays above the reserve."),
    "FA-08": Case("FA-08", "Acquisition", "Poller restart does not duplicate events",
                  "Check that the poller keeps its state (ETags, written event ids) across a restart.",
                  ["Record the event ids in github_repo_updates.", "Restart the poller container and wait for several poll rounds.",
                   "Check in the poller's request log that it polled again and that polls after the restart got 304 (ETags kept).",
                   "Check that no event id occurs twice and earlier events are still present."],
                  "Polls after the restart; some answered 304; no duplicate event ids."),
    "FP-01": Case("FP-01", "Pre-processing", "Valid CRAN log lines are parsed into the clean topic",
                  "Check the parsing of the Flink log job for valid lines.",
                  ["Produce six valid test lines to cran_mirror_logs.", "Read them from cran_mirror_logs_clean (Avro).",
                   "Compare every field with the input."],
                  "All six lines arrive with correct fields; '-' size becomes null."),
    "FP-02": Case("FP-02", "Pre-processing", "Invalid CRAN log lines are rejected with a reason",
                  "Check that invalid lines are kept in the bad zone and never reach the clean data.",
                  ["Produce six invalid lines (garbage, status 999, method FOO, +1 day, -30 days, offset +0200).",
                   "Read /data/raw/cran_mirror_logs_bad as a DataFrame.", "Compare reject_reason with the expectation.",
                   "Check the clean topic."],
                  "Each line is in the bad zone with the expected reason and absent from the clean topic."),
    "FP-03": Case("FP-03", "Pre-processing", "Package downloads are extracted from the clean log",
                  "Check the download extraction job: package, version, artifact type, target system, outcome.",
                  ["Use the valid test lines of FP-01 (source, macOS, Windows, a 404, PACKAGES.rds, an HTML page).",
                   "Read cran_mirror_package_downloads (Avro).", "Compare the derived fields; check the skipped requests."],
                  "Four downloads with correct fields (404 = FAILURE); the two non-package requests are skipped."),
    "FP-04": Case("FP-04", "Pre-processing", "GitHub events: mapping and deduplication",
                  "Check the GitHub Flink job: field extraction, nullable fields and removal of repeated event ids.",
                  ["Produce a ReleaseEvent twice and a PushEvent without organisation to github_repo_updates.",
                   "Read github_repo_updates_clean (Avro).", "Compare the fields; count the occurrences of the repeated event."],
                  "The repeated event appears once; fields are mapped; missing organisation is null."),
    "FP-05": Case("FP-05", "Pre-processing", "Invalid GitHub records are rejected with a reason",
                  "Check the validation rules of the GitHub job.",
                  ["Produce five invalid records (30 days old, track 'other', not JSON, no repo name, invalid timestamp).",
                   "Read /data/raw/github_repo_updates_bad as a DataFrame.", "Compare reject_reason; check the clean topic."],
                  "Each record is in the bad zone with the expected reason and absent from the clean topic."),
    "FP-06": Case("FP-06", "Pre-processing", "Raw records are stored unchanged",
                  "Check that the raw zone keeps every input record byte for byte, valid or not.",
                  ["Read the text files of /data/raw/cran_mirror_logs and /data/raw/github_repo_updates.",
                   "Look for each injected record."],
                  "All injected records are present unchanged."),
    "Q-01": Case("Q-01", "Quality", "Schema conformance and value constraints",
                 "Check that the Parquet files have the columns of the Avro schemas and that values satisfy the constraints Avro cannot express. "
                 "Decoding the Kafka messages is only a sanity check: the jobs write them with the registered schema.",
                 ["Decode every message of the three derived topics with the schema from the registry.",
                  "Compare the Parquet columns with the Avro fields.", "Check status range, methods, allowed values."],
                 "Everything decodes; columns match; all constraints hold."),
    "Q-02": Case("Q-02", "Quality", "No duplicate GitHub events in the clean data",
                 "Check that deduplication on event_id works on the live data.",
                 ["Count distinct event ids in the clean topic and in the Parquet files.", "Compare with the raw topic."],
                 "No event id occurs twice in the clean data."),
    "Q-03": Case("Q-03", "Quality", "Completeness: no record is lost between raw, clean and rejected",
                 "Check that raw = clean + rejected for the CRAN logs, and that Kafka and HDFS agree.",
                 ["Stop the producers and wait until the Flink consumer lag is 0 and the files are finalised.",
                  "Count the raw topic, raw text, clean topic, clean Parquet and bad Parquet."],
                 "raw = clean + rejected; topic counts equal file counts."),
    "Q-04": Case("Q-04", "Quality", "Partitioning by event time",
                 "Check that Parquet files are in the dt/hr directory of the record's time.",
                 ["Read the Parquet datasets and compare dt and hr with event_ts (arrival_ts for rejects)."],
                 "Every row is in the correct partition."),
    "Q-05": Case("Q-05", "Quality", "Keys and partition locality",
                 "Check that records of one package or repository stay in one partition.",
                 ["Group the downloads topic by package_name and the GitHub topic by repo_name.", "Count partitions per key."],
                 "Exactly one partition per key; key equals the field."),
    "Q-06": Case("Q-06", "Quality", "Reject accounting",
                 "Check that rejections happen only for the documented reasons and only where expected.",
                 ["Count the rejected CRAN lines by reason and compare them with the injected invalid lines.",
                  "Count the rejected GitHub records by reason."],
                 "CRAN rejects are exactly the injected ones; GitHub reasons are documented, stale history is rejected."),
    "Q-07": Case("Q-07", "Quality", "Event-time plausibility of the clean data",
                 "Check the time bounds the ingest jobs promise.",
                 ["Compute event_ts - arrival_ts (CRAN) and fetched_ts - event_ts (GitHub) over all clean rows."],
                 "CRAN: -7 d to +5 min; GitHub: 0 to about 7 d."),
}


def main() -> int:
    only = set(sys.argv[1:])
    if only == {"EXAMPLES"}:  # regenerate only the DataFrame printout
        examples()
        return 0

    def go(cid, fn):
        if not only or cid in only:
            execute(CASES[cid], fn)

    print(f"run tag {TAG}, package {PKG}", flush=True)
    go("FA-02", fa02)
    go("FA-01", fa01)
    go("FA-03", fa03)
    go("FA-04", fa04)
    go("FA-05", fa05)
    go("FA-06", fa06)
    full = not only
    if full or "FA-08" in only:
        fa08_prepare()
    if full or any(c.startswith(("FP", "Q")) for c in only):
        inject()
    for cid, fn in (("FP-01", fp01), ("FP-02", fp02), ("FP-03", fp03), ("FP-04", fp04), ("FP-05", fp05), ("FP-06", fp06)):
        go(cid, fn)
    # the poller needs a few rounds after its restart before FA-07 and FA-08 are meaningful
    remaining = 200 - (time.time() - STATE.get("restart_at", 0))
    if (full or "FA-08" in only) and remaining > 0:
        time.sleep(remaining)
    go("FA-07", fa07)
    go("FA-08", fa08)
    # quality tests: stop the producers, let the jobs finish and finalise their files
    if full:
        compose("--profile", "dev", "stop", "mirror-log-generator")
        compose(*POLLER_ARGS, "stop", "github-poller")
        wait_for_flink_lag()
        time.sleep(160)
        wait_for_flink_lag()
    for cid, fn in (("Q-01", q01), ("Q-02", q02), ("Q-03", q03), ("Q-04", q04), ("Q-05", q05), ("Q-06", q06), ("Q-07", q07)):
        go(cid, fn)
    if full:
        examples()
    lib.save_results()
    summary = Counter(c.status for c in lib.RESULTS)
    print(f"SUMMARY: {dict(summary)}")
    return 0 if summary.get("FAIL", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

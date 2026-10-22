"""Report on a poller run: statuses, rate limits, duplicates, volume, delays.

Usage (from poller/): python scripts/check_run.py [DATA_DIR]   (default: DATA_DIR from env or .env, else ./data)
Exit code 1 on a rate-limited request or a duplicated event id. Volume and delays count only polls
that came on schedule, leaving out first polls (30 days of history) and polls after a break.
"""

from __future__ import annotations

import json
import os
import statistics
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

TRACKS = ("global", "watchlist")
SLACK_S = 60  # a poll this much later than planned still counts as on schedule
POLLER_DIR = Path(__file__).resolve().parents[1]
DELAY_MARKS = ((600, "10 min"), (3600, "1 h"), (6 * 3600, "6 h"))


def parse_ts(value: str) -> float:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()


def resolve_data_dir(argv: list[str]) -> Path:
    if len(argv) > 1:
        return Path(argv[1])
    if os.environ.get("DATA_DIR"):
        return Path(os.environ["DATA_DIR"])
    env_file = next((p for p in (Path(".env"), POLLER_DIR / ".env") if p.is_file()), None)
    if env_file is not None:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            value = value.split(" #", 1)[0].strip().strip("'\"")
            if sep and key.strip() == "DATA_DIR" and value:
                path = Path(value)
                return path if path.is_absolute() else env_file.parent / path
    return Path("./data")


def read_jsonl(paths: list[Path]) -> list[dict]:
    rows: list[dict] = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            rows += [json.loads(line) for line in fh if line.strip()]
    return rows


def steady_state(req: list[dict]) -> tuple[set[tuple[str, str]], dict[str, list[tuple[float, float]]]]:
    """(endpoint, ts) of polls that followed the previous poll of the same timeline on schedule, and
    per track the spans between such pairs, i.e. the time the poller was running."""
    last: dict[str, dict] = {}
    steady: set[tuple[str, str]] = set()
    spans: dict[str, list[tuple[float, float]]] = {track: [] for track in TRACKS}
    for r in req:
        prev = last.get(r["endpoint"])
        last[r["endpoint"]] = r
        if prev is None:
            continue
        t0, t1 = parse_ts(prev["ts"]), parse_ts(r["ts"])
        if t1 - t0 <= (prev.get("next_poll_s") or 0) + SLACK_S:
            steady.add((r["endpoint"], r["ts"]))
            spans.setdefault(r["track"], []).append((t0, t1))
    return steady, spans


def merge(spans: list[tuple[float, float]]) -> list[list[float]]:
    """Join spans that overlap or lie less than SLACK_S apart into runs."""
    runs: list[list[float]] = []
    for a, b in sorted(spans):
        if runs and a <= runs[-1][1] + SLACK_S:
            runs[-1][1] = max(runs[-1][1], b)
        else:
            runs.append([a, b])
    return runs


def minutes(runs: list[list[float]]) -> float:
    return sum(b - a for a, b in runs) / 60


def rate_limited(r: dict) -> bool:
    if "rate_limited" in r:  # request logs from v1 on
        return bool(r["rate_limited"])
    return r["status"] in (403, 429) or r.get("error") in ("HTTP 403", "HTTP 429")


def seconds_summary(values: list[float]) -> str:
    if not values:
        return "n/a"
    q = statistics.quantiles(values, n=10, method="inclusive") if len(values) > 1 else [values[0]] * 9
    return f"p50 {q[4]:.0f} s, p90 {q[8]:.0f} s, max {max(values):.0f} s"


def delay_summary(delays: list[float]) -> str:
    if len(delays) < 2:
        return "delay n/a"
    q = statistics.quantiles(delays, n=100, method="inclusive")
    shares = ", ".join(f"<={label} {sum(d <= s for d in delays) / len(delays):.0%}" for s, label in DELAY_MARKS)
    return f"delay p50 {q[49] / 60:.1f} min, p90 {q[89] / 3600:.1f} h | {shares}"


def main(argv: list[str]) -> int:
    data_dir = resolve_data_dir(argv)
    req = read_jsonl(sorted((data_dir / "logs").glob("requests-*.jsonl")))
    recs = read_jsonl(sorted((data_dir / "events").glob("*.jsonl")))
    if not req:
        print(f"no request log in {data_dir / 'logs'}; pass DATA_DIR as an argument or set it in .env")
        return 1
    steady, spans = steady_state(req)
    runs = merge([s for track_spans in spans.values() for s in track_spans])
    span_min = (parse_ts(req[-1]["ts"]) - parse_ts(req[0]["ts"])) / 60
    print(f"data {data_dir}: {req[0]['ts']} -> {req[-1]['ts']}")
    print(f"runs {len(runs)}, poller running {minutes(runs):.1f} of {span_min:.1f} min")

    # Requests: one log line per poll; status is page 1's, a failed later page shows in "error".
    print(f"\npolls {len(req)}, page requests {sum(r['requests'] for r in req)}")
    for track in TRACKS:
        statuses = Counter(r["status"] for r in req if r["track"] == track)
        if statuses:
            print(f"  {track:9} " + " ".join(f"{k}={v}" for k, v in sorted(statuses.items(), key=lambda kv: str(kv[0]))))
    seen_endpoints: set[str] = set()
    repeat, after_restart = [], []
    for r in req:
        if r["endpoint"] in seen_endpoints and r["track"] == "watchlist":
            repeat.append(r)
            if (r["endpoint"], r["ts"]) not in steady:
                after_restart.append(r)
        seen_endpoints.add(r["endpoint"])
    if repeat:
        print(f"  watch-list 304 share after each timeline's first poll: {sum(r['status'] == 304 for r in repeat) / len(repeat):.0%} of {len(repeat)}")
    if after_restart:
        print(f"  watch-list first polls after a restart: 304 in {sum(r['status'] == 304 for r in after_restart)} of {len(after_restart)}")
    # Tiers (v1): the share of 200s decides the tier-2 interval; late_s shows whether the scheduler keeps up.
    for tier in sorted({r["tier"] for r in req if r.get("tier") is not None}):
        rows = [r for r in req if r.get("tier") == tier]
        paid = sum(r["status"] == 200 for r in rows) / len(rows)
        late = [r["late_s"] for r in rows if r.get("late_s") is not None]
        print(f"  tier {tier}: polls {len(rows)}, timelines {len({r['endpoint'] for r in rows})}, "
              f"200 {paid:.0%}, started late {seconds_summary(late)}")
    limited = [r for r in req if rate_limited(r)]
    errors = Counter(r["error"] for r in req if r.get("error"))
    print(f"  rate-limited (403/429): {len(limited)}, errors: {dict(errors) if errors else 0}")

    # Quota: does a 304 cost anything? Compare the fall in X-RateLimit-Remaining with paid pages.
    rated = [r for r in req if r.get("rate_remaining") is not None]
    drop = paid = free = 0
    for prev, cur in zip(rated, rated[1:]):
        if cur["rate_remaining"] > prev["rate_remaining"]:  # hourly reset in between
            continue
        drop += prev["rate_remaining"] - cur["rate_remaining"]
        if cur["status"] == 304:
            free += 1
        else:
            paid += cur["requests"]
    if rated:
        verdict = "304 not charged" if abs(drop - paid) <= abs(drop - paid - free) else "304 CHARGED"
        print(f"  rate_remaining {rated[0]['rate_remaining']} -> {rated[-1]['rate_remaining']}: fell by {drop}; "
              f"non-304 page requests {paid}, 304 polls {free} ({verdict})")

    # Global feed: a poll that saw no event written before did not reach back to the previous poll.
    gl = [r for r in req if r["track"] == "global" and (r["endpoint"], r["ts"]) in steady]
    if gl and all("known" in r for r in gl):
        reached = sum(r["known"] > 0 for r in gl)
        dup = statistics.median(r["dup_in_poll"] for r in gl)
        print(f"  global polls that reached the previous poll: {reached} of {len(gl)}; page duplicates per poll: median {dup:g}")
    elif gl:
        print("  global polls that reached the previous poll: n/a (log written before the 'known' field existed)")

    ids = Counter(str(r["event"].get("id")) for r in recs)
    dups = {i: n for i, n in ids.items() if n > 1}
    print(f"\nevents {len(recs)}, unique ids {len(ids)}, duplicated ids {len(dups)}")
    with_packages = [r for r in recs if "packages" in r]  # records from v1 on
    if with_packages:
        parts = []
        for track in TRACKS:
            rows = [r for r in with_packages if r["track"] == track]
            if rows:
                n = sum(bool(r["packages"]) for r in rows)
                parts.append(f"{track} {n} of {len(rows)} ({n / len(rows):.0%})")
        print("  events in CRAN package repos: " + ", ".join(parts))
    for track in TRACKS:
        rows = [r for r in recs if r["track"] == track and (r["endpoint"], r["fetched_at"]) in steady]
        run_min = minutes(merge(spans.get(track, [])))
        if not rows or run_min <= 0:
            continue
        # PublicEvent carries the repository's creation time in created_at, so it says nothing about delay.
        delays = [
            parse_ts(r["fetched_at"]) - parse_ts(r["event"]["created_at"])
            for r in rows
            if r["event"].get("created_at") and r["event"].get("type") != "PublicEvent"
        ]
        types = Counter(r["event"].get("type") for r in rows).most_common(4)
        print(f"  {track:9} {len(rows)} in steady state ({len(rows) / run_min:.1f}/min), {delay_summary(delays)}")
        print("            " + ", ".join(f"{t} {n}" for t, n in types))
    if dups:
        print("  duplicated ids (first 5):", list(dups.items())[:5])
    return 1 if limited or dups else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

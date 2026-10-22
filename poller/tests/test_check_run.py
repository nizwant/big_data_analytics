import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_run.py"
_spec = importlib.util.spec_from_file_location("check_run", SCRIPT)
check_run = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_run)

T0 = 1_792_620_000.0
EVERY_S = 64.0


def iso(t: float) -> str:
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def write_runs(data_dir: Path, starts: list[float], polls: int = 10, per_poll: int = 4) -> None:
    """Global-feed polls every EVERY_S seconds in runs starting at `starts`; each writes `per_poll` events."""
    (data_dir / "logs").mkdir(parents=True)
    (data_dir / "events").mkdir(parents=True)
    req, events = [], []
    for start in starts:
        for i in range(polls):
            t = start + i * EVERY_S
            req.append({"ts": iso(t), "endpoint": "/events", "track": "global", "status": 200, "requests": 3,
                        "events": 300, "dup_in_poll": 0, "known": 1, "new_events": per_poll,
                        "rate_remaining": 4000, "next_poll_s": 60.0, "error": None})
            for _ in range(per_poll):
                events.append({"fetched_at": iso(t), "track": "global", "endpoint": "/events",
                               "event": {"id": str(len(events)), "type": "PushEvent", "created_at": iso(t - 300)}})
    (data_dir / "logs" / "requests-2026-10-21.jsonl").write_text("".join(json.dumps(r) + "\n" for r in req), encoding="utf-8")
    (data_dir / "events" / "2026-10-21.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")


def test_running_time_leaves_out_the_break_between_runs(tmp_path):
    write_runs(tmp_path, starts=[T0, T0 + 9 * EVERY_S + 1200])
    req = check_run.read_requests(sorted((tmp_path / "logs").glob("*.jsonl")))
    steady, spans = check_run.steady_state(req)
    runs = check_run.merge(spans["global"])
    assert len(runs) == 2
    assert check_run.minutes(runs) == 2 * 9 * EVERY_S / 60
    assert len(steady) == 18  # the first poll of each run is not steady state


def test_exit_code_flags_a_duplicated_event_id(tmp_path):
    write_runs(tmp_path, starts=[T0])
    assert check_run.main(["check_run.py", str(tmp_path)]) == 0
    dup = {"fetched_at": iso(T0), "track": "global", "endpoint": "/events",
           "event": {"id": "0", "type": "PushEvent", "created_at": iso(T0)}}
    with open(tmp_path / "events" / "2026-10-21.jsonl", "a", encoding="utf-8") as fh:
        fh.write(json.dumps(dup) + "\n")
    assert check_run.main(["check_run.py", str(tmp_path)]) == 1


def test_data_dir_comes_from_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "collected"
    (tmp_path / ".env").write_text(f"GITHUB_TOKEN=x\nDATA_DIR={target}  # outside OneDrive\n", encoding="utf-8")
    assert check_run.resolve_data_dir(["check_run.py"]) == target


def append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as fh:
        fh.writelines(json.dumps(r) + "\n" for r in rows)


def run_main(data_dir: Path, capsys) -> tuple[int, str]:
    code = check_run.main(["check_run.py", str(data_dir)])
    return code, capsys.readouterr().out


@pytest.mark.parametrize("status, error", [(403, "HTTP 403"), (429, "HTTP 429"), (200, "HTTP 429")],
                         ids=["403", "429", "429-on-later-page"])
def test_exit_code_flags_a_rate_limited_request(tmp_path, capsys, status, error):
    write_runs(tmp_path, starts=[T0])
    append_jsonl(tmp_path / "logs" / "requests-2026-10-21.jsonl", [
        {"ts": iso(T0 + 10 * EVERY_S), "endpoint": "/orgs/r-lib/events", "track": "watchlist", "status": status,
         "requests": 1, "events": 0, "new_events": 0, "rate_remaining": 0, "next_poll_s": 60.0, "error": error}])
    code, out = run_main(tmp_path, capsys)
    assert code == 1
    assert "rate-limited (403/429): 1" in out


@pytest.mark.parametrize("charged", [False, True])
def test_quota_verdict_on_304(tmp_path, capsys, charged):
    # Watch-list polls alternate 200 and 304; the limit falls by 1 per 200, and per 304 only if charged.
    remaining, rows = 4000, []
    for i in range(10):
        status = 200 if i % 2 == 0 else 304
        if status == 200 or charged:
            remaining -= 1
        rows.append({"ts": iso(T0 + i * 130), "endpoint": "/orgs/r-lib/events", "track": "watchlist",
                     "status": status, "requests": 1, "events": 0, "new_events": 0,
                     "rate_remaining": remaining, "next_poll_s": 120.0, "error": None})
    append_jsonl(tmp_path / "logs" / "requests-2026-10-21.jsonl", rows)
    code, out = run_main(tmp_path, capsys)
    assert code == 0
    assert ("304 CHARGED" if charged else "304 not charged") in out


def test_public_event_is_left_out_of_delays(tmp_path, capsys):
    # PublicEvent.created_at is the repository's creation date; 4-year "delays" must not count.
    write_runs(tmp_path, starts=[T0])  # 4 PushEvents per poll, created 300 s before the fetch
    public = []
    for i in range(10):
        t = T0 + i * EVERY_S
        public += [{"fetched_at": iso(t), "track": "global", "endpoint": "/events",
                    "event": {"id": f"p{i}-{k}", "type": "PublicEvent", "created_at": iso(t - 4 * 365 * 86400)}}
                   for k in range(4)]
    append_jsonl(tmp_path / "events" / "2026-10-21.jsonl", public)
    code, out = run_main(tmp_path, capsys)
    assert code == 0
    assert "delay p50 5.0 min, p90 0.1 h" in out
    assert "PublicEvent 36" in out  # still counted among event types


def test_plain_403_on_one_timeline_is_an_error_not_a_rate_limit(tmp_path, capsys):
    write_runs(tmp_path, starts=[T0])
    append_jsonl(tmp_path / "logs" / "requests-2026-10-21.jsonl", [
        {"ts": iso(T0 + 10 * EVERY_S), "endpoint": "/repos/blocked/x/events", "track": "watchlist", "tier": 2,
         "status": 403, "requests": 1, "events": 0, "new_events": 0, "rate_remaining": 3000, "rate_limited": False,
         "next_poll_s": 3600.0, "late_s": 0.0, "error": "HTTP 403"}])
    code, out = run_main(tmp_path, capsys)
    assert code == 0
    assert "rate-limited (403/429): 0" in out and "'HTTP 403': 1" in out


def test_tier_lines_show_paid_share_and_lateness(tmp_path, capsys):
    rows = [{"ts": iso(T0 + i), "endpoint": f"/repos/o/r{i}/events", "track": "watchlist", "tier": 2,
             "status": 200 if i == 0 else 304, "requests": 1, "events": 0, "new_events": 0, "rate_remaining": 4000,
             "rate_limited": False, "next_poll_s": 3600.0, "late_s": float(10 * i), "error": None} for i in range(4)]
    append_jsonl(tmp_path / "logs" / "requests-2026-10-21.jsonl", rows)
    code, out = run_main(tmp_path, capsys)
    assert code == 0
    assert "tier 2: polls 4, timelines 4, 200 25%, started late p50 15 s, p90 27 s, max 30 s" in out


def test_share_of_events_in_cran_package_repos(tmp_path, capsys):
    write_runs(tmp_path, starts=[T0])  # 40 global events without the packages field (v0 records)
    append_jsonl(tmp_path / "events" / "2026-10-21.jsonl", [
        {"fetched_at": iso(T0), "track": "watchlist", "endpoint": "/orgs/r-lib/events", "packages": ["cli"] if i < 3 else [],
         "event": {"id": f"w{i}", "type": "PushEvent", "created_at": iso(T0 - 60)}} for i in range(4)])
    code, out = run_main(tmp_path, capsys)
    assert code == 0
    assert "events in CRAN package repos: watchlist 3 of 4 (75%)" in out


def test_events_are_read_without_their_payload(tmp_path):
    path = tmp_path / "events.jsonl"
    append_jsonl(path, [{"fetched_at": iso(T0), "track": "watchlist", "endpoint": "/orgs/r-lib/events", "packages": ["cli"],
                         "event": {"id": "1", "type": "PushEvent", "created_at": iso(T0 - 60), "payload": {"big": "x" * 1000}}}])
    (ev,) = check_run.read_events([path])
    assert ev == check_run.Ev("1", "PushEvent", iso(T0 - 60), "watchlist", "/orgs/r-lib/events", iso(T0), True)

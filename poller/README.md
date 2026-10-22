# GitHub Events poller (stream B)

Polls the GitHub Events API and writes each new public event once to daily JSONL files (Kafka in v2).

| Track | Source | Purpose |
|---|---|---|
| `global` | `GET /events`, pages 1–3, at most every 60 s | Stream volume (> 1 event / 30 s) |
| `watchlist` | timelines in `watchlist.csv`, built from `crantogit.csv` | Repositories behind CRAN packages |

## Run

Python 3.10+, from `poller/`:

```bash
python -m venv .venv              # activate: .venv\Scripts\activate (Windows), source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env              # then set GITHUB_TOKEN
python -m gh_poller.watchlist     # build DATA_DIR/watchlist.csv (about 2,800 API lookups the first time)
python -m gh_poller               # Ctrl+C to stop
python -m pytest                  # mocked HTTP; tests marked live call GitHub when GITHUB_TOKEN is set
python scripts/check_run.py       # run report: statuses, rate limits, duplicates, volume, delays
```

For development, `WATCHLIST_PATH=./watchlist.dev.csv` polls only the 12 tier-1 organisations.
Offline, skip the live tests with `python -m pytest -m "not live"`.

## Configuration

Environment variables or `.env` (all settings: `.env.example`).

| Variable | Default | Meaning |
|---|---|---|
| `GITHUB_TOKEN` | required | Fine-grained PAT, public repositories read-only |
| `DATA_DIR` | `./data` | Output, state and caches; keep it outside OneDrive/Dropbox |
| `WATCHLIST_PATH` | `DATA_DIR/watchlist.csv` | Watch-list, reloaded when the file changes |
| `TIER1_INTERVAL_S` | `120` | Tier-1 poll interval |
| `TIER2_INTERVAL_S` | automatic | Tier-2 poll interval (≥ 900); automatic: tier-2 timelines / `TIER2_BUDGET_PER_H`, in whole hours |
| `TIER2_BUDGET_PER_H` | `3500` | Cap on paid tier-2 page requests per hour (304s are free) |
| `ORG_MIN_R_SHARE` | `0.5` | Poll an organisation's timeline only if this share of its public repos are R packages |

## Watch-list

`python -m gh_poller.watchlist` turns `crantogit.csv` into `watchlist.csv` (`package, owner, repo, endpoint, tier`):

- organisation with mostly R repos → `/orgs/{owner}/events`;
- any other owner → `/repos/{owner}/{repo}/events` per repo;
- tier 1: the `cran` mirror and large R organisations; tier 2: the rest.

Account types are cached in `DATA_DIR/account_types.csv`, so an interrupted build resumes. Whatever
refreshes `watchlist.csv` (e.g. Airflow) should write a temporary file and rename it.

## Output

```
DATA_DIR/
  events/YYYY-MM-DD.jsonl          one line per new event
  logs/requests-YYYY-MM-DD.jsonl   one line per poll
  state.sqlite3                    ETags, poll schedule, ids of written events
```

```json
{"fetched_at": "2026-10-21T21:22:53Z", "track": "watchlist", "endpoint": "/orgs/tidyverse/events",
 "packages": ["dplyr"],
 "event": {"id": "…", "type": "ReleaseEvent", "repo": {"name": "tidyverse/dplyr"}, "created_at": "…", …}}
```

- `event` is the raw GitHub event; `packages` lists the CRAN packages in `event.repo.name`.
- The format is defined only in `gh_poller/sink.py:make_record`.
- Delivery is at least once: deduplicate on `event.id`.
- `PublicEvent.created_at` is the repository's creation date; leave it out of delay measurements.

## Reliability

- **Conditional requests:** an unchanged timeline answers `304`, which does not use the 5,000 requests/h quota.
- **Pacing:** at most 10 polls at once, 300 requests/min, tier 1 before tier 2, `X-Poll-Interval` respected.
- **Quota:** tier 2 never exceeds `TIER2_BUDGET_PER_H` paid page requests per hour, also after a restart,
  sleep or quota reset. Below 1,000 remaining, tier-2 intervals double; below 200, tier 2 waits for the reset.
  A rate limit (403/429) pauses all polling until the reset or `Retry-After`.
- **Restarts:** ETags, schedule and written event ids are kept in SQLite.
- **Errors:** `5xx` and network errors are retried (2, 4, 8 s); other errors back off the timeline for
  5 min (on the global feed `404`/`410` too); `404`/`410` on the watch-list is rechecked after 7 days;
  `401` stops the poller.

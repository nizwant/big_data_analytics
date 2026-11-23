# GitHub Events poller (stream B)

Polls the GitHub Events API and sends each new public event once to the Kafka topic `github_repo_updates`
and/or to daily JSONL files.

| Track | Source | Purpose |
|---|---|---|
| `global` | `GET /events`, pages 1–3, at most every 60 s; off by default (`GLOBAL_TRACK_ENABLED=true` to enable) | Stream volume (> 1 event / 30 s) |
| `watchlist` | timelines in `watchlist.csv`, built from `crantogit.csv` | Repositories behind CRAN packages |

## Run

Runs as the Compose service `github-poller` (profile `github`); start, watch-list build and stage/prod
paths are in the main README. It needs `poller/.env` with `GITHUB_TOKEN` (copy `.env.example`).

```bash
python -m gh_poller.watchlist     # build DATA_DIR/watchlist.csv
python -m gh_poller               # the poller (the container's default command)
python scripts/check_run.py       # run report: statuses, rate limits, duplicates, volume, delays (needs EVENTS_JSONL)
```

In the container, prefix with `docker compose --env-file deploy/<env>.env --profile github run --rm github-poller`.

Tests (mocked HTTP; tests marked `live` call GitHub when `GITHUB_TOKEN` is set):

```bash
pip install -r requirements-dev.txt
python -m pytest -m "not live"
```

## Configuration

Environment variables or `.env` (all settings: `.env.example`).

| Variable | Default | Meaning |
|---|---|---|
| `GITHUB_TOKEN` | required | Fine-grained PAT, public repositories read-only |
| `DATA_DIR` | `./data` | Output, state and caches (in Compose: `/data`, mounted from `POLLER_DATA_DIR`) |
| `WATCHLIST_PATH` | `DATA_DIR/watchlist.csv` | Watch-list, reloaded when the file changes |
| `TIER1_INTERVAL_S` | `120` | Tier-1 poll interval |
| `TIER2_INTERVAL_S` | automatic | Tier-2 poll interval (≥ 900); automatic: tier-2 timelines / `TIER2_BUDGET_PER_H`, in whole hours |
| `TIER2_BUDGET_PER_H` | `3500` | Cap on paid tier-2 page requests per hour (304s are free) |
| `CRANTOGIT_URL` | r-universe `crantogit.csv` on GitHub | Source of the package → repo mapping for the watch-list build (URL or file path) |
| `GLOBAL_TRACK_ENABLED` | `false` | Also poll the global feed `GET /events` (`track: global`) |
| `WATCHLIST_MAX_OWNERS` | `0` (all) | Watch-list build: keep the tier-1 organisations plus this many other owners (those with the most repos); in Compose: `POLLER_WATCHLIST_MAX_OWNERS` (default 100, 0 in `stage.env`/`prod.env`) |
| `ORG_MIN_R_SHARE` | `0.5` | Poll an organisation's timeline only if this share of its public repos are R packages |
| `KAFKA_BOOTSTRAP_SERVERS` | empty | Kafka brokers; empty: no Kafka output (in Compose: `kafka:19092`) |
| `KAFKA_TOPIC` | `github_repo_updates` | Topic for the events (created by `deploy/kafka_init.sh`) |
| `WATCHLIST_TOPIC` | `github_repo_list` | Topic for the watch-list rows, sent by every watch-list build when Kafka is set |
| `EVENTS_JSONL` | `true` | Also write the events to `DATA_DIR/events/` (in Compose: `POLLER_EVENTS_JSONL`, default `false`) |

## Watch-list

`python -m gh_poller.watchlist` turns `crantogit.csv` into `watchlist.csv` (`package, owner, repo, endpoint, tier`):

- organisation with mostly R repos → `/orgs/{owner}/events`;
- any other owner → `/repos/{owner}/{repo}/events` per repo;
- tier 1: the `cran` mirror and a hand-picked list of R organisations (`TIER1_ORGS`, no formal rule); tier 2: the rest.

Account types are cached in `DATA_DIR/account_types.csv`, so an interrupted build resumes. Whatever
refreshes `watchlist.csv` should write a temporary file and rename it.

With `KAFKA_BOOTSTRAP_SERVERS` set, the build also sends every row to `github_repo_list`, one JSON message
per row, key `package` (the endpoint for tier-1 organisations without a package):
`{"built_at", "package", "owner", "repo", "endpoint", "tier"}`; `built_at` is the same for one build.
If Kafka does not confirm, the build exits with 1 (the file is already written).

## Output

Kafka: topic `github_repo_updates`, key `event.repo.name` (events of one repository stay in order),
value the JSON record below. A message is confirmed (`acks=all`, idempotent producer) before its
event counts as written; events larger than the broker accepts (Kafka default 1 MB) are logged and dropped.

```
DATA_DIR/
  events/YYYY-MM-DD.jsonl          one line per new event (EVENTS_JSONL)
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
- `src/pyflink/github_events_ingest.py` (Flink) deduplicates the events and writes them, validated and with a schema,
  to `github_repo_updates_clean` (Avro, `schemas/github_repo_updates_clean.avsc`) and to HDFS.
- `event.created_at` is GitHub's own time of the event (use it as event time); `fetched_at` is when the poller
  received it.
- `PublicEvent` is GitHub's "repository made public" event, not a general label for public events. Its `created_at` is the repository's creation date, so leave it out of delay measurements; the Flink job rejects it as stale when that date is older than `MAX_AGE_DAYS`.

## Reliability

- **Conditional requests:** an unchanged timeline answers `304`, which does not use the 5,000 requests/h quota.
- **Fewer requests:** one organisation timeline covers all its R repos instead of one request per repo; used only when
  R repos are at least `ORG_MIN_R_SHARE` of its public repos, since a timeline holds at most 300 events and
  other activity would push R events out.
- **Pacing:** at most 10 polls at once, 300 requests/min, tier 1 before tier 2, `X-Poll-Interval` respected.
- **Quota:** tier 2 never exceeds `TIER2_BUDGET_PER_H` paid page requests per hour, also after a restart
  or quota reset. Below 1,000 remaining, tier-2 intervals double; below 200, tier 2 waits for the reset.
  A rate limit (403/429) pauses all polling until the reset or `Retry-After`.
- **Restarts:** ETags, schedule and written event ids are kept in SQLite.
- **Errors:** `5xx` and network errors are retried (2, 4, 8 s); other errors back off the timeline for
  5 min (on the global feed `404`/`410` too); `404`/`410` on the watch-list is rechecked after 7 days;
  `401` stops the poller.
- **Output failures:** if Kafka (or the JSONL file) does not confirm the events, nothing is committed and all
  polling pauses for 60 s; the next poll of that timeline fetches the same events again.
- **Stop:** `SIGTERM` (`docker stop`) stops like Ctrl+C.

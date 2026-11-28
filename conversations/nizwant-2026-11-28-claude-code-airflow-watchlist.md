# 2026-11-28 — Airflow builds the GitHub watch-list, the poller reads it from Kafka

Conversation with Claude Code. User messages are verbatim; assistant replies are condensed. The assistant's clarifying questions are listed with the options offered and the answer chosen. Messages that only checked on progress are left out.

---

**User:** recently I added airflow, now I want to use it to run github puller according to architecture, read latest deliverable to understand how it should work, is it possible to do with current status of puller? grill me

**Assistant:** Not as it stands, but about half of it was already there.

In Milestone 2, Airflow does not run the poller. The poller is a long-running streaming source, and a daily Airflow DAG builds the watch-list and publishes it to `github_repo_list`. The report marks the poller reading the list from Kafka as planned (`deliverables/Milestone_2.tex:463`, `:553`, `:606-607`).

Already in place:
- `python -m gh_poller.watchlist` is a separate batch step that publishes every row with a `built_at`.
- Flink copies that topic to HDFS.
- The running Airflow container's Python 3.12, httpx and confluent-kafka match what the build needs.

What was missing:
1. Nothing read `github_repo_list`. The poller only reloaded `watchlist.csv` from its own volume, which Airflow cannot reach (and, under the distributed-simulation rule, should not).
2. A complete list could not be told from a partial one. The rows are spread over 3 partitions with no row count, so a failed publish left half a list carrying the newest `built_at`. The topic also kept data for only 7 days.
3. Airflow had no way to run the code: its build context `src/airflow` cannot see `poller/`, and it has no Docker socket.
4. The account-type cache was a local file. With an empty cache, today's `crantogit.csv` costs 2,841 `GET /users` calls on prod, on the poller's 5,000/h quota, and the build stops below 1,000 remaining.
5. The poller exited when there was no watch-list, so Docker restarted it in a loop.

### Questions and answers

1. **What should Airflow run?** Options:
   - *Daily watch-list build* (recommended; matches the report, the poller stays a streaming service);
   - *the poller itself, scheduled* (~720 runs a day, a redesign of its state, contradicts the Lambda diagram);
   - *build + watch the poller* (also a task that fails if the poller stops producing).

   **Answer:** daily watch-list build.
2. **How should Airflow execute the build?** Options:
   - *gh_poller in the Airflow image* (recommended; no socket, and task logs go to HDFS);
   - *DockerOperator with the poller image* (Docker socket, which gives root on the host, works on one host only, and hits the AppArmor stop bug);
   - *Airflow lands the CSV only*, and the poller rebuilds itself.

   **Answer:** gh_poller in the Airflow image.
3. **How should the poller receive a new list?** Options:
   - *read `github_repo_list`* (recommended; needs a completeness marker and longer retention);
   - *`watchlist.csv` in HDFS* over WebHDFS;
   - *shared Docker volume* (breaks the distributed-simulation rule).

   **Answer:** read `github_repo_list`.
4. **Where should the account-type cache live?** Options:
   - *HDFS file* (recommended);
   - *Cassandra table*;
   - *compacted Kafka topic*;
   - *no cache, second token*.

   **Answer:** HDFS file.
5. **Which GitHub token, and how does it reach Airflow?** Options:
   - *same PAT as `AIRFLOW_VAR_GITHUB_TOKEN`* from `poller/.env` (recommended; an env-backed Variable, never stored in the DB);
   - *second PAT* (separate quota, a second secret);
   - *same PAT as a DB Variable*.

   **Answer:** same PAT via `AIRFLOW_VAR_`.
6. **Land the raw `crantogit.csv` in HDFS first?** Options:
   - *yes, `/data/raw/crantogit/dt=…`* (recommended; retries see the same input, and the mapping is kept as of each day);
   - *no, read the URL*.

   **Answer:** yes.
7. **What should the poller do while there is no complete watch-list?** Options:
   - *stay up and wait* (recommended; logs every minute);
   - *exit and let Docker restart it* (the old behaviour);
   - *fall back to a local copy* (local state again).

   **Answer:** stay up and wait.
8. **Refuse to publish a list that shrank sharply?** Options:
   - *guard + force param* (recommended; fail if rows drop more than 10%, a `force=true` run param overrides);
   - *guard, no override*;
   - *no guard*.

   **Answer:** guard + force param.

Defaults the assistant set without objection:
- a `rows_in_snapshot` field on every message;
- 30-day retention on `github_repo_list`;
- daily at 04:00 UTC, no catchup, one active run, 3 retries an hour apart;
- `WATCHLIST_MAX_OWNERS` moves to the airflow service;
- `deploy/start_github.sh` triggers the DAG.

---

**User:** yes, implement it

**Assistant:** Implemented:

- **Poller**
  - New `gh_poller/snapshots.py`. It reads every partition of `github_repo_list` from the beginning, groups rows by `built_at`, and returns the newest build whose distinct rows reach `rows_in_snapshot`. It answers only once every partition has reached its end, so a half-read topic never wins. Offsets are never committed.
  - The runner uses it when Kafka is configured (`WATCHLIST_SOURCE`, default `kafka` when `KAFKA_BOOTSTRAP_SERVERS` is set). It waits up to 30 s for the first read, then checks every 60 s. With no list it logs and waits instead of exiting. The file source stays for tests and runs without Kafka.
  - The build adds `rows_in_snapshot` to every row. It compares against the previous snapshot and raises `WatchlistShrank` on a drop over 10%, unless `--force` is given.
- **Airflow**
  - The image copies `gh_poller` from a second build context (`additional_contexts`); no new pip packages.
  - The new DAG `src/airflow/dags/github_watchlist.py` has two tasks:
    - `land_crantogit` writes `/data/raw/crantogit/dt=…/crantogit.csv` (temporary file, then rename) and reuses it on retries;
    - `build_watchlist` takes the token from the env-backed Variable, reads the CSV and the account cache from HDFS, and runs the poller's `build()` against the previous snapshot. It writes the cache back even when the build stops early. The run param `force` overrides the shrink check.
- **Deploy**
  - `kafka_init.sh` sets 30-day retention on `github_repo_list`.
  - `hdfs_init.sh` adds `/data/raw/crantogit` and `/data/ref/github_account_types`.
  - `start_github.sh` checks the token, unpauses and triggers the DAG, then starts the poller and its Flink job.
  - README, `poller/README.md`, `.env.example` and e2e FA-05 were updated. FA-05 now checks the newest DAG run, the raw CSV and a complete snapshot, and that the poller applied it.
- **Bug found and fixed.** The bare `GITHUB_TOKEN:` in the poller's compose `environment` overrode the `env_file` value with an empty one whenever the shell had no token, so the documented `poller/.env` route never worked. Both services now get `GITHUB_TOKEN_FROM_SHELL` and fall back to the file inside the container. All four combinations were verified with dummy values.

Verified without a token:
- 122 unit tests passed.
- The DAG imports cleanly.
- `land_crantogit` wrote the real file to HDFS.
- A tokenless build fails with a clear message.
- A temporary topic, published from the Airflow image, was read back by the poller container, which applied the newest of two snapshots.

---

**User:** I added pat token, and now I want to test if all data gets to kafka and then to hdfs, I want to know what  has been genrerated in hdfs after this test

**Assistant:** Took a baseline first: every GitHub folder in HDFS and all three GitHub topics were empty. Then:
- recreated `airflow` to pick up the token, using the in-container stop because of the host's AppArmor bug;
- ran `deploy/start_github.sh`;
- let the poller run for 7 minutes.

Results:
- **DAG:** unpausing started a run for today's 04:00 slot, and the script's trigger queued a second one. Both succeeded.
  - First run: 2,630 rows, 100 account lookups, cache written to HDFS.
  - Second run: all 100 from cache, shrink check +0.
- **Poller:** applied the newer snapshot (1,907 timelines: 12 tier 1, 1,895 tier 2) and made 787 requests with no errors.
- **Kafka:** `github_repo_list` 5,260 (2 × 2,630), `github_repo_updates` 5,098, `github_repo_updates_clean` 3,017.
- **HDFS**, written by the Flink job unless noted:
  - `/data/raw/github_repo_list`: 5,260 JSON lines, both builds.
  - `/data/raw/github_repo_updates`: about 5,050 raw events, unique ids, by arrival hour.
  - `/data/clean/github_repo_updates`: 3,014 Parquet rows in 394 files over 157 event-time hours (2026-11-21 to 11-28), 3 more in open files.
  - `/data/raw/github_repo_updates_bad`: 2,081 rows, all `stale_timestamp`.
  - `/data/ref/github_account_types/account_types.csv`: 100 owners, written by the DAG.
  - `/airflow/logs/dag_id=github_watchlist`: 4 task logs, written by Airflow.
- **Reconciliation:** 5,098 raw = 3,017 clean + 2,081 rejected, so nothing was lost or duplicated. Flink completed 13 checkpoints with no failures.

Notes:
- The 41% stale share comes from the poller's first pass over up to 90 days of history; the job keeps 7.
- The clean folder is many small files (one per active event hour per 60 s checkpoint), so it is a compaction candidate for the batch layer.
- The double DAG run is harmless but redundant.

---

**User:** so is everything working, if yes then create a conversation with q and answers and prepare me a pr description, make sure to succinctly describle the test we did here

**Assistant:** Yes. Rechecked the live stack:
- the poller is healthy and still has no errors;
- the Flink job is running with 22 checkpoints and no failures;
- 127 unit tests pass, including the 5 live GitHub tests now that a token is set.

Wrote this entry and drafted the PR description in the repo's template (Summary, Changes, Usage, Notes, Testing).

Open:
- e2e FA-05 has not been run;
- the "planned" markers in `Milestone_2.tex` are unchanged;
- the double run from `start_github.sh`.

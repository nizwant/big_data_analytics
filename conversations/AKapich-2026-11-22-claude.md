# 2026-11-22 — GitHub poller: Kafka in Docker, PR

Conversation with Claude, except that local paths are replaced with `<repo>` and `<data-dir>`; replies are condensed. Context-compaction commands are left out.

---

**User:** Kontynuuję GitHub poller

Stan w docs projektu: claude/poller-integration.md.

Branch feature/github-poller = aktualny dev +  commity pollera.

Repo lokalnie: <repo>.

Do zrobienia:
Lokalny test: kafka + github-poller w Dockerze, zdarzenia w github_repo_updates.
++ PR do dev według .github/pull_request_template.md.


**Assistant:** Checked the branch: `origin/dev` plus three linear poller commits; `origin/feature/github-poller` still held the older 2-commit version branched from `main`, so the push needs `--force-with-lease`. 103 offline tests pass. Docker and SSH are not reachable from Claude's sandbox, so prepared scripts for the user to run, kept in the gitignored `poller/notes/`:

- `compose_test.ps1` + `kafka_check.py`: build the image, start `kafka`, create `github_repo_updates` (3 partitions), start `github-poller` with the dev watch-list, then read the whole topic back and check duplicate `event.id`s, key = `repo.name`, tracks and event types; restart the poller and check again; run `check_run.py` and the healthcheck.
- `push_pr.ps1`: push with lease and open the PR to `dev`, with a body following the team template.
- `stage_check.ps1`: from cran-stage, check HTTPS to `api.github.com`, `raw.githubusercontent.com`, PyPI and Docker Hub, plus Docker, `/srv/bigdata/poller` and disk space.

---

**Assistant (after the rerun):** The Docker test passes.

| Check | After 3 min | After poller restart |
|---|---|---|
| Messages in `github_repo_updates` (3 partitions) | 4,198 | 4,616 |
| Duplicate ids / bad JSON / key ≠ `repo.name` | 0 / 0 / 0 | 0 / 0 / 0 |

- Restart: state survived on the volume; timelines answered `304` and no history was resent.
- 0 × 403/429, healthcheck `healthy`, global track ~251 events/min, largest message 112 kB (limit 1 MB).
- Two `check_run` figures are distorted by the earlier failed run and are not used: the watch-list delay (first polls that delivered nothing were followed by polls carrying 30 days of history) and the "304 charged" verdict (quota dropped by more than all logged requests, so the token was used elsewhere at the same time). The 24 h run will settle both.
- Added to the PR notes: if the topic does not exist, the poller commits nothing and retries every 60 s, so `deploy/init.sh` must run first.

---

**User:** jakikolwiek przeoczony caveat 

**Assistant:** Nothing blocks the PR. Not yet verified on this version: the full watch-list (tier-2 scheduling for 8–16k timelines is tested on mocks only), the 5 live tests (last run 22.10; HTTP code unchanged since), whether `304` uses quota (distorted in this run). Known limits, listed in Notes: a restart loop on `401` with `restart: unless-stopped`; a missing topic leaves the poller healthy but idle. Outside the PR: stage egress to `api.github.com` is unknown. For the team: the feature branch was force-pushed, so anyone with the old copy should reset to `origin`.

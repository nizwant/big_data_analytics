"""Daily GitHub watch-list build: crantogit.csv -> watch-list -> Kafka topic github_repo_list.

The GitHub poller (Compose service github-poller) polls the newest complete watch-list in
github_repo_list, and the Flink job flink-github-events-job copies its rows to HDFS
/data/raw/github_repo_list.

1. land_crantogit downloads crantogit.csv (CRANTOGIT_URL) unchanged to HDFS
   /data/raw/crantogit/dt=YYYY-MM-DD/crantogit.csv, dt being the UTC day the run was due. A retry,
   or another run on the same day, reuses that file, so it builds from the same input.
2. build_watchlist builds the watch-list from that file with the poller's own code
   (gh_poller.watchlist.build, also behind `python -m gh_poller.watchlist`) and publishes one
   message per row to github_repo_list. Account types (GET /users/{owner}) are cached in HDFS
   /data/ref/github_account_types/account_types.csv, read before the build and written back after
   it, also when it stops early. The build stops while fewer than 1,000 requests of the hourly
   quota are left (the poller uses the same token); the next try, an hour later, continues from
   the cache. It also fails, publishing nothing, if the list has more than 10% fewer rows than the
   newest snapshot in the topic: check the landed crantogit.csv, then trigger the DAG with
   {"force": true} to publish it anyway.

Settings come from the airflow service in docker-compose.yml, which also reads poller/.env: the
token as the Variable github_token (environment variable AIRFLOW_VAR_GITHUB_TOKEN),
WATCHLIST_MAX_OWNERS, KAFKA_BOOTSTRAP_SERVERS and the optional build settings of
poller/README.md (CRANTOGIT_URL, ORG_MIN_R_SHARE, WATCHLIST_TOPIC, ...).

deploy/start_github.sh unpauses and triggers it. By hand:
    docker compose exec airflow airflow dags trigger github_watchlist
    docker compose exec airflow airflow dags trigger github_watchlist --conf '{"force": true}'
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, Variable, dag, get_current_context, task

log = logging.getLogger(__name__)

RAW_DIR = "/data/raw/crantogit"
ACCOUNT_CACHE = "/data/ref/github_account_types/account_types.csv"


def _hdfs():
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

    return WebHDFSHook().get_conn()


def _put(client, data: bytes, path: str) -> None:
    """Write next to `path`, then rename over it, so no reader sees a half-written file."""
    client.makedirs(path.rsplit("/", 1)[0])
    client.write(path + ".tmp", data=data, overwrite=True)
    client.delete(path)  # WebHDFS cannot rename over an existing file
    client.rename(path + ".tmp", path)


def _get(client, path: str, local: Path) -> bool:
    """Download `path` (or what an interrupted _put left in path.tmp); False if neither exists."""
    for candidate in (path, path + ".tmp"):
        if client.status(candidate, strict=False):
            client.download(candidate, str(local), overwrite=True)
            return True
    return False


@dag(
    schedule="0 4 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    params={
        "force": Param(
            False,
            type="boolean",
            description="Publish even if the watch-list has more than 10% fewer rows than the newest snapshot.",
        )
    },
    tags=["github", "batch"],
    doc_md=__doc__,
)
def github_watchlist():
    @task(retries=3, retry_delay=timedelta(minutes=10), execution_timeout=timedelta(minutes=10))
    def land_crantogit() -> str:
        import httpx
        from gh_poller.config import DEFAULT_CRANTOGIT_URL

        day = get_current_context()["dag_run"].run_after.astimezone(timezone.utc).strftime("%Y-%m-%d")
        path = f"{RAW_DIR}/dt={day}/crantogit.csv"
        client = _hdfs()
        if client.status(path, strict=False):
            log.info("%s exists, building from it", path)
            return path
        source = os.environ.get("CRANTOGIT_URL", "").strip() or DEFAULT_CRANTOGIT_URL
        if source.startswith(("http://", "https://")):
            # Without the token: it is not an API request.
            resp = httpx.get(source, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            data = resp.content
        else:
            data = Path(source).read_bytes()
        _put(client, data, path)
        log.info("%s: %d bytes from %s", path, len(data), source)
        return path

    @task(retries=3, retry_delay=timedelta(hours=1), execution_timeout=timedelta(hours=1))
    def build_watchlist(crantogit_path: str) -> list[str]:
        import asyncio
        import tempfile

        from gh_poller.config import load_config
        from gh_poller.snapshots import read_latest_snapshot
        from gh_poller.watchlist import account_cache_path, build

        token = Variable.get("github_token", default=None)
        if not token:
            raise RuntimeError(
                "No GitHub token: set GITHUB_TOKEN in poller/.env (or the shell), then recreate the airflow "
                "container (docker compose up -d airflow); it reaches the DAG as the Variable github_token."
            )
        force = bool(get_current_context()["params"].get("force"))
        client = _hdfs()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            client.download(crantogit_path, str(tmp / "crantogit.csv"))
            environ = {
                **os.environ,
                "GITHUB_TOKEN": token,
                "DATA_DIR": str(tmp),
                "WATCHLIST_PATH": str(tmp / "watchlist.csv"),
                "CRANTOGIT_URL": str(tmp / "crantogit.csv"),
                "EVENTS_JSONL": "false",
            }
            try:
                cfg = load_config(env_file=None, environ=environ)
            except SystemExit as exc:  # invalid settings in poller/.env
                raise RuntimeError(str(exc)) from None
            if not cfg.kafka_bootstrap_servers:
                raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is not set: the watch-list would not be published")
            cache = account_cache_path(cfg)
            if _get(client, ACCOUNT_CACHE, cache):
                log.info("account types from %s", ACCOUNT_CACHE)
            previous = read_latest_snapshot(cfg.kafka_bootstrap_servers, cfg.watchlist_topic)
            log.info(
                "newest snapshot in %s: %s",
                cfg.watchlist_topic,
                f"built at {previous.built_at}, {len(previous.rows)} rows" if previous else "none",
            )
            try:
                lines = asyncio.run(build(cfg, previous=previous, force=force))
            finally:
                if cache.exists():  # also after an early stop, so the next try continues from here
                    _put(client, cache.read_bytes(), ACCOUNT_CACHE)
        for line in lines:
            log.info("%s", line)
        return lines

    build_watchlist(land_crantogit())


github_watchlist()

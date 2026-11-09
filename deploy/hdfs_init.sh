#!/usr/bin/env bash
# Creates the HDFS directory layout. Idempotent; safe to re-run.
# Extra arguments are passed to `docker compose`, e.g.:
#   deploy/hdfs_init.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

DIRS=(
  /data/raw/cran_mirror_logs
  /data/raw/github_repo_updates
  /data/raw/github_repo_list
)

echo "Waiting for HDFS to leave safe mode..."
docker compose "$@" exec -T namenode hdfs dfsadmin -safemode wait

docker compose "$@" exec -T namenode hdfs dfs -mkdir -p "${DIRS[@]}"
docker compose "$@" exec -T namenode hdfs dfs -ls -R /data

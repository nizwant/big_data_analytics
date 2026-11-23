#!/usr/bin/env bash
# Step 1: starts the stack structure (Kafka, registry, HDFS, Flink CRAN jobs, Cassandra, Airflow)
# and runs deploy/init.sh (topics, schemas, HDFS dirs, keyspace). Generates no data: the log
# generator and the GitHub poller are started by deploy/start_logs.sh and deploy/start_github.sh.
# Idempotent. Extra arguments are passed to `docker compose`, e.g.:
#   deploy/up.sh --env-file deploy/prod.env --env-file deploy/prod.secrets.env
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose "$@" up -d --build
deploy/init.sh "$@"

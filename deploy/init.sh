#!/usr/bin/env bash
# Runs all initialization scripts (Kafka topics, HDFS dirs, Cassandra keyspace).
# Idempotent; safe to re-run. Extra arguments are passed to `docker compose`, e.g.:
#   deploy/init.sh --env-file deploy/prod.env
set -euo pipefail

DIR="$(dirname "$0")"

"$DIR/kafka_init.sh" "$@"
"$DIR/hdfs_init.sh" "$@"
"$DIR/cassandra_init.sh" "$@"

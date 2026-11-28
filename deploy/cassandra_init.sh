#!/usr/bin/env bash
# Creates the Cassandra keyspace and the tables of the batch views. Idempotent; safe to re-run.
# Extra arguments are passed to `docker compose`, e.g.:
#   deploy/cassandra_init.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

KEYSPACE=cran_analytics
CQLSH=(docker compose "$@" exec -T cassandra cqlsh)

echo "Waiting for Cassandra..."
for _ in $(seq 60); do
  "${CQLSH[@]}" -e "describe keyspaces" >/dev/null 2>&1 && break
  sleep 5
done

"${CQLSH[@]}" -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Filled by the Spark job src/spark/jobs/package_daily_downloads.py; one partition per
# package, newest day first.
"${CQLSH[@]}" -e "CREATE TABLE IF NOT EXISTS $KEYSPACE.batch_package_daily_downloads (
  package_name text,
  day date,
  downloads bigint,
  failed_requests bigint,
  unique_ips bigint,
  source_downloads bigint,
  windows_downloads bigint,
  macos_downloads bigint,
  computed_at timestamp,
  PRIMARY KEY ((package_name), day)
) WITH CLUSTERING ORDER BY (day DESC);"

"${CQLSH[@]}" -e "describe keyspaces"
"${CQLSH[@]}" -e "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '$KEYSPACE';"

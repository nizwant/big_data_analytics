#!/usr/bin/env bash
# Creates the Cassandra keyspace. Idempotent; safe to re-run.
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
"${CQLSH[@]}" -e "describe keyspaces"

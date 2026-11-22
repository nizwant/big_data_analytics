#!/usr/bin/env bash
# Creates the Kafka topics. Idempotent; safe to re-run.
# Extra arguments are passed to `docker compose`, e.g.:
#   deploy/kafka_init.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

TOPICS=(cran_mirror_logs cran_mirror_logs_clean cran_mirror_package_downloads github_repo_updates github_repo_updates_clean github_repo_list)
PARTITIONS=3
KAFKA_TOPICS=(docker compose "$@" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092)

echo "Waiting for Kafka..."
for _ in $(seq 60); do
  # Storage of the schema registry (see docker-compose.yml). The registry requires exactly
# one partition and a compacted topic, so schemas are never expired.
"${KAFKA_TOPICS[@]}" --create --if-not-exists \
  --topic _schemas --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact

"${KAFKA_TOPICS[@]}" --list >/dev/null 2>&1 && break
  sleep 5
done

for topic in "${TOPICS[@]}"; do
  "${KAFKA_TOPICS[@]}" --create --if-not-exists \
    --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1
done
# Storage of the schema registry (see docker-compose.yml). The registry requires exactly
# one partition and a compacted topic, so schemas are never expired.
"${KAFKA_TOPICS[@]}" --create --if-not-exists \
  --topic _schemas --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact

"${KAFKA_TOPICS[@]}" --list

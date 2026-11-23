#!/usr/bin/env bash
# Wipes the dev environment: stops and removes all containers and deletes all
# data volumes (Kafka, HDFS, Cassandra, Airflow metadata and logs), including stale volumes from services
# that no longer exist in docker-compose.yml. Dev only: it uses the default
# named volumes and never touches the /srv/bigdata directories of stage/prod.
# Afterwards, start fresh with:
#   docker compose --profile dev up -d --build && deploy/init.sh
#
# Usage: deploy/dev_clean.sh [-y] [--images]
#   -y        skip the confirmation prompt
#   --images  also remove the images built by compose (bigdata/pyflink, bigdata/airflow, log generator)
set -euo pipefail

cd "$(dirname "$0")/.."

YES=0
IMAGES=0
for arg in "$@"; do
  case "$arg" in
    -y) YES=1 ;;
    --images) IMAGES=1 ;;
    *) echo "Unknown argument: $arg" >&2; exit 1 ;;
  esac
done

PROJECT=$(docker compose config --format json | python3 -c 'import sys, json; print(json.load(sys.stdin)["name"])')

if [ "$YES" -ne 1 ]; then
  echo "This deletes ALL dev containers and data (Kafka, HDFS, Cassandra, Airflow) of project '$PROJECT'."
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

DOWN_ARGS=(--profile dev down --volumes --remove-orphans)
[ "$IMAGES" -eq 1 ] && DOWN_ARGS+=(--rmi local)
docker compose "${DOWN_ARGS[@]}"

# Volumes of services that were removed from the compose file are not covered by `down -v`.
STALE=$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")
if [ -n "$STALE" ]; then
  echo "Removing leftover volumes:"
  # shellcheck disable=SC2086
  docker volume rm $STALE
fi

echo "Dev environment cleaned."

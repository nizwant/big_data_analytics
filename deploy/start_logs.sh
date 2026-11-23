#!/usr/bin/env bash
# Step 2: starts the fake mirror log generator (about 4 events/s to the topic cran_mirror_logs).
# Run deploy/up.sh first. Stop it with: docker compose --profile dev stop mirror-log-generator
# Extra arguments are passed to `docker compose`, e.g.: deploy/start_logs.sh --env-file deploy/stage.env
set -euo pipefail

cd "$(dirname "$0")/.."

docker compose "$@" --profile dev up -d --no-deps --build mirror-log-generator

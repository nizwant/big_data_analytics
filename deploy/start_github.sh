#!/usr/bin/env bash
# Step 3: builds the watch-list (also sent to github_repo_list), then starts the GitHub poller and
# its Flink job. Run deploy/up.sh first. Needs GITHUB_TOKEN in the shell (e.g. direnv) or in poller/.env.
# Stop with: docker compose --profile github stop github-poller flink-github-events-job
# Extra arguments are passed to `docker compose`, e.g.: deploy/start_github.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${GITHUB_TOKEN:-}" ]] && ! grep -qs '^GITHUB_TOKEN=.\+' poller/.env; then
  echo "GITHUB_TOKEN is not set (shell or poller/.env)." >&2
  exit 1
fi

docker compose "$@" --profile github run --rm github-poller python -m gh_poller.watchlist
docker compose "$@" --profile github up -d --no-deps --build github-poller flink-github-events-job

#!/usr/bin/env bash
# Step 3: unpauses the Airflow DAG github_watchlist and triggers a run, which builds the watch-list
# and publishes it to github_repo_list (then daily at 04:00 UTC), and starts the GitHub poller and
# its Flink job. The poller waits until the run has published the watch-list. Run deploy/up.sh first.
# Needs GITHUB_TOKEN in the shell (e.g. direnv) or in poller/.env when the airflow and github-poller
# containers are created.
# Stop with: docker compose --profile github stop github-poller flink-github-events-job
# Extra arguments are passed to `docker compose`, e.g.: deploy/start_github.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ -z "${GITHUB_TOKEN:-}" ]] && ! grep -qs '^GITHUB_TOKEN=.\+' poller/.env; then
  echo "GITHUB_TOKEN is not set (shell or poller/.env)." >&2
  exit 1
fi
if ! docker compose "$@" exec -T airflow bash -c 'test -n "${GITHUB_TOKEN_FROM_SHELL:-${GITHUB_TOKEN:-}}"'; then
  echo "The airflow container has no GitHub token (it is read when the container is created)." >&2
  echo "Recreate it: docker compose $* up -d airflow" >&2
  exit 1
fi

echo "Waiting for Airflow to load the DAG github_watchlist..."
for _ in $(seq 30); do
  docker compose "$@" exec -T airflow airflow dags details github_watchlist >/dev/null 2>&1 && break
  sleep 5
done
docker compose "$@" exec -T airflow airflow dags unpause github_watchlist
docker compose "$@" exec -T airflow airflow dags trigger github_watchlist

docker compose "$@" --profile github up -d --no-deps --build github-poller flink-github-events-job
echo "The poller starts polling when the run of github_watchlist has published the watch-list (Airflow UI, port 8080)."

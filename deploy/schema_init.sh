#!/usr/bin/env bash
# Registers the Avro schemas in schemas/*.avsc in the schema registry. The subject is
# the file name plus "-value" (schemas/cran_mirror_logs_clean.avsc becomes subject
# cran_mirror_logs_clean-value, the default for the topic cran_mirror_logs_clean).
# Idempotent: registering an unchanged schema returns the existing version, and
# an incompatible change is rejected by the registry.
# Extra arguments are passed to `docker compose`, e.g.:
#   deploy/schema_init.sh --env-file deploy/prod.env
set -euo pipefail

cd "$(dirname "$0")/.."

DC=(docker compose "$@")

# The registry image has python3 but no curl, so the requests are made with python.
REGISTRY_PY=(python3 -c '
import json, sys, urllib.request
subject = sys.argv[1] if len(sys.argv) > 1 else None
if subject:
    request = urllib.request.Request(
        f"http://localhost:8081/subjects/{subject}/versions",
        data=json.dumps({"schema": sys.stdin.read()}).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
else:
    request = "http://localhost:8081/subjects"
response = json.loads(urllib.request.urlopen(request, timeout=10).read())
print("  id=%s version=%s" % (response["id"], response["version"]) if subject else response)
')

echo "Waiting for the schema registry..."
for _ in $(seq 60); do
  "${DC[@]}" exec -T schema-registry "${REGISTRY_PY[@]}" </dev/null >/dev/null 2>&1 && break
  sleep 5
done

for file in schemas/*.avsc; do
  subject="$(basename "$file" .avsc)-value"
  echo "Registering $file as $subject"
  "${DC[@]}" exec -T schema-registry "${REGISTRY_PY[@]}" "$subject" < "$file"
done

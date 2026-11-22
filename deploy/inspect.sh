#!/usr/bin/env bash
# Prints an overview of the running stack: HDFS contents, Kafka topics and offsets,
# the Flink consumer group lag and the Flink job state. Read-only.
# Extra arguments are passed to `docker compose`, e.g.:
#   deploy/inspect.sh --env-file deploy/prod.env
set -uo pipefail

cd "$(dirname "$0")/.."

DC=(docker compose "$@")
KAFKA_BIN=/opt/kafka/bin
BOOTSTRAP=kafka:19092
GROUPS_TO_SHOW=(flink-cran-logs-ingest flink-cran-package-downloads)

section() { printf '\n=== %s ===\n' "$1"; }
kafka() { "${DC[@]}" exec -T kafka "$KAFKA_BIN/$1" --bootstrap-server "$BOOTSTRAP" "${@:2}" 2>/dev/null; }

section "HDFS: datanodes"
"${DC[@]}" exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -E "^Live datanodes|^Hostname|^DFS Used:|^Num of Blocks" || echo "(namenode not reachable)"

section "HDFS: /data (directories; finalized files per directory, in-progress hidden)"
"${DC[@]}" exec -T namenode hdfs dfs -ls -R /data 2>/dev/null | grep -v inprogress | awk '
  $1 ~ /^d/ { dirs[$NF] = 1; next }
  { d = $NF; sub("/[^/]*$", "", d); files[d]++; bytes[d] += $5 }
  END {
    for (d in dirs) {
      if (files[d]) printf "%s  [%d files, %.1f KB]\n", d, files[d], bytes[d] / 1024
      else print d
    }
  }' | sort

section "HDFS: size per directory (data size, size with replication)"
"${DC[@]}" exec -T namenode hdfs dfs -du -h /data/raw /data/clean /airflow 2>/dev/null

section "Kafka: topics"
kafka kafka-topics.sh --describe | grep -E "^Topic: [^_]" | awk '{print $2, "partitions=" $6, "replication=" $8}'

section "Kafka: messages per topic (sum of latest offsets; per-partition in brackets)"
for topic in $(kafka kafka-topics.sh --list | grep -v '^_'); do
  kafka kafka-get-offsets.sh --topic "$topic" | awk -F: -v t="$topic" '
    { total += $3; parts = parts (parts ? ", " : "") $2 ":" $3 }
    END { printf "%s  %d  [%s]\n", t, total, parts }'
done

for group in "${GROUPS_TO_SHOW[@]}"; do
  section "Kafka: consumer group $group (lag)"
  kafka kafka-consumer-groups.sh --describe --group "$group" || echo "(no committed offsets yet)"
done

section "Schema registry: subjects (latest version)"
"${DC[@]}" exec -T schema-registry python3 -c '
import json, urllib.request
base = "http://localhost:8081"
for subject in json.load(urllib.request.urlopen(base + "/subjects", timeout=10)):
    latest = json.load(urllib.request.urlopen(base + "/subjects/" + subject + "/versions/latest", timeout=10))
    print("%s  version=%s id=%s" % (subject, latest["version"], latest["id"]))
' 2>/dev/null || echo "(schema registry not reachable)"

section "Flink: jobs"
"${DC[@]}" exec -T jobmanager curl -s http://localhost:8081/jobs/overview 2>/dev/null \
  | python3 -c 'import sys, json; [print(j["state"], j["name"][:70]) for j in json.load(sys.stdin)["jobs"]]' \
  2>/dev/null || echo "(jobmanager not reachable)"

section "Airflow: components"
"${DC[@]}" exec -T airflow curl -s http://localhost:8080/api/v2/monitor/health 2>/dev/null \
  | python3 -c '
import sys, json
for name, info in json.load(sys.stdin).items():
    beat = info.get("latest_scheduler_heartbeat") or info.get("latest_triggerer_heartbeat") \
        or info.get("latest_dag_processor_heartbeat") or ""
    print(name, info.get("status"), beat)
' 2>/dev/null || echo "(airflow api-server not reachable)"

section "Airflow: DAGs and import errors"
"${DC[@]}" exec -T airflow airflow dags list --columns dag_id,is_paused 2>/dev/null \
  || echo "(airflow not reachable)"
"${DC[@]}" exec -T airflow airflow dags list-import-errors 2>/dev/null

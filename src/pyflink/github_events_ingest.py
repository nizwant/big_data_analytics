"""Ingests the GitHub poller's output (github_repo_updates, github_repo_list).

Writes:
1. raw records to HDFS /data/raw/github_repo_updates (text, by arrival time);
2. valid records, repeated event ids dropped, to the Kafka topic github_repo_updates_clean
   (Avro, schemas/github_repo_updates_clean.avsc, key repo_name) and as Parquet to
   /data/clean/github_repo_updates (by event time);
3. rejected records, with reject_reason, to /data/raw/github_repo_updates_bad (Parquet);
4. watch-list rows to /data/raw/github_repo_list (text).

The Kafka sink is at least once, so a job restart can repeat records sent since the
last checkpoint.
"""

import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "github_repo_updates")
WATCHLIST_TOPIC = os.environ.get("KAFKA_WATCHLIST_TOPIC", "github_repo_list")
CLEAN_TOPIC = os.environ.get("KAFKA_CLEAN_TOPIC", "github_repo_updates_clean")
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "flink-github-events-ingest")
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://namenode:8020/data")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = os.environ.get("CLEAN_SCHEMA_PATH", "/opt/schemas/github_repo_updates_clean.avsc")
# Parquet files are finalized on checkpoint, so this is also the file roll interval.
CHECKPOINT_INTERVAL_S = int(os.environ.get("CHECKPOINT_INTERVAL_S", "60"))
# How long seen event ids are kept for deduplication.
DEDUP_TTL_H = int(os.environ.get("DEDUP_TTL_H", "48"))

TS_FORMAT = "yyyy-MM-dd''T''HH:mm:ss''Z''"  # 2026-11-22T21:50:20Z, quotes doubled for SQL

# Must match the Avro schema, including nullability. Timestamps are UTC.
CLEAN_COLUMNS = [
    ("event_id", "STRING NOT NULL"),
    ("event_type", "STRING NOT NULL"),
    ("event_ts", "TIMESTAMP(3) NOT NULL"),
    ("fetched_ts", "TIMESTAMP(3) NOT NULL"),
    ("track", "STRING NOT NULL"),
    ("endpoint", "STRING NOT NULL"),
    ("repo_id", "BIGINT NOT NULL"),
    ("repo_name", "STRING NOT NULL"),
    ("actor_id", "BIGINT NOT NULL"),
    ("actor_login", "STRING NOT NULL"),
    ("org_login", "STRING"),
    ("packages", "ARRAY<STRING> NOT NULL"),
    ("ref", "STRING"),
    ("ref_type", "STRING"),
    ("release_tag", "STRING"),
    ("payload_json", "STRING NOT NULL"),
]
CLEAN_SELECT = ", ".join(f"`{n}`" for n, _ in CLEAN_COLUMNS)
REQUIRED = [n for n, t in CLEAN_COLUMNS if t.endswith("NOT NULL")]


def load_schema() -> str:
    """Returns the Avro schema text, after checking it against the job's columns."""
    with open(SCHEMA_PATH) as f:
        text = f.read()
    fields = {field["name"] for field in json.loads(text)["fields"]}
    job_columns = {name for name, _ in CLEAN_COLUMNS}
    if fields != job_columns:
        raise RuntimeError(
            f"Job columns and {SCHEMA_PATH} differ: "
            f"only in schema {sorted(fields - job_columns)}, "
            f"only in job {sorted(job_columns - fields)}"
        )
    return text


def parsed_view(source: str) -> str:
    """A column is NULL when absent or invalid."""
    def value(path: str) -> str:
        return f"JSON_VALUE(line, '$.{path}')"

    return f"""
        CREATE TEMPORARY VIEW parsed AS
        SELECT
            line,
            arrival_ts,
            proc_ts,
            {value('event.id')} AS event_id,
            {value('event.type')} AS event_type,
            TO_TIMESTAMP({value('event.created_at')}, '{TS_FORMAT}') AS event_ts,
            TO_TIMESTAMP({value('fetched_at')}, '{TS_FORMAT}') AS fetched_ts,
            {value('track')} AS track,
            {value('endpoint')} AS endpoint,
            CAST({value('event.repo.id')} AS BIGINT) AS repo_id,
            {value('event.repo.name')} AS repo_name,
            CAST({value('event.actor.id')} AS BIGINT) AS actor_id,
            {value('event.actor.login')} AS actor_login,
            {value('event.org.login')} AS org_login,
            JSON_QUERY(line, '$.packages' RETURNING ARRAY<STRING>) AS packages,
            {value('event.payload.ref')} AS `ref`,
            {value('event.payload.ref_type')} AS ref_type,
            {value('event.payload.release.tag_name')} AS release_tag,
            JSON_QUERY(line, '$.event.payload') AS payload_json
        FROM {source}
    """


def validated_view() -> str:
    """reject_reason is NULL for valid records; the first failing rule wins."""
    missing = " OR ".join(f"`{n}` IS NULL" for n in REQUIRED if n not in ("event_id", "event_ts", "fetched_ts"))
    return f"""
        CREATE TEMPORARY VIEW validated AS
        SELECT
            *,
            CASE
                WHEN NOT (line IS JSON OBJECT) OR event_id IS NULL THEN 'unparseable'
                WHEN event_ts IS NULL OR fetched_ts IS NULL THEN 'invalid_timestamp'
                WHEN track NOT IN ('global', 'watchlist') THEN 'invalid_track'
                WHEN {missing} THEN 'missing_field'
                ELSE CAST(NULL AS STRING)
            END AS reject_reason
        FROM parsed
    """


def deduplicated_view() -> str:
    return f"""
        CREATE TEMPORARY VIEW deduplicated AS
        SELECT {CLEAN_SELECT}
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_ts ASC) AS row_num
            FROM validated
            WHERE reject_reason IS NULL
        )
        WHERE row_num = 1
    """


def kafka_source(name: str, topic: str) -> str:
    return f"""
        CREATE TABLE {name} (
            line STRING,
            kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
            arrival_ts AS CAST(kafka_ts AS TIMESTAMP(3)),
            proc_ts AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{CONSUMER_GROUP}',
            'properties.auto.offset.reset' = 'earliest',
            'scan.startup.mode' = 'group-offsets',
            'format' = 'raw'
        )
    """


def raw_text_table(name: str, path: str) -> str:
    return f"""
        CREATE TABLE {name} (
            line STRING,
            dt STRING,
            hr STRING
        ) PARTITIONED BY (dt, hr) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'raw',
            'sink.rolling-policy.check-interval' = '10 s',
            'sink.rolling-policy.rollover-interval' = '{CHECKPOINT_INTERVAL_S} s',
            'sink.rolling-policy.inactivity-interval' = '{CHECKPOINT_INTERVAL_S} s'
        )
    """


def parquet_table(name: str, columns: str, path: str) -> str:
    return f"""
        CREATE TABLE {name} (
            {columns},
            dt STRING,
            hr STRING
        ) PARTITIONED BY (dt, hr) WITH (
            'connector' = 'filesystem',
            'path' = '{path}',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """


def partition(ts: str) -> str:
    return f"DATE_FORMAT({ts}, 'yyyy-MM-dd'), DATE_FORMAT({ts}, 'HH')"


def add_inserts(statements) -> None:
    statements.add_insert_sql(f"INSERT INTO raw_hdfs SELECT line, {partition('arrival_ts')} FROM github_updates_kafka")
    statements.add_insert_sql(
        f"INSERT INTO watchlist_raw_hdfs SELECT line, {partition('arrival_ts')} FROM github_list_kafka"
    )
    statements.add_insert_sql(f"INSERT INTO clean_kafka SELECT {CLEAN_SELECT} FROM deduplicated")
    statements.add_insert_sql(f"INSERT INTO clean_hdfs SELECT {CLEAN_SELECT}, {partition('event_ts')} FROM deduplicated")
    statements.add_insert_sql(f"""
        INSERT INTO bad_hdfs
        SELECT arrival_ts, line, reject_reason, {partition('arrival_ts')}
        FROM validated
        WHERE reject_reason IS NOT NULL
    """)


def create_views(t_env) -> None:
    t_env.execute_sql(parsed_view("github_updates_kafka"))
    t_env.execute_sql(validated_view())
    t_env.execute_sql(deduplicated_view())


def main() -> None:
    schema_text = load_schema()

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = t_env.get_config()
    config.set("table.local-time-zone", "UTC")
    config.set("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL_S} s")
    config.set("table.exec.state.ttl", f"{DEDUP_TTL_H} h")

    t_env.execute_sql(kafka_source("github_updates_kafka", RAW_TOPIC))
    t_env.execute_sql(kafka_source("github_list_kafka", WATCHLIST_TOPIC))
    t_env.execute_sql(raw_text_table("raw_hdfs", f"{HDFS_ROOT}/raw/github_repo_updates"))
    t_env.execute_sql(raw_text_table("watchlist_raw_hdfs", f"{HDFS_ROOT}/raw/github_repo_list"))

    clean_columns = ",\n            ".join(f"`{n}` {t}" for n, t in CLEAN_COLUMNS)
    avro_schema = schema_text.replace("'", "''")
    t_env.execute_sql(f"""
        CREATE TABLE clean_kafka (
            {clean_columns}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{CLEAN_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'key.format' = 'raw',
            'key.fields' = 'repo_name',
            'value.fields-include' = 'ALL',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'value.avro-confluent.subject' = '{CLEAN_TOPIC}-value',
            'value.avro-confluent.schema' = '{avro_schema}'
        )
    """)

    parquet_columns = ",\n            ".join(f"`{n}` {t.replace(' NOT NULL', '')}" for n, t in CLEAN_COLUMNS)
    t_env.execute_sql(parquet_table("clean_hdfs", parquet_columns, f"{HDFS_ROOT}/clean/github_repo_updates"))
    t_env.execute_sql(parquet_table(
        "bad_hdfs",
        "arrival_ts TIMESTAMP(3),\n            raw_line STRING,\n            reject_reason STRING",
        f"{HDFS_ROOT}/raw/github_repo_updates_bad",
    ))

    create_views(t_env)
    statements = t_env.create_statement_set()
    add_inserts(statements)
    statements.execute().wait()


if __name__ == "__main__":
    main()

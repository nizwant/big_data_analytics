"""Ingests CRAN mirror access logs from Kafka.

Reads raw Apache common-log lines from `cran_mirror_logs` and writes:

1. every raw line, unchanged, to HDFS /data/raw/cran_mirror_logs (text, partitioned
   by arrival time: dt=YYYY-MM-DD/hr=HH);
2. every line that parses and passes validation to the Kafka topic
   `cran_mirror_logs_clean` (Avro, schema from the schema registry) and to HDFS
   /data/clean/cran_mirror_logs
   (Parquet, partitioned by event time);
3. every line that fails parsing or validation, with the reason, to HDFS
   /data/raw/cran_mirror_logs_bad (Parquet, partitioned by arrival time).

The clean record layout is the Avro schema schemas/cran_mirror_logs_clean.avsc
(registered by deploy/schema_init.sh). The job refuses to start if its output columns
drift from the schema's fields; Flink also checks the column types against the
schema when it plans the Kafka sink. Value constraints (allowed HTTP methods, status
range) are enforced in the job because Avro cannot express them.
"""

import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "cran_mirror_logs")
CLEAN_TOPIC = os.environ.get("KAFKA_CLEAN_TOPIC", "cran_mirror_logs_clean")
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "flink-cran-logs-ingest")
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://namenode:8020/data")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_PATH = os.environ.get("CLEAN_SCHEMA_PATH", "/opt/schemas/cran_mirror_logs_clean.avsc")
# Avro cannot express value constraints, so the valid values live here. Flink cannot
# write Avro enums, which is why http_method is a plain string in the schema.
ALLOWED_METHODS = ["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "CONNECT", "TRACE"]
STATUS_MIN = 100
STATUS_MAX = 599
# Parquet files are finalized on checkpoint, so this is also the file roll interval.
CHECKPOINT_INTERVAL_S = int(os.environ.get("CHECKPOINT_INTERVAL_S", "60"))
# Event time may be at most this far ahead of the Kafka arrival time (clock skew) ...
MAX_FUTURE_SKEW_S = int(os.environ.get("MAX_FUTURE_SKEW_S", "300"))
# ... and at most this far behind it. Raise it when backfilling old logs through Kafka.
MAX_AGE_DAYS = int(os.environ.get("MAX_AGE_DAYS", "7"))

# Apache common log format. Only +0000 is accepted: the mirror logs in UTC and
# the timestamp is parsed without its offset, so anything else is rejected
# instead of being silently mis-dated.
LINE_PATTERN = (
    r'^(\S+) \S+ \S+ \[(\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}) \+0000\] '
    r'"(\S+) (\S+) ([^"]+)" (\d{3}) (\d+|-)$'
)

# Clean record columns (name, Flink type). Must match the fields of the Avro schema,
# including nullability: only bytes_sent is a nullable union. Avro timestamp-millis
# maps to TIMESTAMP(3); the values are UTC.
CLEAN_COLUMNS = [
    ("event_ts", "TIMESTAMP(3) NOT NULL"),
    ("arrival_ts", "TIMESTAMP(3) NOT NULL"),
    ("ip", "STRING NOT NULL"),
    ("http_method", "STRING NOT NULL"),
    ("path", "STRING NOT NULL"),
    ("protocol", "STRING NOT NULL"),
    ("status", "INT NOT NULL"),
    ("bytes_sent", "BIGINT"),
]


def load_schema() -> str:
    """Returns the Avro schema text, after checking it against the job's columns."""
    with open(SCHEMA_PATH) as f:
        text = f.read()
    schema = json.loads(text)
    fields = {field["name"]: field for field in schema["fields"]}
    job_columns = {name for name, _ in CLEAN_COLUMNS}
    if set(fields) != job_columns:
        raise RuntimeError(
            f"Job columns and {SCHEMA_PATH} differ: "
            f"only in schema {sorted(set(fields) - job_columns)}, "
            f"only in job {sorted(job_columns - set(fields))}"
        )
    return text


def sql_list(values) -> str:
    return ", ".join("'" + v.replace("'", "''") + "'" for v in values)


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


def main() -> None:
    schema_text = load_schema()

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = t_env.get_config()
    config.set("table.local-time-zone", "UTC")
    config.set("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL_S} s")

    t_env.execute_sql(f"""
        CREATE TABLE cran_logs_kafka (
            line STRING,
            kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{RAW_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{CONSUMER_GROUP}',
            'properties.auto.offset.reset' = 'earliest',
            'scan.startup.mode' = 'group-offsets',
            'format' = 'raw'
        )
    """)

    # Raw lines as text, one per line. Row-encoded files roll on a timer, so roll
    # at the checkpoint interval to keep them from staying hidden for long.
    t_env.execute_sql(f"""
        CREATE TABLE raw_hdfs (
            line STRING,
            dt STRING,
            hr STRING
        ) PARTITIONED BY (dt, hr) WITH (
            'connector' = 'filesystem',
            'path' = '{HDFS_ROOT}/raw/cran_mirror_logs',
            'format' = 'raw',
            'sink.rolling-policy.check-interval' = '10 s',
            'sink.rolling-policy.rollover-interval' = '{CHECKPOINT_INTERVAL_S} s',
            'sink.rolling-policy.inactivity-interval' = '{CHECKPOINT_INTERVAL_S} s'
        )
    """)

    clean_columns = ",\n            ".join(f"{n} {t}" for n, t in CLEAN_COLUMNS)
    avro_schema = schema_text.replace("'", "''")
    t_env.execute_sql(f"""
        CREATE TABLE clean_kafka (
            {clean_columns}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{CLEAN_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'avro-confluent.subject' = '{CLEAN_TOPIC}-value',
            'avro-confluent.schema' = '{avro_schema}'
        )
    """)

    parquet_columns = ",\n            ".join(
        f"{n} {t.replace(' NOT NULL', '')}" for n, t in CLEAN_COLUMNS
    )
    t_env.execute_sql(parquet_table("clean_hdfs", parquet_columns, f"{HDFS_ROOT}/clean/cran_mirror_logs"))
    t_env.execute_sql(parquet_table(
        "bad_hdfs",
        "arrival_ts TIMESTAMP(3),\n            raw_line STRING,\n            reject_reason STRING",
        f"{HDFS_ROOT}/raw/cran_mirror_logs_bad",
    ))

    # ts is NULL when the line does not match the pattern or its timestamp is invalid.
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW parsed AS
        SELECT
            line,
            CAST(kafka_ts AS TIMESTAMP(3)) AS arrival_ts,
            REGEXP_EXTRACT(line, '{LINE_PATTERN}', 1) AS ip,
            TO_TIMESTAMP(REGEXP_EXTRACT(line, '{LINE_PATTERN}', 2), 'dd/MMM/yyyy:HH:mm:ss') AS event_ts,
            REGEXP_EXTRACT(line, '{LINE_PATTERN}', 3) AS http_method,
            REGEXP_EXTRACT(line, '{LINE_PATTERN}', 4) AS path,
            REGEXP_EXTRACT(line, '{LINE_PATTERN}', 5) AS protocol,
            CAST(REGEXP_EXTRACT(line, '{LINE_PATTERN}', 6) AS INT) AS status,
            CAST(NULLIF(REGEXP_EXTRACT(line, '{LINE_PATTERN}', 7), '-') AS BIGINT) AS bytes_sent
        FROM cran_logs_kafka
    """)

    # reject_reason is NULL for valid records. The first failing rule wins.
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW validated AS
        SELECT
            *,
            CASE
                WHEN event_ts IS NULL THEN 'unparseable'
                WHEN status NOT BETWEEN {STATUS_MIN} AND {STATUS_MAX} THEN 'invalid_status'
                WHEN http_method NOT IN ({sql_list(ALLOWED_METHODS)}) THEN 'invalid_method'
                WHEN event_ts > TIMESTAMPADD(SECOND, {MAX_FUTURE_SKEW_S}, arrival_ts) THEN 'future_timestamp'
                WHEN event_ts < TIMESTAMPADD(DAY, -{MAX_AGE_DAYS}, arrival_ts) THEN 'stale_timestamp'
                ELSE CAST(NULL AS STRING)
            END AS reject_reason
        FROM parsed
    """)

    clean_select = "event_ts, arrival_ts, ip, http_method, path, protocol, status, bytes_sent"
    statements = t_env.create_statement_set()
    statements.add_insert_sql("""
        INSERT INTO raw_hdfs
        SELECT
            line,
            DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'yyyy-MM-dd'),
            DATE_FORMAT(CAST(kafka_ts AS TIMESTAMP(3)), 'HH')
        FROM cran_logs_kafka
    """)
    statements.add_insert_sql(f"""
        INSERT INTO clean_kafka
        SELECT
            event_ts, arrival_ts, ip, http_method, path, protocol, status, bytes_sent
        FROM validated
        WHERE reject_reason IS NULL
    """)
    statements.add_insert_sql(f"""
        INSERT INTO clean_hdfs
        SELECT
            {clean_select},
            DATE_FORMAT(event_ts, 'yyyy-MM-dd'),
            DATE_FORMAT(event_ts, 'HH')
        FROM validated
        WHERE reject_reason IS NULL
    """)
    statements.add_insert_sql("""
        INSERT INTO bad_hdfs
        SELECT
            arrival_ts,
            line,
            reject_reason,
            DATE_FORMAT(arrival_ts, 'yyyy-MM-dd'),
            DATE_FORMAT(arrival_ts, 'HH')
        FROM validated
        WHERE reject_reason IS NOT NULL
    """)
    statements.execute().wait()


if __name__ == "__main__":
    main()

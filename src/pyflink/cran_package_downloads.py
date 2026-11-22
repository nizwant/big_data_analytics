"""Extracts package downloads from the validated CRAN mirror log records.

Reads `cran_mirror_logs_clean` (Avro) and writes `cran_mirror_package_downloads`
(Avro, keyed by package_name so that all downloads of a package go to the same
partition, in order) and the same records as Parquet to HDFS /data/clean/cran_mirror_package_downloads
(partitioned by event time, dt=YYYY-MM-DD/hr=HH). Only requests for a package file (<name>_<version>.tar.gz, .tgz or .zip
under a known location) are kept; other requests, such as PACKAGES.rds, check logs
and web pages, are not downloads and are skipped. From the path it extracts the
package name, package version, artifact type and target system. The path, protocol
and byte count are dropped, and the HTTP status is reduced to SUCCESS or FAILURE.

Both record layouts are Avro schemas in schemas/*.avsc. The job refuses to start if
its columns drift from them; Flink also checks the column types against the schemas.
"""

import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
SOURCE_TOPIC = os.environ.get("KAFKA_SOURCE_TOPIC", "cran_mirror_logs_clean")
SINK_TOPIC = os.environ.get("KAFKA_SINK_TOPIC", "cran_mirror_package_downloads")
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "flink-cran-package-downloads")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
SCHEMA_DIR = os.environ.get("SCHEMA_DIR", "/opt/schemas")
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://namenode:8020/data")
# Parquet files are finalized on checkpoint, so this is also the file roll interval.
CHECKPOINT_INTERVAL_S = int(os.environ.get("CHECKPOINT_INTERVAL_S", "60"))

# Name and version as in <name>_<version>.<ext>. R package names are letters, digits
# and dots and start with a letter; versions are digits separated by dots or hyphens
# (for example 0.0.2-6). Matched against the last path segment.
PACKAGE_PATTERN = r"^.*/([A-Za-z][A-Za-z0-9.]*)_([0-9][0-9.-]*)\.(tar\.gz|tgz|zip)$"

# Columns (name, Flink type) of the two topics. Must match the Avro schemas,
# including nullability; Avro timestamp-millis maps to TIMESTAMP(3), values are UTC.
SOURCE_COLUMNS = [
    ("event_ts", "TIMESTAMP(3) NOT NULL"),
    ("arrival_ts", "TIMESTAMP(3) NOT NULL"),
    ("ip", "STRING NOT NULL"),
    ("http_method", "STRING NOT NULL"),
    ("path", "STRING NOT NULL"),
    ("protocol", "STRING NOT NULL"),
    ("status", "INT NOT NULL"),
    ("bytes_sent", "BIGINT"),
]
SINK_COLUMNS = [
    ("event_ts", "TIMESTAMP(3) NOT NULL"),
    ("arrival_ts", "TIMESTAMP(3) NOT NULL"),
    ("ip", "STRING NOT NULL"),
    ("http_method", "STRING NOT NULL"),
    ("package_name", "STRING NOT NULL"),
    ("package_version", "STRING NOT NULL"),
    ("artifact_type", "STRING NOT NULL"),
    ("target_system", "STRING NOT NULL"),
    ("outcome", "STRING NOT NULL"),
]


def load_schema(topic: str, columns: list[tuple[str, str]]) -> str:
    """Returns the Avro schema text of a topic, after checking it against the columns."""
    path = os.path.join(SCHEMA_DIR, f"{topic}.avsc")
    with open(path) as f:
        text = f.read()
    schema_fields = {field["name"] for field in json.loads(text)["fields"]}
    job_columns = {name for name, _ in columns}
    if schema_fields != job_columns:
        raise RuntimeError(
            f"Job columns and {path} differ: "
            f"only in schema {sorted(schema_fields - job_columns)}, "
            f"only in job {sorted(job_columns - schema_fields)}"
        )
    return text


def avro_options(topic: str, schema_text: str) -> str:
    # The schema is given explicitly so that Flink reads and writes the registered
    # record, with its name, instead of deriving one from the table columns.
    return f"""
            'format' = 'avro-confluent',
            'avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'avro-confluent.subject' = '{topic}-value',
            'avro-confluent.schema' = '{schema_text.replace("'", "''")}'"""


def main() -> None:
    source_schema = load_schema(SOURCE_TOPIC, SOURCE_COLUMNS)
    sink_schema = load_schema(SINK_TOPIC, SINK_COLUMNS)

    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = t_env.get_config()
    config.set("table.local-time-zone", "UTC")
    config.set("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL_S} s")

    source_columns = ",\n            ".join(f"{n} {t}" for n, t in SOURCE_COLUMNS)
    t_env.execute_sql(f"""
        CREATE TABLE logs_clean (
            {source_columns}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{CONSUMER_GROUP}',
            'properties.auto.offset.reset' = 'earliest',
            'scan.startup.mode' = 'group-offsets',{avro_options(SOURCE_TOPIC, source_schema)}
        )
    """)

    sink_columns = ",\n            ".join(f"{n} {t}" for n, t in SINK_COLUMNS)
    t_env.execute_sql(f"""
        CREATE TABLE package_downloads (
            {sink_columns}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'key.format' = 'raw',
            'key.fields' = 'package_name',
            'value.fields-include' = 'ALL',{avro_options(SINK_TOPIC, sink_schema)}
        )
    """)

    # Same columns as the topic, as nullable Parquet columns, plus the partition columns.
    parquet_columns = ",\n            ".join(f"{n} {t.replace(' NOT NULL', '')}" for n, t in SINK_COLUMNS)
    t_env.execute_sql(f"""
        CREATE TABLE package_downloads_hdfs (
            {parquet_columns},
            dt STRING,
            hr STRING
        ) PARTITIONED BY (dt, hr) WITH (
            'connector' = 'filesystem',
            'path' = '{HDFS_ROOT}/clean/cran_mirror_package_downloads',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY'
        )
    """)

    # package_name is NULL when the last path segment is not <name>_<version>.<ext>.
    t_env.execute_sql(f"""
        CREATE TEMPORARY VIEW extracted AS
        SELECT
            event_ts, arrival_ts, ip, http_method, path, status,
            REGEXP_EXTRACT(path, '{PACKAGE_PATTERN}', 1) AS package_name,
            REGEXP_EXTRACT(path, '{PACKAGE_PATTERN}', 2) AS package_version,
            REGEXP_EXTRACT(path, '{PACKAGE_PATTERN}', 3) AS artifact_type
        FROM logs_clean
    """)

    # target_system is NULL for a file type in an unexpected location, which is skipped.
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW classified AS
        SELECT
            *,
            CASE
                WHEN artifact_type = 'zip' AND path LIKE '/bin/windows/%' THEN 'windows'
                WHEN artifact_type = 'tgz' AND path LIKE '/bin/macosx/%' THEN 'macos'
                WHEN artifact_type = 'tar.gz'
                     AND (path LIKE '/src/contrib/%' OR path LIKE '/contrib/main/%') THEN 'source'
            END AS target_system
        FROM extracted
        WHERE package_name IS NOT NULL
    """)

    t_env.execute_sql("""
        CREATE TEMPORARY VIEW downloads AS
        SELECT
            event_ts, arrival_ts, ip, http_method,
            package_name, package_version, artifact_type, target_system,
            CASE WHEN status < 400 THEN 'SUCCESS' ELSE 'FAILURE' END AS outcome
        FROM classified
        WHERE target_system IS NOT NULL
    """)

    statements = t_env.create_statement_set()
    statements.add_insert_sql("INSERT INTO package_downloads SELECT * FROM downloads")
    statements.add_insert_sql("""
        INSERT INTO package_downloads_hdfs
        SELECT
            *,
            DATE_FORMAT(event_ts, 'yyyy-MM-dd'),
            DATE_FORMAT(event_ts, 'HH')
        FROM downloads
    """)
    statements.execute().wait()


if __name__ == "__main__":
    main()

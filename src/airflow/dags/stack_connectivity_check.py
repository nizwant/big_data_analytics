"""Checks that Airflow can reach HDFS and Kafka. Triggered manually, no schedule.

- hdfs: writes, reads back and deletes a file through WebHDFS (the webhdfs_default
  connection, also used for the task logs), then lists /data;
- kafka: lists the topics (KAFKA_BOOTSTRAP_SERVERS from the environment).

Both are set on the airflow service in docker-compose.yml. Run it after deploying:
    docker compose exec airflow airflow dags trigger stack_connectivity_check
"""

import os

from airflow.sdk import dag, task

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
HDFS_PROBE_PATH = "/tmp/airflow_connectivity_check.txt"


@dag(schedule=None, catchup=False, tags=["infra"])
def stack_connectivity_check():
    @task
    def hdfs() -> list[str]:
        from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

        client = WebHDFSHook().get_conn()
        client.write(HDFS_PROBE_PATH, data=b"ok", overwrite=True)
        with client.read(HDFS_PROBE_PATH) as reader:
            content = reader.read()
        client.delete(HDFS_PROBE_PATH)
        if content != b"ok":
            raise RuntimeError(f"Read back {content!r} from {HDFS_PROBE_PATH}, expected b'ok'")
        return client.list("/data")

    @task
    def kafka() -> list[str]:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        topics = admin.list_topics(timeout=10).topics
        return sorted(name for name in topics if not name.startswith("_"))

    hdfs()
    kafka()


stack_connectivity_check()

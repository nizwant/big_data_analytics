# CRAN Poland Mirror Analytics

This project proposes an analytics system for Poland's CRAN mirror (`cran.m2.ai`). It correlates package downloads with GitHub releases to track R package demand and adoption across the Polish data science ecosystem.

## Planned capabilities

- Monitor popular packages, download surges, and mirror health, including HTTP errors and bot/CI traffic.
- Measure the time between GitHub releases and download peaks, and track version migration.
- Match CRAN packages to their upstream repositories using the `r-universe` `crantogit` mapping.

See [Milestone 1](deliverables/Milestone_1.pdf) for the project proposal and architecture.

## Running

The stack includes:

- **Kafka** 4.3.2 (single-node KRaft) with the topics `cran_mirror_logs` (raw log lines), `cran_mirror_logs_clean` (validated records as Avro, schema in `schemas/cran_mirror_logs_clean.avsc`), `cran_mirror_package_downloads` (one record per package file download, Avro, schema in `schemas/cran_mirror_package_downloads.avsc`), `_schemas` (storage of the schema registry, compacted, 1 partition), `github_repo_updates`, and `github_repo_list` (topic auto-creation is disabled).
- **Schema Registry** (Confluent, 8.4.1) at `<host>:8085`. It keeps its schemas in the Kafka topic `_schemas`, so they persist with the Kafka data and survive restarts. `deploy/schema_init.sh` registers every `schemas/*.avsc` as the subject `<file name>-value`, for example `cran_mirror_logs_clean-value`; changes must stay backward compatible (the registry rejects others). Describe a schema with `curl localhost:8085/subjects/cran_mirror_logs_clean-value/versions/latest`.
- **HDFS** 3.4.2 (1 NameNode + 1 DataNode with replication 1 in dev; 3 DataNodes with replication 2 on stage/prod via `COMPOSE_PROFILES=ha` and `HDFS_REPLICATION=2`) for the batch layer. The NameNode is formatted automatically on first start. Inside the Docker network it is reachable at `hdfs://namenode:8020`. Only the web UI is published, at `<host>:9870`.
- **Cassandra** 5.0.9 (single node, 1 GB heap) for the serving layer, published at `<host>:9042` without authentication, with the keyspace `cran_analytics` (SimpleStrategy, RF 1). The Flink and Spark jobs will add the `rt_*` and `batch_*` tables.

- **Flink** 1.20.3 with PyFlink (1 JobManager + 1 TaskManager, 3 slots), UI at `<host>:8081`. `flink-cran-logs-job` (`src/pyflink/cran_logs_ingest.py`) reads `cran_mirror_logs` and writes: every raw line as text to `hdfs://namenode:8020/data/raw/cran_mirror_logs/dt=YYYY-MM-DD/hr=HH` (by arrival time); every parsed and valid record to the `cran_mirror_logs_clean` topic (Avro, wire format of the Confluent registry) and as Parquet to `/data/clean/cran_mirror_logs/dt=…/hr=…` (by event time); and every rejected line, with a `reject_reason`, as Parquet to `/data/raw/cran_mirror_logs_bad`. A line is rejected when it does not parse (`unparseable`), the status or method is outside the schema (`invalid_status`, `invalid_method`), or the event time is more than 5 minutes after (`future_timestamp`) or more than 7 days before (`stale_timestamp`) the Kafka arrival time (`MAX_FUTURE_SKEW_S` and `MAX_AGE_DAYS`). Files are finalized on checkpoint (every 60 s). `flink-cran-package-downloads-job` (`src/pyflink/cran_package_downloads.py`) reads `cran_mirror_logs_clean` and writes `cran_mirror_package_downloads` (keyed by `package_name`, so all downloads of a package go to the same partition) and the same records as Parquet to `/data/clean/cran_mirror_package_downloads/dt=…/hr=…` (by event time): only requests for a package file (`<name>_<version>.tar.gz|tgz|zip`) are kept, with `package_name`, `package_version`, `artifact_type` (`tar.gz`, `tgz`, `zip`) and `target_system` (`windows` for `/bin/windows` zips, `macos` for `/bin/macosx` tgz, `source` for `.tar.gz` under `/src/contrib` or `/contrib/main`, which are platform independent) extracted from the path. The path, protocol and byte count are dropped, and the status becomes `outcome`: `SUCCESS` below 400, otherwise `FAILURE`. The job fails and restarts until the topic exists, so run `deploy/init.sh` after the first start.
- **Airflow** 3.4.0 for the batch layer, UI and REST API at `<host>:8080` (dev login `airflow` / `airflow`). One container, `airflow`, runs the api-server, the scheduler (which also runs the tasks, LocalExecutor), the DAG processor and the triggerer; if one of them dies the container exits and Docker restarts it. On every start it migrates the metadata database and creates the admin user (both idempotent). The database is a separate Postgres 16 (`airflow-postgres`, not published). DAGs live in `src/airflow/dags`, mounted read-only; new files show up within a minute and new DAGs start paused. Task logs are written to HDFS under `/airflow/logs/dag_id=…/run_id=…/task_id=…/attempt=N.log` when a task finishes, read from there by the UI, and deleted from the container, which therefore keeps no state; the colons of the run ID become dashes in the path, because HDFS does not allow `:`. Only the log of a task that is still running is served from the container, and the DAG processor's parse logs stay inside the container (Airflow cannot upload them). The image (`src/airflow/Dockerfile`, about 0.9 GB) is Airflow's slim image plus the `hdfs` (WebHDFS) and `confluent-kafka` clients and the HDFS provider for the logs, with versions pinned by Airflow's constraints file. DAGs reach HDFS through the `webhdfs_default` connection (`webhdfs://airflow@namenode:9870`, also used for the logs; for example `WebHDFSHook().get_conn()`) and Kafka through `KAFKA_BOOTSTRAP_SERVERS` (`kafka:19092`), like the Flink jobs. The DAG `stack_connectivity_check` (manual trigger only) writes, reads and deletes a file in HDFS and lists the Kafka topics, to check the wiring after a deploy:

  ```sh
  docker compose exec airflow airflow dags trigger stack_connectivity_check
  ```

The Hadoop image is published only for amd64, so on Apple Silicon it runs under emulation.

**Dev** (also starts the fake log generator, which sends about 4 events/s to `cran_mirror_logs` topic):

```sh
docker compose --profile dev up -d --build
deploy/init.sh    # create topics, HDFS dirs and keyspace (idempotent)
```

To wipe the dev environment (containers and all Kafka, HDFS, Cassandra and Airflow data), run `deploy/dev_clean.sh` (`-y` skips the prompt, `--images` also removes the built images). It only touches the dev named volumes, never the stage/prod directories.

**Stage/prod** (data in `/srv/bigdata/{kafka,hdfs,cassandra,airflow}`; Kafka and Hadoop run as uid 1000, Cassandra as uid 999; the Airflow metadata database's Postgres sets the owner of its directory itself, and Airflow's logs go to HDFS):

```sh
sudo mkdir -p /srv/bigdata/kafka /srv/bigdata/hdfs/namenode /srv/bigdata/hdfs/datanode{1,2,3} /srv/bigdata/cassandra /srv/bigdata/airflow/postgres
sudo chown -R 1000:1000 /srv/bigdata/kafka /srv/bigdata/hdfs
sudo chown -R 999:999 /srv/bigdata/cassandra
docker compose --env-file deploy/prod.env --env-file deploy/prod.secrets.env up -d    # or deploy/stage.*
deploy/init.sh --env-file deploy/prod.env
```

The Airflow passwords and key in `docker-compose.yml` default to dev values. On stage/prod, put real ones in `deploy/prod.secrets.env` (or `stage.secrets.env`), which git ignores, and pass it as a second `--env-file` to every `up`:

```sh
cat > deploy/prod.secrets.env <<EOF
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_DB_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_FERNET_KEY=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')
EOF
```

Set them before the first start: Postgres takes `AIRFLOW_DB_PASSWORD` only when it initializes its data directory, and connection passwords saved in the UI become unreadable if `AIRFLOW_FERNET_KEY` changes later. The admin password is also set only when the user is first created; change it afterwards in the UI.

`deploy/init.sh` runs these scripts in order. Each one waits for its service, is safe to re-run, and passes any arguments through to `docker compose`:

- `deploy/kafka_init.sh` creates the Kafka topics (3 partitions, RF 1) and the registry's `_schemas` topic.
- `deploy/schema_init.sh` waits for the schema registry and registers the Avro schemas in `schemas/`.
- `deploy/hdfs_init.sh` waits for HDFS to leave safe mode, then creates `/data/raw/{cran_mirror_logs,cran_mirror_logs_bad,github_repo_updates,github_repo_list}` and `/data/clean/{cran_mirror_logs,cran_mirror_package_downloads}`, plus `/airflow/logs` for the Airflow task logs.
- `deploy/cassandra_init.sh` creates the `cran_analytics` keyspace.

In dev the log generator starts before its topic exists and buffers events until `deploy/init.sh` creates it.

Useful checks: `deploy/inspect.sh` prints an overview of the running stack (HDFS datanodes and directory tree, Kafka topics with message counts, the Flink consumer group lag and Flink job state, Airflow component health, DAGs and import errors). It is read-only and takes the same `--env-file` arguments as the init scripts. Individual commands:

```sh
docker compose exec namenode hdfs dfs -ls -R /data
docker compose exec cassandra cqlsh -e "describe keyspace cran_analytics"
docker compose exec airflow airflow dags list-import-errors
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_DATA_DIR` | `kafka-data` volume | Kafka data path |
| `HDFS_NAMENODE_DIR` | `hdfs-namenode` volume | NameNode metadata path |
| `HDFS_DATANODE1_DIR` … `HDFS_DATANODE3_DIR` | `hdfs-datanode1`…`3` volumes | Block storage path of each DataNode |
| `CASSANDRA_DATA_DIR` | `cassandra-data` volume | Cassandra data path |
| `CASSANDRA_MAX_HEAP` | `512M` | Cassandra JVM max heap |
| `AIRFLOW_POSTGRES_DIR` | `airflow-postgres` volume | Airflow metadata database path |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` | `airflow`, `airflow` | Airflow UI admin, created on first start |
| `AIRFLOW_DB_PASSWORD` | `airflow` | Password of the Airflow metadata database |
| `AIRFLOW_FERNET_KEY` | empty (no encryption) | Encryption key for connection passwords stored in the database |
| `BIND_ADDRESS` | `127.0.0.1` | Host interface for published ports (9092, 9042, 9870, 8081, 8085, 8080) |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Host advertised to clients outside Docker |

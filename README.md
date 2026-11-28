# CRAN Poland Mirror Analytics

This project proposes an analytics system for Poland's CRAN mirror (`cran.m2.ai`). It correlates package downloads with GitHub releases to track R package demand and adoption across the Polish data science ecosystem.

## Planned capabilities

- Monitor popular packages, download surges, and mirror health, including HTTP errors and bot/CI traffic.
- Measure the time between GitHub releases and download peaks, and track version migration.
- Match CRAN packages to their upstream repositories using the `r-universe` `crantogit` mapping.

See [Milestone 1](deliverables/Milestone_1.pdf) for the project proposal and architecture.

## Running

The stack includes:

- **Kafka** 4.3.2 (single-node KRaft) with the topics `cran_mirror_logs` (raw log lines), `cran_mirror_logs_clean` (validated records as Avro, schema in `schemas/cran_mirror_logs_clean.avsc`), `cran_mirror_package_downloads` (one record per package file download, Avro, schema in `schemas/cran_mirror_package_downloads.avsc`), `_schemas` (storage of the schema registry, compacted, 1 partition), `github_repo_updates` (raw GitHub events as JSON), `github_repo_updates_clean` (validated GitHub events with repeats removed, Avro, schema in `schemas/github_repo_updates_clean.avsc`), and `github_repo_list` (the watch-list snapshots of the Airflow DAG `github_watchlist`, one JSON message per row, kept 30 days instead of 7; topic auto-creation is disabled).
- **Schema Registry** (Confluent, 8.4.1) at `<host>:8085`. It keeps its schemas in the Kafka topic `_schemas`, so they persist with the Kafka data and survive restarts. `deploy/schema_init.sh` registers every `schemas/*.avsc` as the subject `<file name>-value`, for example `cran_mirror_logs_clean-value`; changes must stay backward compatible (the registry rejects others). Describe a schema with `curl localhost:8085/subjects/cran_mirror_logs_clean-value/versions/latest`.
- **HDFS** 3.4.2 (1 NameNode + 1 DataNode with replication 1 in dev; 3 DataNodes with replication 2 on stage/prod via `COMPOSE_PROFILES=ha` and `HDFS_REPLICATION=2`) for the batch layer. The NameNode is formatted automatically on first start. Inside the Docker network it is reachable at `hdfs://namenode:8020`. Only the web UI is published, at `<host>:9870`.
- **Cassandra** 5.0.9 (single node, 1 GB heap) for the serving layer, published at `<host>:9042` without authentication, with the keyspace `cran_analytics` (SimpleStrategy, RF 1). `deploy/cassandra_init.sh` creates the `batch_*` tables of the Spark jobs; the Flink jobs will add the `rt_*` tables.

- **Flink** 1.20.3 with PyFlink (1 JobManager + 1 TaskManager, 7 slots), UI at `<host>:8081`. `flink-cran-logs-job` (`src/pyflink/cran_logs_ingest.py`) reads `cran_mirror_logs` and writes: every raw line as text to `hdfs://namenode:8020/data/raw/cran_mirror_logs/dt=YYYY-MM-DD/hr=HH` (by arrival time); every parsed and valid record to the `cran_mirror_logs_clean` topic (Avro, wire format of the Confluent registry) and as Parquet to `/data/clean/cran_mirror_logs/dt=…/hr=…` (by event time); and every rejected line, with a `reject_reason`, as Parquet to `/data/raw/cran_mirror_logs_bad`. A line is rejected when it does not parse (`unparseable`), the status or method is outside the schema (`invalid_status`, `invalid_method`), or the event time is more than 5 minutes after (`future_timestamp`) or more than 7 days before (`stale_timestamp`) the Kafka arrival time (`MAX_FUTURE_SKEW_S` and `MAX_AGE_DAYS`). Files are finalized on checkpoint (every 60 s). `flink-cran-package-downloads-job` (`src/pyflink/cran_package_downloads.py`) reads `cran_mirror_logs_clean` and writes `cran_mirror_package_downloads` (keyed by `package_name`, so all downloads of a package go to the same partition) and the same records as Parquet to `/data/clean/cran_mirror_package_downloads/dt=…/hr=…` (by event time): only requests for a package file (`<name>_<version>.tar.gz|tgz|zip`) are kept, with `package_name`, `package_version`, `artifact_type` (`tar.gz`, `tgz`, `zip`) and `target_system` (`windows` for `/bin/windows` zips, `macos` for `/bin/macosx` tgz, `source` for `.tar.gz` under `/src/contrib` or `/contrib/main`, which are platform independent) extracted from the path. The path, protocol and byte count are dropped, and the status becomes `outcome`: `SUCCESS` below 400, otherwise `FAILURE`. The job fails and restarts until the topic exists, so run `deploy/init.sh` after the first start. `flink-github-events-job` (`src/pyflink/github_events_ingest.py`, profile `github`, parallelism 1) reads `github_repo_updates` and writes: every raw record as text to `/data/raw/github_repo_updates/dt=…/hr=…` (by arrival time); every valid record, with repeated `event.id`s dropped (ids are remembered for `DEDUP_TTL_H`, 48 h; the Kafka sink is at least once, so a job restart can still repeat a record), to the `github_repo_updates_clean` topic (Avro, keyed by `repo_name`) and as Parquet to `/data/clean/github_repo_updates/dt=…/hr=…` (by event time, GitHub's `created_at`); and every rejected record, with a `reject_reason` (`unparseable`, `invalid_timestamp`, `invalid_track`, `stale_timestamp`, `missing_field`; a record is stale when `created_at` is more than `MAX_AGE_DAYS`, 7, before its Kafka arrival time, which drops most of the 90 days of history that the poller's first poll of a timeline returns), as Parquet to `/data/raw/github_repo_updates_bad`. The job also copies the watch-list rows from `github_repo_list` as text to `/data/raw/github_repo_list`.
- **Spark** 3.5.8 with PySpark, a standalone cluster for the batch layer (image `src/spark/Dockerfile`, shared settings in `src/spark/spark-defaults.conf`). `spark-master` has its UI at `<host>:8090`, which also proxies the worker and application UIs, so the whole cluster can be browsed through that one port. Dev runs one worker, `spark-worker1`, offering 1 core and 1 GB to executors; the `ha` profile adds `spark-worker2` and `spark-worker3`, and stage/prod give each worker 2 cores and 2 GB (`SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`). An executor takes 1 core and 1 GB, so a dev worker runs one executor and a stage/prod worker two. An application takes every free core, so applications run one after another. Standalone Spark has no cluster deploy mode for Python, so every job is its own container (profile `spark-jobs`) that runs `spark-submit`, which is the driver, and exits when the job is done; the executors run on the workers. The cluster keeps no state of its own: event logs go to `hdfs://namenode:8020/spark/event-logs`, where the history server (`spark-history`, `<host>:18080`) reads them, and driver logs to `/spark/driver-logs`; both are deleted after 30 days. Executor logs stay in the worker containers (deleted 7 days after the application ends), because standalone Spark cannot ship them. Driver and executors run the same image, so a job can import only what it carries: the Cassandra connector (DataStax 3.5.1, which does not support Spark 4; the reason the cluster is on 3.5), numpy, pandas, pyarrow and the MLflow client (`mlflow-skinny`). `spark-package-daily-downloads` (`src/spark/jobs/package_daily_downloads.py`) reads the Parquet of `/data/clean/cran_mirror_package_downloads` and computes per package and day (UTC) the downloads (successful GET requests), the failed requests, the distinct client IPs of the downloads and the downloads per target system. It writes them to the Cassandra table `cran_analytics.batch_package_daily_downloads` (key `package_name`, newest `day` first) and as Parquet to `/data/batch/package_daily_downloads/dt=…`. Every run recomputes all days, so a run is idempotent and corrects a day that was still filling up at the previous one; files that Flink has not finalized yet are skipped. Its last log line names the workers whose executors took part. Run it with:

  ```sh
  docker compose --profile spark-jobs run --rm spark-package-daily-downloads
  docker compose exec cassandra cqlsh -e "SELECT * FROM cran_analytics.batch_package_daily_downloads LIMIT 10"
  ```

- **Airflow** 3.4.0 for the batch layer, UI and REST API at `<host>:8080` (dev login `airflow` / `airflow`). One container, `airflow`, runs the api-server, the scheduler (which also runs the tasks, LocalExecutor), the DAG processor and the triggerer; if one of them dies the container exits and Docker restarts it. On every start it migrates the metadata database and creates the admin user (both idempotent). The database is a separate Postgres 16 (`airflow-postgres`, not published). DAGs live in `src/airflow/dags`, mounted read-only; new files show up within a minute and new DAGs start paused. Task logs are written to HDFS under `/airflow/logs/dag_id=…/run_id=…/task_id=…/attempt=N.log` when a task finishes, read from there by the UI, and deleted from the container, which therefore keeps no state; the colons of the run ID become dashes in the path, because HDFS does not allow `:`. Only the log of a task that is still running is served from the container, and the DAG processor's parse logs stay inside the container (Airflow cannot upload them). The image (`src/airflow/Dockerfile`, about 0.9 GB) is Airflow's slim image plus the `hdfs` (WebHDFS) and `confluent-kafka` clients and the HDFS provider for the logs, with versions pinned by Airflow's constraints file, and the poller's `gh_poller` package (copied from `poller/`, the build context `poller`). DAGs reach HDFS through the `webhdfs_default` connection (`webhdfs://airflow@namenode:9870`, also used for the logs; for example `WebHDFSHook().get_conn()`) and Kafka through `KAFKA_BOOTSTRAP_SERVERS` (`kafka:19092`), like the Flink jobs. The DAG `stack_connectivity_check` (manual trigger only) writes, reads and deletes a file in HDFS and lists the Kafka topics, to check the wiring after a deploy:

  ```sh
  docker compose exec airflow airflow dags trigger stack_connectivity_check
  ```

  The DAG `github_watchlist` (`src/airflow/dags/github_watchlist.py`, daily at 04:00 UTC, unpaused by `deploy/start_github.sh`) refreshes the GitHub poller's watch-list. `land_crantogit` downloads `crantogit.csv` unchanged to `/data/raw/crantogit/dt=YYYY-MM-DD/crantogit.csv` (a retry or a second run on the same day reuses it); `build_watchlist` builds the watch-list from it with the poller's own code and publishes it to `github_repo_list`, one message per row, each carrying `built_at` and `rows_in_snapshot`, so a reader can tell a complete snapshot from one whose publishing failed. The account types it looks up (`GET /users/{owner}`; about 2,800 requests for a full list without cache) are cached in `/data/ref/github_account_types/account_types.csv`. The build uses the poller's token: it stops while fewer than 1,000 requests of the hourly quota are left, and the next try (3 tries, an hour apart) continues from the cache. A list with more than 10% fewer rows than the newest snapshot is not published (the task fails); check the landed file, then run it anyway:

  ```sh
  docker compose exec airflow airflow dags trigger github_watchlist --conf '{"force": true}'
  ```

  The token reaches Airflow as the Variable `github_token` (environment variable `AIRFLOW_VAR_GITHUB_TOKEN`, never stored in the database), from `GITHUB_TOKEN` in the shell or in `poller/.env`, which the `airflow` service also reads (with the build settings of [poller/README.md](poller/README.md)); it is read when the container is created.

- **GitHub poller** (`poller/`, Compose service `github-poller`, profile `github`) polls the GitHub Events API: the timelines of repositories behind CRAN packages (the watch-list) and, only with `GLOBAL_TRACK_ENABLED=true`, the global public feed. It polls the newest complete watch-list in `github_repo_list`, published by the DAG `github_watchlist`: on start it reads the whole topic, and then checks it every minute for a newer build; until there is one it waits. Every new event goes once, as JSON, to the topic `github_repo_updates`, keyed by the repository name (`owner/repo`); the value is `{"fetched_at", "track", "endpoint", "packages", "event"}` with the raw GitHub event in `event`. Delivery is at least once, so consumers deduplicate on `event.id`. The service needs `GITHUB_TOKEN` in the shell or in `poller/.env` (copy `poller/.env.example`; the shell wins); its state (ETags, seen event ids) lives in `POLLER_DATA_DIR`. Details: [poller/README.md](poller/README.md).

The Hadoop image is published only for amd64, so on Apple Silicon it runs under emulation.

**Dev** (nothing generates data until you ask; each step is idempotent):

```sh
deploy/up.sh             # 1. the structure: containers, topics, schemas, HDFS dirs, keyspace and tables
deploy/start_logs.sh     # 2. the fake log generator, about 4 events/s to the `cran_mirror_logs` topic
deploy/start_github.sh   # 3. the watch-list DAG, the GitHub poller and its Flink job (needs GITHUB_TOKEN)
```

All three pass extra arguments to `docker compose`, so stage/prod use `--env-file deploy/prod.env` as below.

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

GitHub watch-list, poller and its Flink job (need the topics and schemas from `deploy/init.sh`, and `poller/.env` with the token of the machine account; Airflow reads it when its container is created, so create the file before the `up` above or recreate `airflow` afterwards):

```sh
sudo mkdir -p /srv/bigdata/poller && sudo chown 1000:1000 /srv/bigdata/poller
deploy/start_github.sh --env-file deploy/prod.env   # unpauses and triggers github_watchlist, starts the poller and its Flink job
```

The first full build looks up about 2,800 accounts and so may need several tries of `build_watchlist` (an hour apart) while the poller uses most of the quota; the poller starts polling once the first build is published.

`deploy/init.sh` runs these scripts in order. Each one waits for its service, is safe to re-run, and passes any arguments through to `docker compose`:

- `deploy/kafka_init.sh` creates the Kafka topics (3 partitions, RF 1) and the registry's `_schemas` topic, and sets the retention of `github_repo_list` to 30 days.
- `deploy/schema_init.sh` waits for the schema registry and registers the Avro schemas in `schemas/`.
- `deploy/hdfs_init.sh` waits for HDFS to leave safe mode, then creates `/data/raw/{cran_mirror_logs,cran_mirror_logs_bad,github_repo_updates,github_repo_updates_bad,github_repo_list,crantogit}`, `/data/ref/github_account_types` for the account-type cache of the watch-list build, and `/data/clean/{cran_mirror_logs,cran_mirror_package_downloads,github_repo_updates}`, `/data/batch` for the Spark batch views, `/airflow/logs` for the Airflow task logs, and `/spark/event-logs` and `/spark/driver-logs` for Spark.
- `deploy/cassandra_init.sh` creates the `cran_analytics` keyspace and the batch view tables (`batch_package_daily_downloads`).

In dev the log generator starts before its topic exists and buffers events until `deploy/init.sh` creates it.

Useful checks: `deploy/inspect.sh` prints an overview of the running stack (HDFS datanodes and directory tree, Kafka topics with message counts, the Flink consumer group lag and Flink job state, the Spark workers and last applications, Airflow component health, DAGs and import errors). It is read-only and takes the same `--env-file` arguments as the init scripts. Individual commands:

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
| `SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY` | `1`, `1g` (stage/prod: `2`, `2g`) | Cores and memory each Spark worker offers to executors |
| `AIRFLOW_POSTGRES_DIR` | `airflow-postgres` volume | Airflow metadata database path |
| `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` | `airflow`, `airflow` | Airflow UI admin, created on first start |
| `AIRFLOW_DB_PASSWORD` | `airflow` | Password of the Airflow metadata database |
| `AIRFLOW_FERNET_KEY` | empty (no encryption) | Encryption key for connection passwords stored in the database |
| `BIND_ADDRESS` | `127.0.0.1` | Host interface for published ports (9092, 9042, 9870, 8081, 8085, 8080, 8090, 18080) |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Host advertised to clients outside Docker |
| `POLLER_DATA_DIR` | `poller-data` volume | GitHub poller state and logs |
| `POLLER_WATCHLIST_MAX_OWNERS` | `100` (stage/prod: `0`) | Size of the watch-list built by the DAG `github_watchlist`: the tier-1 organisations plus this many other owners; `0` keeps all |
| `POLLER_WATCHLIST_PATH` | `/data/watchlist.csv` | Watch-list file inside the poller container, used only with `WATCHLIST_SOURCE=file` in `poller/.env` |
| `POLLER_EVENTS_JSONL` | `false` | Also write the events to daily JSONL files in `POLLER_DATA_DIR` |

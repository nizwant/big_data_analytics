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
- **HDFS** 3.4.2 (1 NameNode + 3 DataNodes, replication 2) for the batch layer. The NameNode is formatted automatically on first start. Inside the Docker network it is reachable at `hdfs://namenode:8020`. Only the web UI is published, at `<host>:9870`.
- **Cassandra** 5.0.9 (single node, 1 GB heap) for the serving layer, published at `<host>:9042` without authentication, with the keyspace `cran_analytics` (SimpleStrategy, RF 1). The Flink and Spark jobs will add the `rt_*` and `batch_*` tables.

- **Flink** 1.20.3 with PyFlink (1 JobManager + 1 TaskManager, 3 slots), UI at `<host>:8081`. `flink-cran-logs-job` (`src/pyflink/cran_logs_ingest.py`) reads `cran_mirror_logs` and writes: every raw line as text to `hdfs://namenode:8020/data/raw/cran_mirror_logs/dt=YYYY-MM-DD/hr=HH` (by arrival time); every parsed and valid record to the `cran_mirror_logs_clean` topic (Avro, wire format of the Confluent registry) and as Parquet to `/data/clean/cran_mirror_logs/dt=…/hr=…` (by event time); and every rejected line, with a `reject_reason`, as Parquet to `/data/raw/cran_mirror_logs_bad`. A line is rejected when it does not parse (`unparseable`), the status or method is outside the schema (`invalid_status`, `invalid_method`), or the event time is more than 5 minutes after (`future_timestamp`) or more than 7 days before (`stale_timestamp`) the Kafka arrival time (`MAX_FUTURE_SKEW_S` and `MAX_AGE_DAYS`). Files are finalized on checkpoint (every 60 s). `flink-cran-package-downloads-job` (`src/pyflink/cran_package_downloads.py`) reads `cran_mirror_logs_clean` and writes `cran_mirror_package_downloads` (keyed by `package_name`, so all downloads of a package go to the same partition) and the same records as Parquet to `/data/clean/cran_mirror_package_downloads/dt=…/hr=…` (by event time): only requests for a package file (`<name>_<version>.tar.gz|tgz|zip`) are kept, with `package_name`, `package_version`, `artifact_type` (`tar.gz`, `tgz`, `zip`) and `target_system` (`windows` for `/bin/windows` zips, `macos` for `/bin/macosx` tgz, `source` for `.tar.gz` under `/src/contrib` or `/contrib/main`, which are platform independent) extracted from the path. The path, protocol and byte count are dropped, and the status becomes `outcome`: `SUCCESS` below 400, otherwise `FAILURE`. The job fails and restarts until the topic exists, so run `deploy/init.sh` after the first start.

The Hadoop image is published only for amd64, so on Apple Silicon it runs under emulation.

**Dev** (also starts the fake log generator, which sends about 4 events/s to `cran_mirror_logs` topic):

```sh
docker compose --profile dev up -d --build
deploy/init.sh    # create topics, HDFS dirs and keyspace (idempotent)
```

To wipe the dev environment (containers and all Kafka, HDFS and Cassandra data), run `deploy/dev_clean.sh` (`-y` skips the prompt, `--images` also removes the built images). It only touches the dev named volumes, never the stage/prod directories.

**Stage/prod** (data in `/srv/bigdata/{kafka,hdfs,cassandra}`; Kafka and Hadoop run as uid 1000, Cassandra as uid 999):

```sh
sudo mkdir -p /srv/bigdata/kafka /srv/bigdata/hdfs/namenode /srv/bigdata/hdfs/datanode{1,2,3} /srv/bigdata/cassandra
sudo chown -R 1000:1000 /srv/bigdata/kafka /srv/bigdata/hdfs
sudo chown -R 999:999 /srv/bigdata/cassandra
docker compose --env-file deploy/prod.env up -d    # or deploy/stage.env
deploy/init.sh --env-file deploy/prod.env
```

`deploy/init.sh` runs these scripts in order. Each one waits for its service, is safe to re-run, and passes any arguments through to `docker compose`:

- `deploy/kafka_init.sh` creates the Kafka topics (3 partitions, RF 1) and the registry's `_schemas` topic.
- `deploy/schema_init.sh` waits for the schema registry and registers the Avro schemas in `schemas/`.
- `deploy/hdfs_init.sh` waits for HDFS to leave safe mode, then creates `/data/raw/{cran_mirror_logs,cran_mirror_logs_bad,github_repo_updates,github_repo_list}` and `/data/clean/{cran_mirror_logs,cran_mirror_package_downloads}`.
- `deploy/cassandra_init.sh` creates the `cran_analytics` keyspace.

In dev the log generator starts before its topic exists and buffers events until `deploy/init.sh` creates it.

Useful checks: `deploy/inspect.sh` prints an overview of the running stack (HDFS datanodes and directory tree, Kafka topics with message counts, the Flink consumer group lag and Flink job state). It is read-only and takes the same `--env-file` arguments as the init scripts. Individual commands:

```sh
docker compose exec namenode hdfs dfs -ls -R /data
docker compose exec cassandra cqlsh -e "describe keyspace cran_analytics"
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_DATA_DIR` | `kafka-data` volume | Kafka data path |
| `HDFS_NAMENODE_DIR` | `hdfs-namenode` volume | NameNode metadata path |
| `HDFS_DATANODE1_DIR` … `HDFS_DATANODE3_DIR` | `hdfs-datanode1`…`3` volumes | Block storage path of each DataNode |
| `CASSANDRA_DATA_DIR` | `cassandra-data` volume | Cassandra data path |
| `CASSANDRA_MAX_HEAP` | `1G` | Cassandra JVM max heap |
| `BIND_ADDRESS` | `127.0.0.1` | Host interface for published ports (9092, 9042, 9870, 8081, 8085) |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Host advertised to clients outside Docker |

# CRAN Poland Mirror Analytics

This project proposes an analytics system for Poland's CRAN mirror (`cran.m2.ai`). It correlates package downloads with GitHub releases to track R package demand and adoption across the Polish data science ecosystem.

## Planned capabilities

- Monitor popular packages, download surges, and mirror health, including HTTP errors and bot/CI traffic.
- Measure the time between GitHub releases and download peaks, and track version migration.
- Match CRAN packages to their upstream repositories using the `r-universe` `crantogit` mapping.

See [Milestone 1](deliverables/Milestone_1.pdf) for the project proposal and architecture.

## Running

The stack includes:

- **Kafka** 4.3.2 (single-node KRaft) with the topics `cran_mirror_logs`, `github_repo_updates`, and `github_repo_list` (topic auto-creation is disabled).
- **HDFS** 3.4.2 (1 NameNode + 1 DataNode, replication 1) for the batch layer. The NameNode is formatted automatically on first start. Inside the Docker network it is reachable at `hdfs://namenode:8020`. Only the web UI is published, at `<host>:9870`.
- **Cassandra** 5.0.9 (single node, 1 GB heap) for the serving layer, published at `<host>:9042` without authentication, with the keyspace `cran_analytics` (SimpleStrategy, RF 1). The Flink and Spark jobs will add the `rt_*` and `batch_*` tables.

The Hadoop image is published only for amd64, so on Apple Silicon it runs under emulation.

**Dev** (also starts the fake log generator, which sends about 4 events/s to `cran_mirror_logs` topic):

```sh
docker compose --profile dev up -d --build
deploy/init.sh    # create topics, HDFS dirs and keyspace (idempotent)
```

**Stage/prod** (data in `/srv/bigdata/{kafka,hdfs,cassandra}`; Kafka and Hadoop run as uid 1000, Cassandra as uid 999):

```sh
sudo mkdir -p /srv/bigdata/kafka /srv/bigdata/hdfs/namenode /srv/bigdata/hdfs/datanode /srv/bigdata/cassandra
sudo chown -R 1000:1000 /srv/bigdata/kafka /srv/bigdata/hdfs
sudo chown -R 999:999 /srv/bigdata/cassandra
docker compose --env-file deploy/prod.env up -d    # or deploy/stage.env
deploy/init.sh --env-file deploy/prod.env
```

`deploy/init.sh` runs these scripts in order. Each one waits for its service, is safe to re-run, and passes any arguments through to `docker compose`:

- `deploy/kafka_init.sh` creates the Kafka topics (3 partitions, RF 1).
- `deploy/hdfs_init.sh` waits for HDFS to leave safe mode, then creates `/data/raw/{cran_mirror_logs,github_repo_updates,github_repo_list}`.
- `deploy/cassandra_init.sh` creates the `cran_analytics` keyspace.

In dev the log generator starts before its topic exists and buffers events until `deploy/init.sh` creates it.

Useful checks:

```sh
docker compose exec namenode hdfs dfs -ls -R /data
docker compose exec cassandra cqlsh -e "describe keyspace cran_analytics"
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_DATA_DIR` | `kafka-data` volume | Kafka data path |
| `HDFS_NAMENODE_DIR` | `hdfs-namenode` volume | NameNode metadata path |
| `HDFS_DATANODE_DIR` | `hdfs-datanode` volume | DataNode block storage path |
| `CASSANDRA_DATA_DIR` | `cassandra-data` volume | Cassandra data path |
| `CASSANDRA_MAX_HEAP` | `1G` | Cassandra JVM max heap |
| `BIND_ADDRESS` | `127.0.0.1` | Host interface for published ports (9092, 9042, 9870) |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Host advertised to clients outside Docker |

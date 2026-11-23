# 2026-11-22 — Docker compose resource usage, DataNode scale-down, Flink slots, dependency chain

Conversation with Claude Code. User messages are verbatim; assistant replies are condensed. Messages that only checked on progress or asked for this dump are left out.

---

**User:** Can you check what is eating some much resources in this docker compose (both cpu and ram)? and how to fix that?

**Assistant:** The containers added up to about 5.1 GB of RAM and a few percent CPU each, so they were not what filled the 15 GB host. Brave (~3.5 GB+) and VS Code (~1 GB) took a large share, and 4 GB of swap was already in use. The CPU readings were inflated because the stack had just restarted (JVM startup); at steady state every container was below 4%.

Largest containers: Cassandra 1.29 GB, Airflow 1.0 GB, NameNode 435 MB, Kafka 345 MB (heap set to 1 GB), three DataNodes ~300 MB each, schema-registry 320 MB.

Suggested changes: TaskManager 3072m/6 slots down to 2048m/3 slots, merge the two Flink job-client containers, fewer DataNodes, Cassandra heap 512M, Kafka `KAFKA_HEAP_OPTS`, optionally drop the Airflow triggerer, and `mem_limit` as a safety net.

---

**User:** Can we scale down the datanodes to 1 (for the dev only), set that flink task manager flag and limit cassandra heap also those kafka args (keep the ariflow)

**Assistant:** Applied in `docker-compose.yml`:

- `datanode2` and `datanode3` moved to a `ha` profile, so dev runs one DataNode. Replication is `${HDFS_REPLICATION:-1}` in both the HDFS config and the Flink client setting.
- TaskManager `process.size` 2048m and 3 slots; Cassandra heap default `512M`; Kafka `KAFKA_HEAP_OPTS: -Xms256m -Xmx512m`. Airflow untouched.
- `deploy/stage.env` and `deploy/prod.env` set `COMPOSE_PROFILES=ha` and `HDFS_REPLICATION=2`, so stage and prod keep 3 DataNodes with replication 2. README updated.

Existing dev HDFS data was written with replication 2, so the advice was a clean reset with `deploy/dev_clean.sh`, or `up -d --remove-orphans` followed by `hdfs dfs -setrep -R 1 /`.

---

**User:** What about now?

**Assistant:** The new settings had not taken effect yet (`datanode2` and `datanode3` were still running). Host RAM used had dropped from 12 GB to 10 GB and available memory had risen from 2.4 GB to 5.1 GB, but swap was still at 4.8 GB. The stack was about 5.4 GB, slightly higher because the Flink jobs were now running (job clients ~390 MB each). CPU was below 7% for every container.

---

**User:** Why no cran-package-downloads? (with `deploy/inspect.sh` output: empty `cran_mirror_package_downloads` topic and HDFS directory, no `flink-cran-package-downloads` consumer group, both Flink jobs listed as RUNNING)

**Assistant:** Caused by the earlier change. The TaskManager had 3 slots, each job runs at parallelism 3, and the raw-logs job took all of them. The package-downloads job was RUNNING in Flink but its tasks stayed in `CREATED` (0 records read, 3 failed checkpoints), so it never consumed anything and never created its group.

Fix: `taskmanager.numberOfTaskSlots` back to 6 (process size stays 2048m) and the stack was recreated. The cluster then showed 6 slots and all tasks of both jobs RUNNING. Counts were still 0 right after startup; Parquet files appear after the first 60 s checkpoint. Alternative: keep 3 slots and set `parallelism.default` to 1.

---

**User:** Can you add dependency chain in docker?

**Assistant:** Most of the chain already existed (namenode, datanode1, jobmanager, taskmanager; kafka, schema-registry; Flink jobs after jobmanager, taskmanager, kafka and schema-registry; airflow after its postgres). Added to `docker-compose.yml`:

- Both Flink job containers also wait for `datanode1` to be healthy, since they write Parquet to HDFS.
- `airflow` also waits for `namenode` and `kafka` to be healthy, since its DAGs use WebHDFS and Kafka.

Left alone: Cassandra (nothing uses it yet), the TaskManager (no healthcheck, so jobs wait only for `service_started`), and topics and schemas, which `deploy/init.sh` creates and Compose cannot order. Validated with `docker compose config`; not restarted.

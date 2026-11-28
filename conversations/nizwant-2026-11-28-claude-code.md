# 2026-11-28 — Where Flink runs, Spark standalone cluster with 3 workers, first batch job

Conversation with Claude Code. User messages are verbatim; assistant replies are condensed. The assistant's clarifying questions are listed with the options offered and the answer chosen.

---

**User:** where does flink runs, ie executes jobs (in current setup)?

**Assistant:** In the single `taskmanager` container (7 slots, 3 GB). The `jobmanager` only coordinates: scheduling, checkpoints and the UI on 8081. The three `flink-*-job` containers are clients: each runs `flink run -m jobmanager:8081 -py …`, stays attached and resubmits the job if it dies. The jobs use only the Table API/SQL with no Python UDFs, so Python runs only in the submitter, which builds the plan; on the TaskManager everything runs as JVM operators. The two CRAN jobs take 3 slots each (`parallelism.default: 3`) and the GitHub job 1 (`-p 1`), exactly the 7 slots. There is no TaskManager scaling, and checkpoints go to HDFS.

---

**User:** does it needs spark cluster?

**Assistant:** No. Flink is its own engine with its own cluster, and Spark was not deployed (the reports mark it as planned). The two meet only through storage: Flink writes Parquet to HDFS, and Spark (batch layer, scheduled by Airflow) will read it and write the Cassandra `batch_*` tables. They share only the host's RAM.

---

**User:** okay, I want to create spark cluster simulation, 3 workers

**User (while the assistant was reading the stack):** grill me for details

**Assistant:** Checked the options first: apache/spark images exist for 4.3.0 and 3.5.8; the DataStax Cassandra connector's latest release, 3.5.1, supports Spark 3.5 only; the host has 12 cores and 30 GB of RAM, about 15 GB free. Then asked three rounds of questions.

### Questions and answers

1. **Which Spark version?** Options: *3.5.8 + connector* (recommended; writing to Cassandra is one `df.write.format("org.apache.spark.sql.cassandra")`, but the image's Python 3.10 differs from Airflow's 3.12, which matters only if Airflow runs the driver); *4.3.0* (newest, Python 3.12, but no Cassandra connector, so writes through the Python driver per partition). **Answer:** 3.5.8 + connector.
2. **Where should the driver run?** Standalone Spark has no cluster deploy mode for Python, so the driver runs where `spark-submit` runs. Options: *submitter containers* (like the Flink jobs, one-off containers of the Spark image); *inside Airflow* (SparkSubmitOperator; Java, Spark and PySpark in the Airflow image); *Spark Connect server* (thin client in Airflow, needs Spark 4 to be solid); *decide later*. **Answer:** submitter containers.
3. **How big should each worker be?** Options: *2 cores, 2 GB* (recommended); *1 core, 1 GB*; *4 cores, 4 GB*. **Answer:** "2 cores 2gb ram in stage and prod 1gb 1 core in dev".
4. **Should Spark follow the HDFS dev/ha split?** Options: *always 3 workers*; *mirror HDFS* (1 worker in dev, the `ha` profile adds two); *separate `spark` profile*. **Answer:** mirror HDFS.
5. **What should Spark keep in HDFS?** Executor logs cannot leave the workers in standalone mode. Options: *history server + driver logs* (recommended); *history server only*; *nothing*. **Answer:** history server + driver logs.
6. **Which job should prove the cluster works?** Options: *smoke test over real data* (recommended; reads Flink's Parquet, counts downloads per package and day, writes Cassandra and HDFS); *SparkPi only*; *generic submitter, no job*. **Answer:** smoke test over real data.
7. **How should several applications share the cluster?** Options: *FIFO, each takes all free cores* (recommended); *cap per application* with `spark.cores.max`. **Answer:** FIFO.
8. **Which Python packages should the image carry?** Options: *numpy + pandas + pyarrow* (recommended); *also mlflow now*; *bare PySpark*. **Answer:** also mlflow now.
9. **Which Cassandra table should the smoke test write?** Options: *real batch view* `batch_package_daily_downloads` created in `deploy/cassandra_init.sh` (recommended); *throwaway table*; *the job creates its own table*. **Answer:** real batch view.

### Result

**Assistant:** Built the cluster:

- Image `bigdata/spark:3.5.8` (`src/spark/Dockerfile`) from `apache/spark:3.5.8-scala2.12-java17-python3-ubuntu`, plus the connector assembly jar, numpy 1.26.4 (PySpark 3.5 predates NumPy 2), pandas 2.2.3, pyarrow 17.0.0 and `mlflow-skinny` 3.18.0 (the client, without server or UI). The master, workers, history server and drivers all use this one image, so the driver and executor Python always match.
- Shared settings in `src/spark/spark-defaults.conf`: master URL, HDFS, event and driver logs, Cassandra host, reverse proxy, executors of 1 core and 1 GB. Executor size is fixed, so the worker size alone decides one executor per dev worker and two per stage/prod worker. `dfs.replication` differs per environment and the file cannot read variables, so it reaches every `spark-submit` through `SPARK_SUBMIT_OPTS`; executors get it from the driver.
- Compose: `spark-master` (UI on 8090, since 8080 is Airflow; it proxies the worker and application UIs), `spark-worker1` plus `spark-worker2/3` with `ha`, sized by `SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY` (`2`/`2g` in `stage.env` and `prod.env`), `spark-history` (18080), and the job container `spark-package-daily-downloads` (profile `spark-jobs`, `restart: "no"`). `init: true` makes tini reap the executor processes; worker cleanup deletes the work directories 7 days after an application ends.
- Job `src/spark/jobs/package_daily_downloads.py`: per package and UTC day, the downloads (successful GET requests), failed requests, distinct client IPs of the downloads and downloads per target system. It writes Cassandra `batch_package_daily_downloads` (key `package_name`, `day` descending) and Parquet `/data/batch/package_daily_downloads/dt=…`, one file per day. Every run recomputes all days, so it is idempotent. It logs through log4j, so its summary line, including the executors per worker, also lands in the driver log in HDFS.
- `deploy/hdfs_init.sh` creates `/data/batch`, `/spark/event-logs` and `/spark/driver-logs`; `deploy/cassandra_init.sh` creates the table; `deploy/inspect.sh` shows the Spark workers and the last applications; `deploy/dev_clean.sh` and the README were updated.

Verified on dev with `--profile ha`:

- All 3 workers registered (healthcheck: the worker reports a master URL only once registered).
- The history server restarts until `hdfs_init.sh` has created its directory, then recovers.
- On empty input the job wrote 0 rows and exited 0.
- Over live data (log generator → Flink → Parquet) it wrote 10, then 61 package-days, with an executor on each of the three workers.
- After the rerun, Cassandra's `count(*)` and `sum(downloads)` equalled the job's own totals (61 and 63), so reruns overwrite rather than duplicate.
- The event log, the driver log (with the job's line) and the history listing were in place. Replication 1 on the event log showed that `SPARK_SUBMIT_OPTS` reaches the HDFS client (its default would be 3).
- Not tested: the stage/prod size (2 executors per worker), since that needs recreating the workers.

Incident: the first `docker compose run` of the job tried to recreate cassandra, datanode1 and spark-master. cassandra and datanode1 still had config hashes from 2026-11-22, and an `up --build` running in parallel had re-tagged `bigdata/spark`. The host's AppArmor problem (see the 2026-11-22 log) blocked the stops and left three never-started containers in state Created. Those were removed; the running containers were unaffected. The rest of the testing used `--no-deps`. To get data, the NameNode, Kafka, the schema registry, Flink and the log generator were started.

Open: how Airflow will start the job containers. It cannot run `docker compose` itself, so it needs the Docker socket (DockerOperator) or another trigger.

---

**User:** yes, commit it

**Assistant:** Committed as `dfc9787` "Add Spark standalone cluster and first batch job" on `feat/spark-cluster-setup` (not pushed).

---

**User:** create the entry in conversatins, remember to add question/answers part, and give me description to PR, that matches the template

**Assistant:** Wrote this entry and drafted the PR description in the repo's template (Summary, Changes, Usage, Notes, Testing).

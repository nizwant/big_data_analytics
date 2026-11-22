# 2026-11-22 — Report review, Airflow infrastructure, slimming Airflow to one container, task logs in HDFS

Conversation with Claude Code. User messages are verbatim; assistant replies are condensed. Messages that only asked for this dump are left out.

---

**User:** I want to setup airflow, I'm not sure if airflow is the right tool, you can read the report from deliverables, we didn't had time much time while preparing it so you can go through it and review what is there

**Assistant:** Read `deliverables/Milestone_1` (the .md and the PDF with its figures), the compose stack, the Flink jobs and the earlier conversation logs, and checked the live crantogit CSV.

Verdict on Airflow: yes, for the batch layer. The batch work is a set of daily steps with dependencies, partitioned like the `dt=` directories in HDFS: wait for a day's data, compact, dedupe, run Spark aggregates and write Cassandra `batch_*`; pull crantogit and build the GitHub watch-list; train weekly. Airflow's model maps one run to one day and backfills by date. Cron has no dependencies, retries or backfill; Dagster would fit but diverges from the report. Airflow should not run the GitHub poller, manage the Flink jobs or process data in its own workers. Prerequisite: nothing marks an HDFS partition complete (no partition-commit policy, and events are accepted up to 7 days late), so either add watermarks and `_SUCCESS` files or run the DAG with a delay.

Report review, wrong or risky:

- The logs are Apache common format, without the User-Agent the report lists, so bot detection has to be based on IP behaviour.
- The crantogit columns are `package,url,subdir,registry` (19,116 rows, 18,850 on GitHub), not the ones in Table 1. The coverage gap is packages missing from the file, not other hosts; 1,550 rows are Bioconductor and 531 packages share a repo through `subdir`.
- The GitHub Events API is not real-time (GitHub documents 30 s to 6 h latency) and polling ~18,850 repos does not fit the rate limits; poll a watch-list and use GH Archive for history.
- The lag from GitHub tag to peak downloads mostly measures CRAN acceptance; record the CRAN publication date too.
- "Adoption in Poland" is adoption via the Polish mirror; nothing geolocates clients.
- The multi-year history has no source, and old logs cannot go through Kafka (the ingest job rejects events older than 7 days).
- The load forecast is a batch output; only bot classification needs inference in Flink, and that needs a Python model, not Spark MLlib.

Out of date compared with the code: topic names, JSON instead of Avro and no mention of the schema registry, Figure 3 showing Kafka writing HDFS directly, the request filtering in 3.3, and the promised watermarks. Nits: "DiagramL", "Table 3" without a Table 2, and `cran.m2.ai` and "3 slots" in the README.

Also found: commit e6fc2f1 put the `_schemas` creation inside the wait loop of `deploy/kafka_init.sh`, so under `set -e` the script exits instead of waiting; the Flink file sinks write about 13,000 small files a day; the disk is 92% full. Asked whether to set up Airflow alone or together with Spark.

---

**User:** I want to setup airflow, but for now lets just focus on infrastructure

**Assistant:** Added Airflow 3.4.0 following Airflow's reference compose file, with the LocalExecutor: `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, a one-shot `airflow-init` and `airflow-postgres` (Postgres 16.16).

- The image added the HDFS, Kafka and Cassandra providers, pinned by Airflow's constraints file. The first build failed because the HDFS provider's Kerberos libraries (`gssapi`, `krb5`) have no Linux wheels, so they were compiled in a separate build stage.
- Connections to HDFS, Kafka and Cassandra were set as environment variables.
- The JWT secret and API secret key had to be set explicitly: otherwise each container generates its own, and tasks cannot report back to the api-server.
- Stage/prod secrets go in an untracked `deploy/<env>.secrets.env`. `inspect.sh`, `dev_clean.sh`, the env files and the README were updated, and the `kafka_init.sh` loop was fixed.

Verified: a `stack_connectivity_check` DAG (HDFS write, read and delete; Kafka topics; Cassandra version) succeeded through the scheduler, login worked, and re-running `airflow-init` was idempotent. Airflow used about 1.25 GB of memory.

---

**User:** is there a way to make it leaner? thats a lot of containers and complexity

**Assistant:** Collapsed Airflow into one `airflow` container running the api-server, scheduler, DAG processor and triggerer (`... & wait -n`, so if one exits, the container exits and Docker restarts it). The image's entrypoint migrates the database and creates the admin user on every start, so `airflow-init` is gone. Processes in one container share one generated config, so only the Fernet key still has to be fixed.

- Switched to Airflow's slim base image (885 MB instead of 2.46 GB) and replaced the three providers with the plain `hdfs` and `confluent-kafka` clients, configured through `WEBHDFS_URL` and `KAFKA_BOOTSTRAP_SERVERS` like the Flink jobs. That removed the Kerberos build stage. Cassandra left Airflow, since Spark will write to it. Added `graphviz` only to silence a CLI notice.
- Kept Postgres rather than SQLite, which Airflow advises against outside development.
- Result: 2 containers instead of 6, 3 stage/prod secrets instead of 5.

Verified: the check DAG (now HDFS and Kafka) succeeded, all four components reported healthy, login worked, and killing the scheduler process restarted the container, which was healthy again after about 16 s.

Two findings along the way:

- The host runs two Docker daemons, the snap and apt's `docker-ce`, which share the AppArmor profile `docker-default`. The apt daemon started later at boot and replaced the profile, so the snap daemon, which runs this stack, gets `permission denied` on every container stop. Worked around it by stopping containers from inside (`docker exec <container> bash -c 'kill -TERM 1'`); the real fix needs sudo and is the user's call.
- The earlier `kafka_init.sh` wait test was invalid, because the Kafka stop had been silently denied. Retested against a project with no Kafka: the old script exited after 0 s, the fixed one was still waiting at 15 s.

---

**User:** genrate me PR description

**Assistant:** Drafted the description in the repo's PR template (Summary, Changes, Usage, Notes, Testing). Before writing the Testing section, started the final image against an empty throwaway Postgres to confirm it creates the schema and the admin user on a fresh database.

---

**User:** I think airflow logs should land in HDFS. we are doing simulation here of distributed system in docker, keep that in mind

**Assistant:** Switched Airflow to remote task logging in HDFS, so the Airflow container keeps no state of its own; Postgres is the only stateful part.

- Airflow 3.4 supports `hdfs://` log folders through the HDFS provider, which brought the provider back. It declares `hdfs[kerberos]` (the `gssapi`/`krb5` packages that need a compiler) but imports Kerberos only when `[core] security = kerberos`, so it is installed with `--no-deps`; the image stays at 885 MB with no build stage, and `pip check` flags only the unused `pandas`.
- Task logs are uploaded to `/airflow/logs/...` when a task finishes, read from there by the UI, and deleted locally (`delete_local_logs`). The logs volume, `AIRFLOW_LOGS_DIR` and the stage/prod `chown` step are gone; `hdfs_init.sh` creates `/airflow/logs` and `inspect.sh` shows its size.
- The handler and the DAGs share one connection, `webhdfs_default` (`webhdfs://airflow@namenode:9870`), which replaced `WEBHDFS_URL`; the check DAG now uses `WebHDFSHook`.
- The first upload failed with `HdfsError: Invalid path name`: HDFS does not allow `:` in paths, and Airflow's log path contains the run ID with its timestamp. A custom `log_filename_template` turns the colons into dashes.
- Setting `dag_processor_log_target` to `stdout` only duplicated the DAG processor's parse logs (the local file is written anyway, and Airflow cannot upload those), so it was left at the default.

Verified: the check DAG succeeded, both task logs were in HDFS with no local copy left, and the REST API used by the UI returned the full log from HDFS.

A teammate's commits had meanwhile changed the dev setup (one DataNode, replication 1 in dev; the `ha` profile for stage/prod; smaller heaps). Compose then wanted to recreate Kafka and the NameNode, which the host's AppArmor problem blocks, so only `airflow` was recreated (`docker compose up -d --no-deps airflow`). The unused `bigdata_airflow-logs` volume, holding only test logs, was removed.

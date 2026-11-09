# 2026-11-09 — HDFS, Cassandra, and startup scripts

Conversation with GitHub Copilot. User messages are verbatim; assistant replies are condensed.

---

**User:** okay, now we have kafka setup, could we do in similar fashion HDFS and cassandra? ask me if uncertain

**Assistant:** Added HDFS and Cassandra to the Docker Compose stack in the same style as Kafka. HDFS uses one NameNode and one DataNode with replication 1; Cassandra is a single node. Added the HDFS directory initialization script and documented the stage/prod data paths, required directory ownership, startup, and checks. The local stack was tested, including HDFS read/write and Cassandra connectivity.

---

**User:** can we get rid of init containers? for the sake of startup scripts?

**Assistant:** Removed the Kafka and Cassandra init containers and added executable, idempotent scripts: `deploy/kafka_init.sh`, `deploy/cassandra_init.sh`, and `deploy/init.sh`. The aggregate script also runs the HDFS initializer. Updated the log generator to wait for Kafka and buffer events until its topic is created, and updated the README with the explicit startup steps. On a fresh dev stack, `deploy/init.sh` succeeded twice and generated events reached Kafka. The stack was shut down and its test volumes removed.

---

**User:** can you write me a pr description in a format given in repo?

**Assistant:** Read `.github/pull_request_template.md` and drafted a PR description using its Summary, Changes, Usage, Notes, and Testing sections.

---

**User:** from the beggining of this conversation and as a copiable text, also create a new entry in the conversations in the same format

**Assistant:** Expanded the PR description to cover the HDFS and Cassandra setup as well as replacing init containers with startup scripts. Saved this conversation in the existing dated conversation-log format.

## Copy-ready PR description

## Summary

Extend the Kafka stack with HDFS for raw-data storage and Cassandra for serving, then replace Kafka and Cassandra init containers with explicit, repeatable startup scripts.

## Changes

- Add an HDFS NameNode and DataNode, with persistent storage, replication 1, and the HDFS web UI.
- Add a single-node Cassandra service with persistent storage and the `cran_analytics` keyspace.
- Add `deploy/hdfs_init.sh` to wait for HDFS readiness and create the raw-data directories.
- Add `deploy/kafka_init.sh` and `deploy/cassandra_init.sh` to create the Kafka topics and Cassandra keyspace.
- Add `deploy/init.sh` to run all three initialization scripts in sequence.
- Remove the Kafka and Cassandra init containers. The dev log generator now waits for Kafka and buffers events until its topic exists.
- Document setup, initialization, data directories, and verification commands in the README and environment files.

## Usage

```sh
# Dev
docker compose --profile dev up -d --build
deploy/init.sh

# Stage or prod (create and set ownership of the data directories first; see README)
docker compose --env-file deploy/prod.env up -d
deploy/init.sh --env-file deploy/prod.env
```

## Notes

- Initialization is now an explicit step after `docker compose up`; the scripts are idempotent and accept Docker Compose arguments such as `--env-file`.
- The Hadoop image is amd64-only, so Apple Silicon development machines run it under emulation.
- Cassandra is published without authentication; restrict access to the configured bind address and network.

## Testing

- **Dev:** Started the stack, ran `deploy/init.sh` twice successfully, verified Kafka events were produced, and checked HDFS file read/write, the NameNode UI, and Cassandra connectivity. Also verified HDFS data and the Cassandra keyspace persisted across service restarts.
- **Stage/prod:** Not deployed. Verified the Compose configuration parses with both `deploy/stage.env` and `deploy/prod.env`.

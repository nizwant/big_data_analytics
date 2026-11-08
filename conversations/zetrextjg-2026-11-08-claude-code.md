# 2026-11-08 — Kafka and log generator health check

Conversation with Claude Code. User messages are verbatim; assistant replies are condensed.

---

**User:** Can you check that kafka running in the docker is healthy and the mirror-log-generator works producing events in the cran_mirror_logs topic

**Assistant:** Inspected the running containers and the compose file, then checked the topic directly.

- `bigdata-kafka-1` is up, Docker healthcheck is `healthy` with no failures, listening on `127.0.0.1:9092`.
- `bigdata-kafka-init-1` exited with code 0 after creating `cran_mirror_logs`, `github_repo_updates` and `github_repo_list` (3 partitions each).
- `bigdata-mirror-log-generator-1` is up and logs that it is sending random lines from `/data/cran_logs_1h.txt` (7,420 lines) at about 4 events/s.
- Topic offsets across the 3 partitions grew from 641 to 668 in 5 seconds (about 5 events/s), evenly balanced between partitions.
- Consuming 3 messages from the beginning returned valid Apache access-log lines.
- The only warning was Kafka's harmless note about mixing `.` and `_` in topic names.


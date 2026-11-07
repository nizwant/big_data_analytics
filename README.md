# CRAN Poland Mirror Analytics

This project proposes an analytics system for Poland's CRAN mirror (`cran.m2.ai`). It correlates package downloads with GitHub releases to track R package demand and adoption across the Polish data science ecosystem.

## Planned capabilities

- Monitor popular packages, download surges, and mirror health, including HTTP errors and bot/CI traffic.
- Measure the time between GitHub releases and download peaks, and track version migration.
- Match CRAN packages to their upstream repositories using the `r-universe` `crantogit` mapping.

See [Milestone 1](deliverables/Milestone_1.pdf) for the project proposal and architecture.

## Running

The stack includes Kafka 4.3.2 (single-node KRaft), and a one-shot topic initializer. It creates the topics `cran_mirror_logs`, `github_repo_updates`, and `github_repo_list`.

**Dev** (also starts the fake log generator, which sends about 4 events/s to `cran_mirror_logs` topic):

```sh
docker compose --profile dev up -d --build
```

**Stage/prod** (Kafka data in `/srv/bigdata/kafka`; the container runs as uid 1000):

```sh
sudo chown -R 1000:1000 /srv/bigdata/kafka
docker compose --env-file deploy/prod.env up -d    # or deploy/stage.env
```

Kafka listens on `<vm-ip>:9092` without authentication.

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_DATA_DIR` | `kafka-data` volume | Kafka data path |
| `BIND_ADDRESS` | `127.0.0.1` | Host interface for port 9092 |
| `KAFKA_EXTERNAL_HOST` | `localhost` | Host advertised to clients outside Docker |

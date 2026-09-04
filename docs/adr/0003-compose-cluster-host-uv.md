# ADR-0003: Compose cluster and host uv

## Status

Accepted

## Context

Spark, HDFS, YARN, Hive, and Kafka differ across developer machines. Host JDK and host Spark installs break the “same commands on three OSes” goal. Ingest and the Kafka producer still need a light Python toolchain on the host.

## Decision

Split runtimes:

- **Host:** `uv` and Python 3.11 (`>=3.11,<3.12`) for ingest, Kafka producer, tests, and lint.
- **Cluster:** Docker Compose only for HDFS, Kafka, Spark (master + worker), Hive, and Superset.
- Submit Spark with `docker compose exec` against `spark://spark:7077`. Do not use host `local[*]`.
- Package MapReduce with the Maven Docker image (`make mr-package`). No host JDK requirement.
- Use **kafka-python-ng** (no librdkafka) and **kagglehub** (no Kaggle CLI).
- CI runs `uv sync`, ruff, and pytest on Ubuntu, macOS, and Windows. CI does not boot Compose.

Kafka listeners: Compose clients use `kafka:9092`; host clients use `localhost:9094`.

## Consequences

- Docs and Make targets assume Compose is up for Spark, Hive, and MR submit paths.
- Staged data under `./data` must be visible inside Spark at `/opt/data` (mounts and permissions). That path sync is a spike, not a second Spark install.
- Changing pinned image tags should happen as a set.

## Sources

`docs/agents/scaffold.md`, `docs/dev-modes.md`

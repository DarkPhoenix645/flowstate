# ADR-0003: Compose cluster and host uv

## Status

Accepted. Revised 2026-10-24 (YARN services). Revised 2026-10-28 (`runner`). Postgres: [ADR-0018](0018-postgres-for-model-config.md).

## Context

Spark, HDFS, YARN, Hive, and Kafka differ across developer machines. Host JDK and host Spark installs break the “same commands on three OSes” goal. Ingest and the Kafka producer still need a light Python toolchain on the host.

## Decision

Split runtimes:

- **Host:** `uv` and Python 3.11 (`>=3.11,<3.12`) for ingest, Kafka producer, tests, and lint.
- **Cluster:** Docker Compose only for HDFS, YARN (ResourceManager + NodeManager), Kafka, Spark (master + worker), Hive, Grafana, Postgres, and `runner`.
- `runner` publishes `127.0.0.1:8091`. The image has the Spark 3.5.6 client, the Hadoop 3.2.1 client, and Beeline. It mounts `./src` at `/opt/src` and uses `HADOOP_CONF_DIR=/etc/hadoop`. The host API calls it over HTTP. User-facing runs do not use `docker compose exec`.
- Postgres holds model config, run config, benchmarks, and run records ([ADR-0018](0018-postgres-for-model-config.md)). Mobility data stays on disk and HDFS. Bind `127.0.0.1:5432`.
- Submit Spark with `docker compose exec` against `spark://spark:7077`. Do not use host `local[*]`.
- Spark master and worker mount `infra/hive/core-site.xml` and `infra/hive/hdfs-site.xml`. `HADOOP_CONF_DIR` is `/etc/hadoop` ([ADR-0014](0014-hadoop-client-conf-at-etc-hadoop.md)). That sets `fs.defaultFS=hdfs://namenode:9000` and `dfs.replication=1`. A Spark write without that conf uses replication 3 and fails on one datanode.
- Batch jobs read `hdfs://namenode:9000/lake/...`. Do not treat `/opt/data` as the batch input. Keep the `./data` mount at `/opt/data` until #22 moves the stream-sink onto HDFS.
- Package MapReduce with the Maven Docker image (`make mr-package`). No host JDK requirement.
- Submit the YARN probe with `make yarn-probe` inside the `resourcemanager` container ([ADR-0015](0015-yarn-mapreduce-submit.md)). Spark stays `spark://spark:7077`.
- Use **kafka-python-ng** (no librdkafka) and **kagglehub** (no Kaggle CLI).
- CI runs `uv sync`, ruff, and pytest on Ubuntu, macOS, and Windows. CI does not boot Compose.

Kafka listeners: Compose clients use `kafka:9092`; host clients use `localhost:9094`.

## Consequences

- Docs and Make targets assume Compose is up for Spark, Hive, and MR submit paths.
- Recreate the Spark containers after this conf change. `HADOOP_CONF_DIR` is an environment value. A running container from before the change does not pick it up.
- Changing pinned image tags should happen as a set.

## Sources

`docs/agents/scaffold.md`, `docs/dev-modes.md`, `docs/work-plan.md`, [ADR-0009](0009-grafana-city-dashboard.md), [ADR-0014](0014-hadoop-client-conf-at-etc-hadoop.md), issue #7

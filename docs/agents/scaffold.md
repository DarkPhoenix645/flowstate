# Scaffold notes (for agents)

## Toolchain (locked)

- **Make**, not Just. Windows: Git Bash or WSL.
- **uv**, Python **3.11** (`>=3.11,<3.12`) for host ingest/producer/tests.
- **Docker Compose** for HDFS, Kafka, Spark (master + worker), Hive, Superset. No host Hadoop/Spark/JDK.
- Spark jobs: `docker compose exec spark spark-submit --master spark://spark:7077 …`
- MapReduce package: `maven:3.9-eclipse-temurin-8` (`make mr-package`).
- **kafka-python-ng** — no librdkafka.
- **kagglehub** — no Kaggle CLI.
- CI: Ubuntu + macOS + Windows (`uv sync`, ruff, pytest). Does not boot Compose.

## Layout

```
flowstate/
├── pyproject.toml
├── Makefile
├── docker-compose.yml
├── .python-version
├── docs/                 # humans
├── docs/agents/          # specs
├── infra/hadoop|kafka|hive
├── src/flowstate/{ingest,batch,streaming,warehouse,benchmarks}
├── dashboards/{superset,explore}
├── tests/
└── data/                 # gitignored bodies; README only
```

Stubs print `TODO:` / throw `UnsupportedOperationException`. Do not fill in job logic unless asked.

## Compose caveats

- Kafka dual listeners: Compose `kafka:9092`, host `localhost:9094`.
- Spark master + worker are in compose; do not add `local[*]` on the host.
- Hive 4 + Derby + two Hive containers is fragile; warehouse-on-HDFS is a spike.
- Pin tags already; bump together when changing.
- `data/lake` mounted on namenode at `/mnt/lake` for optional copy-in; real lake should be HDFS.

## Hatch / imports

Package lives at `src/flowstate`. pytest `pythonpath = ["src"]`. Host `make` uses `uv run python -m flowstate.*`. Spark containers set `PYTHONPATH=/opt/src` and `FLOWSTATE_DATA_DIR=/opt/data`.

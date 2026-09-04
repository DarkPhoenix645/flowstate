# Scaffold notes (for agents)

Locked toolchain and runtime: [ADR-0003](../adr/0003-compose-cluster-host-uv.md), [ADR-0004](../adr/0004-make-task-runner.md).

## Layout

```
flowstate/
├── pyproject.toml
├── Makefile
├── docker-compose.yml
├── .python-version
├── CONTEXT.md
├── docs/                 # humans + adr/ + agents/
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
- Hive 4 + Derby + two Hive containers is fragile; warehouse-on-HDFS is a spike ([spikes.md](spikes.md) #3).
- Pin tags already; bump together when changing.
- `data/lake` mounted on namenode at `/mnt/lake` for optional copy-in; real lake should be HDFS.
- Spark submit form: `docker compose exec spark spark-submit --master spark://spark:7077 …`
- MapReduce package image: `maven:3.9-eclipse-temurin-8` (`make mr-package`).

## Hatch / imports

Package lives at `src/flowstate`. pytest `pythonpath = ["src"]`. Host `make` uses `uv run python -m flowstate.*`. Spark containers set `PYTHONPATH=/opt/src` and `FLOWSTATE_DATA_DIR=/opt/data`.

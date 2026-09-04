# Pre-meet briefing (condensed)

Use this file only as the team-meet dump. Other docs stay owner-free, date-free, and Docker-Spark-only.

Source: CityPulse org review + FlowState Lambda plan. Human version: `docs/architecture.md` + `docs/team.md`.

## Expectations

CityPulse GitHub org ≠ something you deploy. Architecture reference only.

## Bottom line

- Mirror CityPulse **component boundaries** in Kafka/Spark/Hive; cite it in the report.
- Scaffold is uv (host ingest) + Compose (all compute) + **Make** + 3-OS CI so every OS looks the same in <15 minutes (`make setup && make up`).
- No host Spark / no host JDK. Spark submit and Maven package run in containers.
- Riskiest unknowns: Hive-on-HDFS-in-Docker and YARN MapReduce — spikes first.

## Original justfile → Make

Recipes mapped to Make (`setup`, `ingest`, `amplify`, `hotspots`, `up`/`down`, `stream-produce`, `stream-consume`, `hive-ddl`, `mr-package`, `test`, `lint`). Variables: `DATASET`, `ROWS`, `TOPIC`, `RATE`. `hotspots` / `stream-consume` exec into Compose Spark, not `local[*]`.

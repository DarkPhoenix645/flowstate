# CONTEXT

FlowState is a Lambda pipeline on Indian urban mobility data. CityPulse (EU FP7) is the architecture reference. The stack is Kafka, Spark, HDFS, and Hive.

Agents: read this glossary before you invent synonyms. Read ADRs under `docs/adr/` before you change a locked choice. Spec detail lives in `docs/agents/`.

## Glossary

| Term | Meaning | Avoid |
|------|---------|--------|
| ride event | One mobility record that matches the canonical ride schema | “row”, “message”, “trip record” as competing names for the same contract |
| canonical ride schema | Shared columns in `src/flowstate/schema.py` / `docs/agents/schema.md` | Per-loader private schemas for Kafka |
| staged parquet | Ingest output under `data/staged/<dataset>/` before lake load | “raw dump”, “CSV lake” |
| data lake | Mobility data on HDFS (Compose namenode); host `data/` is a mount/staging aid | Treating only local `./data` as the lake |
| amplify | Resample seed rides to 1M / 10M / 100M while keeping hour-of-day and cancel rates | “generate fake random rides” without seed fidelity |
| batch layer | Spark batch jobs + MapReduce baseline over the lake | Calling streaming windows “batch” |
| speed layer | Kafka replay + Spark Structured Streaming rolling metrics | Host-local Spark streaming |
| warehouse | Hive tables over the lake, partitioned by `city` and `vehicle_type` | Ad-hoc SQL without partitions |
| CityPulse | EU FP7 reference system (AMQP / RDF / Java). Cite boundaries; do not run their repos | “CityPulse fork”, “port CityPulse code” |
| workstream | Fixed path area (`ingest/`, `batch/`, …), not a named owner | Forking layouts per person |

## Where decisions live

| Kind | Path |
|------|------|
| Accepted architecture choices | `docs/adr/` |
| Contracts, spikes, CityPulse notes | `docs/agents/` |
| Human onboarding | `docs/getting-started.md`, `docs/architecture.md`, `docs/team.md` |

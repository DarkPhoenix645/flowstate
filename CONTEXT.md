# CONTEXT

FlowState is a Lambda pipeline on Indian urban mobility data. CityPulse (EU FP7) is the architecture reference. The stack is Kafka, Spark, HDFS, Hive, and Grafana.

Agents: read this glossary before you invent synonyms. Read ADRs under `docs/adr/` before you change a locked choice. Spec detail lives in `docs/agents/`.

## Glossary

| Term | Meaning | Avoid |
|------|---------|--------|
| ride event | One mobility record that matches the canonical ride schema. `mode` is ride_hail, metro, bus, or aviation | Per-mode private Kafka contracts for the Bengaluru resampler stream |
| canonical ride schema | Shared columns in `src/flowstate/schema.py` / `docs/agents/schema.md` | Per-loader private schemas for Kafka |
| staged parquet | Ingest output under `data/staged/<dataset>/` before lake load | “raw dump”, “CSV lake” |
| data lake | Mobility data on HDFS (Compose namenode); host `data/` is a mount/staging aid | Treating only local `./data` as the lake |
| amplify | `make amplify` / `N` multimodal events on the lake from the resampler | A second random generator beside the resampler |
| resampler | Generator that draws Bengaluru events for all four modes from models calibrated to observed aggregate clocks; one process feeds the lake and Kafka | Taxi-only generator; separate batch vs stream samplers; training on NCR taxis; a neural forecaster (RNN/Hawkes are future candidates, not v1) |
| scenario | An injected command that forks a conditional synthetic stream; its response is assumption-driven unless a held-out test validates it | Calling a scenario response a learned causal effect |
| provenance status | The label every event carries: `observed`, `observed_proxy`, `derived`, or `synthetic` | Presenting synthetic or proxy records as observed trips |
| origin-destination | Counts from one place to another. The short form is OD. | One passenger trip. The licence short form ODbL |
| Bengaluru alignment window | Shared clock for Bengaluru events: 2024-10-26 through 2026-04-15 (the ride-hail clock ends there) | Joining Jan/Jul 2024 Bengaluru bookings or NCR taxis to metro days as if they overlap |
| batch layer | Spark batch jobs + MapReduce baseline over the lake | Calling streaming windows “batch” |
| speed layer | Kafka emit + Spark Structured Streaming rolling metrics | Host-local Spark streaming; “replay” as the only emit path; calling the stream observed ridership |
| warehouse | Hive tables over the lake, partitioned by `city` and `vehicle_type` | Ad-hoc SQL without partitions |
| CityPulse | EU FP7 reference system (AMQP / RDF / Java). Cite boundaries; do not run their repos | “CityPulse fork”, “port CityPulse code” |
| workstream | Fixed path area (`ingest/`, `batch/`, …), not a named owner | Forking layouts per person |

## Where decisions live

| Kind | Path |
|------|------|
| Accepted architecture choices | `docs/adr/` |
| Contracts, spikes, CityPulse notes | `docs/agents/` |
| Human onboarding | `docs/getting-started.md`, `docs/architecture.md`, `docs/team.md` |

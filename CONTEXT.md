# CONTEXT

FlowState is a Lambda pipeline on Indian urban mobility data. CityPulse (EU FP7) is the architecture reference. The stack is Kafka, Spark, HDFS, Hive, and Grafana.

Agents: read this glossary before you invent synonyms. Read ADRs under `docs/adr/` before you change a locked choice. Spec detail lives in `docs/agents/`.

## Glossary

| Term                       | Meaning                                                                                                                                            | Avoid                                                                                                                                         |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| ride event                 | One mobility record that matches the canonical ride schema. `mode` is ride_hail, metro, bus, or aviation                                           | Per-mode private Kafka contracts for the Bengaluru resampler stream                                                                           |
| canonical ride schema      | Shared columns in `src/flowstate/schema.py` / `docs/agents/schema.md`                                                                              | Per-loader private schemas for Kafka                                                                                                          |
| staged parquet             | Ingest output under `data/staged/<dataset>/` before lake load                                                                                      | “raw dump”, “CSV lake”                                                                                                                        |
| data lake                  | Mobility data on HDFS (Compose namenode); host `data/` is a mount/staging aid                                                                      | Treating only local `./data` as the lake                                                                                                      |
| amplify                    | `make amplify` / `N` multimodal events: a thin copy of the alignment window from the resampler (ADR-0013)                                          | A second random generator; a handful of full source days                                                                                      |
| resampler                  | Generator that draws Bengaluru events for all four modes from models calibrated to observed aggregate clocks; one process feeds the lake and Kafka | Taxi-only generator; separate batch vs stream samplers; training on NCR taxis; a neural forecaster (RNN/Hawkes are future candidates, not v1) |
| model bundle               | The versioned fit the resampler emits from: NB2 clocks, profiles, config, coverage manifest, and the frozen evaluation record (`data/staged/rides_amplified/bundle.json`) | An unversioned model dump; per-run refits that overwrite decisions |
| model config               | The scenario policy and the resample config for a run. Postgres stores immutable versions. The process reads the active row of each kind (ADR-0018) | A file or env fallback; Derby used as this store |
| config version             | One immutable `config_version` row. An edit adds a row. `config_active` names the row in force for that kind | Updating a stored version in place |
| scenario                   | An injected command that forks a conditional synthetic stream; its response is assumption-driven unless a held-out test validates it               | Calling a scenario response a learned causal effect                                                                                           |
| replay session             | One run of the long-running producer from `start` to `stop`. It owns the play head and at most one fork (ADR-0017)                                | Restarting the producer to scrub; two forks in one replay |
| run id                     | `<session>.<epoch>` on every event. A seek backwards starts a new epoch, so consumers read the newest run id and treat the others as history | Treating a later `event_time` as a late arrival after a scrub |
| covered day                | An OpenSky day-direction with no failed window and a count of at least half the 75th percentile of its direction. Only covered days train the aviation fit | Reading a thin fetch window as a low-traffic day |
| KIA catchment              | The seven ward ids whose centroid is within 17.85 km of the VOBL pin. They receive the synthetic flight-response rides (ADR-0017)                  | A claim about where airport passengers live or travel |
| provenance status          | The label every event carries: `observed`, `observed_proxy`, `derived`, or `synthetic`                                                             | Presenting synthetic or proxy records as observed trips                                                                                       |
| origin-destination         | Counts from one place to another. The short form is OD.                                                                                            | One passenger trip. The licence short form ODbL                                                                                               |
| Bengaluru alignment window | Shared clock for Bengaluru events: 2024-10-26 through 2026-04-15 (the ride-hail clock ends there)                                                  | Joining Jan/Jul 2024 Bengaluru bookings or NCR taxis to metro days as if they overlap                                                         |
| batch layer                | Spark batch jobs + MapReduce baseline over the lake                                                                                                | Calling streaming windows “batch”                                                                                                             |
| speed layer                | Kafka emit + Spark Structured Streaming rolling metrics                                                                                            | Host-local Spark streaming; “replay” as the only emit path; calling the stream observed ridership                                             |
| warehouse                  | Hive tables over the lake, partitioned by `city` and `vehicle_type`                                                                                | Ad-hoc SQL without partitions                                                                                                                 |
| CityPulse                  | EU FP7 reference system (AMQP / RDF / Java). Cite boundaries; do not run their repos                                                               | “CityPulse fork”, “port CityPulse code”                                                                                                       |
| workstream                 | Fixed path area (`ingest/`, `batch/`, …), not a named owner                                                                                        | Forking layouts per person                                                                                                                    |

## Where decisions live

| Kind                                  | Path                                                              |
| ------------------------------------- | ----------------------------------------------------------------- |
| Accepted architecture choices         | `docs/adr/`                                                       |
| Forward work (lake layout, consumers) | `docs/work-plan.md`                                               |
| Contracts, spikes, CityPulse notes    | `docs/agents/`                                                    |
| Human onboarding                      | `docs/getting-started.md`, `docs/architecture.md`, `docs/team.md` |

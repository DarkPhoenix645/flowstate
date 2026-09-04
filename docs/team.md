# Workstreams

Areas to staff. Paths are fixed; do not fork layouts.

## Data & lake — `src/flowstate/ingest/`

- kagglehub for ride-hailing CSVs; BMRCL HTML; data.gov.in XLSX; GTFS zip.
- Each loader writes **standardized parquet** under `data/staged/` (schema: [agents/schema.md](agents/schema.md)).
- `amplify.py`: synthetic 1M / 10M / 100M, seed-faithful (hour-of-day, cancel rates).
- Done when: `make ingest && make amplify` materializes the lake. Document veracity per dataset.
- Spike: Hive-on-HDFS warehouse dir (early).

## Batch baseline & benchmarks — `src/flowstate/batch/mapreduce/`, `src/flowstate/benchmarks/`

- Java MR jobs equivalent to Spark aggregations (zone × hour, cancellation counts). Maven jar (`make mr-package`), YARN submit.
- `runner.py`: Spark vs MR at 1M/10M/100M, multiple runs, CSV in `benchmarks/results/` (**commit timings**).
- Spike: YARN submission on bde2020 images.

## Spark analytics — `src/flowstate/batch/spark_jobs/`

- Descriptive: demand heatmaps, booking-value by vehicle, metro trends, GTFS, DGCA share.
- Diagnostic: cancellation root-cause, cross-modal correlation, aviation seasonality.
- One file, one `main()`, config-driven paths. Submit with `make hotspots` (Compose Spark), not host `spark-submit`.
- Spike: City-Dashboard historical vs live split (for later Superset tabs).

## Speed layer — `src/flowstate/streaming/`

- `producer.py`: replay staged rides into `rides.raw`, `--events-per-sec`, event-time interpolation.
- `rolling_metrics.py`: 1-min tumbling + 5-min sliding, `watermark("timestamp", "10 minutes")` → active rides / zone, cancel rate, surge flag. Console + parquet sink. Submit on Compose Spark (`make stream-consume`).
- `ride_event_schema.py`: one JSON schema for producer and consumer.
- Spike: Event-Detector "one metric node per job"; watermarks / late data on replay.

## Warehouse & dashboards — `warehouse/`, `dashboards/`

Hive `PARTITIONED BY (city, vehicle_type)`, load scripts, Superset JDBC to HiveServer2 + live tab from stream sink, report with benchmark plot + CityPulse citation.

## Milestones

| Milestone | Done means |
|---|---|
| Env setup | Repo usable; `make setup && make test` green; CI matrix; ingest stub runs; `make up` healthy |
| Batch MVP | Lake in HDFS; 2 Spark jobs + 1 MR on the stack; first benchmark row |
| Streaming | Producer ≥1k events/s; rolling metrics on replay; live output visible |
| Benchmarks + UI | 1M/10M/100M table + charts; Hive from Superset; dashboard; report draft |

## Spikes (~half day each)

| Spike | Area |
|---|---|
| Event-Detector node pattern | Speed layer |
| City-Dashboard live vs historical | Spark analytics / dashboards |
| Hive + HDFS warehouse dir in Compose | Data & lake |
| YARN + Maven MR on bde2020 | Batch baseline |
| Watermarks / event-time on Kafka replay | Speed layer |
| Host-staged parquet readable from Spark containers | Data & lake / Spark analytics |

Notes for agents: [agents/spikes.md](agents/spikes.md), [agents/workstreams.md](agents/workstreams.md).

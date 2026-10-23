# Workstreams

Areas to staff. Paths are fixed; do not fork layouts.

## Data & lake — `src/flowstate/ingest/`

- Bengaluru sources in [data-sources.md](data-sources.md): Namma Yatri, Namma Metro, BMRCL RTI, BMTC GTFS, DGCA, OpenSky.
- Each loader writes **standardized parquet** under `data/staged/` (schema: [agents/schema.md](agents/schema.md)).
- `amplify.py`: write 1M / 10M / 100M Bengaluru multimodal events from the calibrated resampler ([ADR-0010](adr/0010-bengaluru-calibrated-resampler.md)).
- Done when: `make ingest && make amplify` materializes the lake. Document veracity per dataset.
- Hive warehouse directory: `hdfs://namenode:9000/user/hive/warehouse`.

## Batch baseline & benchmarks — `src/flowstate/batch/mapreduce/`, `src/flowstate/benchmarks/`

- Java MR jobs equivalent to Spark aggregations (zone × hour, cancellation counts). Maven jar (`make mr-package`), YARN submit.
- `runner.py`: Spark vs MR at 1M/10M/100M, multiple runs, CSV in `benchmarks/results/` (**commit timings**).
- YARN submit: `make yarn-probe` ([ADR-0015](adr/0015-yarn-mapreduce-submit.md)). DemandCount is #24.

## Spark analytics — `src/flowstate/batch/spark_jobs/`

- Descriptive: demand heatmaps, booking-value by vehicle, metro trends, GTFS, DGCA share.
- Diagnostic: cancellation root-cause, cross-modal correlation, aviation seasonality.
- One file, one `main()`, config-driven paths. Submit with `make hotspots` (Compose Spark), not host `spark-submit`.
- Spike: City-Dashboard historical vs live split (for later Grafana views).

## Speed layer — `src/flowstate/streaming/`

- `producer.py`: emit the same resampler on the [ADR-0008](adr/0008-speed-layer-streaming-shape.md) topics, `--events-per-sec`.
- `rolling_metrics.py`: active rides / zone only. `make stream-consume`. Cancel rate and surge are later files. Layout: [agents/event-detector.md](agents/event-detector.md). Policy: [agents/watermarks.md](agents/watermarks.md).
- `ride_event_schema.py`: one JSON schema for producer and consumer.
- Watermark policy is written (#11). Human sign-off still owns close.

## Warehouse & dashboards — `warehouse/`, `dashboards/`

Hive `PARTITIONED BY (city, vehicle_type)`, load scripts, Grafana over HiveServer2 for batch views plus live views from the stream sink, report with benchmark plot + CityPulse citation.

## Milestones

| Milestone       | Done means                                                                                   |
| --------------- | -------------------------------------------------------------------------------------------- |
| Env setup       | Repo usable; `make setup && make test` green; CI matrix; ingest stub runs; `make up` healthy |
| Batch MVP       | Lake in HDFS; 2 Spark jobs + 1 MR on the stack; first benchmark row                          |
| Streaming       | Producer ≥1k events/s; rolling metrics on replay; live output visible                        |
| Benchmarks + UI | 1M/10M/100M table + charts; Hive from Grafana; dashboard; report draft                       |

## Spikes (~half day each)

| Spike                                   | Area                          |
| --------------------------------------- | ----------------------------- |
| Event-Detector node pattern             | Speed layer                   |
| City-Dashboard live vs historical       | Spark analytics / dashboards  |
| Hive + HDFS warehouse dir in Compose    | Data & lake                   |
| YARN + Maven MR on bde2020              | Batch baseline                |
| Watermarks / event-time on Kafka replay | Speed layer                   |
| Spark reads the HDFS lake               | Data & lake / Spark analytics |

Notes for agents: [agents/spikes.md](agents/spikes.md), [agents/workstreams.md](agents/workstreams.md).

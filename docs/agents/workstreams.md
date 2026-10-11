# Workstreams (detail)

Human summary: [../team.md](../team.md). Stack decisions: [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md). Areas, not assigned people.

## Data & lake `ingest/`

Files: `namma_yatri.py`, `namma_ridership.py`, `bmrcl_rti.py`, `bmtc_gtfs.py`, `dgca_aviation.py`, `opensky_arrivals.py`, `significant_dates.py`, `manifest.py`. Dispatcher: `python -m flowstate.ingest --dataset all|ridehail|metro|bus|aviation|events|manifest|validate`.

Each downloader/validator owns its ADR-0011 source and refreshes `flowstate-datasets/provenance/manifest.json`. Staged parquet per source is the T12–T14 scope. Source registry and windows: [../data-sources.md](../data-sources.md). Rejected sources: [../rejected-sources.md](../rejected-sources.md).

Amplify CLI: `amplify.py --rows N --seed S` with `N` in `{1e6, 1e7, 1e8}`. Multimodal events come from the calibrated resampler ([ADR-0010](../adr/0010-bengaluru-calibrated-resampler.md)). Scale targets: [ADR-0006](../adr/0006-amplify-for-scale-benchmarks.md).

Done when `make ingest && make amplify` fills the lake. Per-dataset veracity notes (missing values, mixed formats).

Schema contract: [schema.md](schema.md).

## Batch baseline `batch/mapreduce/`, `benchmarks/`

Decision: [ADR-0007](../adr/0007-mapreduce-spark-benchmark-pair.md).

Maven module `src/flowstate/batch/mapreduce/` (`pom.xml`, Hadoop 3.2.1 `provided`, matches bde2020 3.2.1). Classes: `DemandCountMapper`, `DemandCountReducer`, `DemandCountDriver`. Package: `make mr-package`.

`benchmarks/runner.py` → `benchmarks/results/timings.csv` (committed).

## Spark analytics `batch/spark_jobs/`

One job per file, `main()`, paths from `flowstate.config.Settings`. Files: `demand_hotspots.py`, `cancellation_analysis.py`, `cross_modal.py`, `ridership_trends.py`. Submit on Compose Spark only (ADR-0003).

## Speed layer `streaming/`

Decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md) (topic contract, scenario engine), [ADR-0010](../adr/0010-bengaluru-calibrated-resampler.md).

`producer.py` on the host: conditionally stimulable, emits the `flowstate.*.v1` topics with the provenance envelope. Event time, `mode`, and mix come from the same resampler as amplify. `rolling_metrics.py` on Compose Spark. Metrics in scaffold: active rides/zone, cancel rate, surge. Surge framing is scenario-based: a flight-injection demo changes a configured synthetic airport-access response marked `assumption_driven` unless a held-out test validates it.

## Warehouse & UI

`warehouse/ddl/*.sql`, `warehouse/load.py`, `dashboards/grafana/`, `dashboards/explore/`. Partitions: ADR-0005. Dashboards: ADR-0009.

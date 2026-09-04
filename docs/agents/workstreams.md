# Workstreams (detail)

Human summary: [../team.md](../team.md). Areas, not assigned people.

## Data & lake `ingest/`

Files: `kaggle_rides.py`, `bmrc_ridership.py`, `bmtc_gtfs.py`, `mmrda_ridership.py`, `dgca_aviation.py`, `amplify.py`. Dispatcher: `python -m flowstate.ingest --dataset all|kaggle|…`.

Each `run(settings)` writes parquet under `settings.staged_dir / <name>/`. kagglehub + `KAGGLE_USERNAME`/`KAGGLE_KEY`. No per-OS Kaggle CLI.

`amplify.py --rows N --seed S`: resample seed rows to N in `{1e6, 1e7, 1e8}`. This is the "big data" scaling story.

Done when `make ingest && make amplify` fills the lake. Per-dataset veracity notes (missing values, mixed formats).

## Batch baseline `batch/mapreduce/`, `benchmarks/`

Maven module `src/flowstate/batch/mapreduce/` (`pom.xml`, Hadoop 3.2.1 `provided`, matches bde2020 3.2.1). Classes: `DemandCountMapper`, `DemandCountReducer`, `DemandCountDriver`. Mirror Spark aggregations (zone×hour, cancellations). Package with `make mr-package` (Maven Docker image).

`benchmarks/runner.py` writes `benchmarks/results/timings.csv` (committed). Headline chart for the report.

## Spark analytics `batch/spark_jobs/`

One job per file, `main()`, paths from `flowstate.config.Settings`. Files: `demand_hotspots.py`, `cancellation_analysis.py`, `cross_modal.py`, `ridership_trends.py`. Submit on Compose Spark only.

## Speed layer `streaming/`

`producer.py --topic --events-per-sec` on the host. `rolling_metrics.py` on Compose Spark: 1-minute tumbling, 5-minute sliding, watermark 10 minutes; metrics: active rides/zone, cancel rate, surge (demand z-score vs batch baseline). `ride_event_schema.py` is the single schema.

RiverBench citypulse-traffic is the replay-pattern reference.

## Warehouse & UI

`warehouse/ddl/*.sql`, `warehouse/load.py`, `dashboards/superset/`, `dashboards/explore/`.

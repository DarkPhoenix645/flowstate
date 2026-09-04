# Workstreams (detail)

Human summary: [../team.md](../team.md). Stack decisions: [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md). Areas, not assigned people.

## Data & lake `ingest/`

Files: `kaggle_rides.py`, `bmrc_ridership.py`, `bmtc_gtfs.py`, `mmrda_ridership.py`, `dgca_aviation.py`, `amplify.py`. Dispatcher: `python -m flowstate.ingest --dataset all|kaggle|…`.

Each `run(settings)` writes parquet under `settings.staged_dir / <name>/`. Credentials: `KAGGLE_USERNAME` / `KAGGLE_KEY` via kagglehub (ADR-0003).

Amplify CLI: `amplify.py --rows N --seed S` with `N` in `{1e6, 1e7, 1e8}` — rationale in [ADR-0006](../adr/0006-amplify-for-scale-benchmarks.md).

Done when `make ingest && make amplify` fills the lake. Per-dataset veracity notes (missing values, mixed formats).

Schema contract: [schema.md](schema.md).

## Batch baseline `batch/mapreduce/`, `benchmarks/`

Decision: [ADR-0007](../adr/0007-mapreduce-spark-benchmark-pair.md).

Maven module `src/flowstate/batch/mapreduce/` (`pom.xml`, Hadoop 3.2.1 `provided`, matches bde2020 3.2.1). Classes: `DemandCountMapper`, `DemandCountReducer`, `DemandCountDriver`. Package: `make mr-package`.

`benchmarks/runner.py` → `benchmarks/results/timings.csv` (committed).

## Spark analytics `batch/spark_jobs/`

One job per file, `main()`, paths from `flowstate.config.Settings`. Files: `demand_hotspots.py`, `cancellation_analysis.py`, `cross_modal.py`, `ridership_trends.py`. Submit on Compose Spark only (ADR-0003).

## Speed layer `streaming/`

Decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md).

`producer.py --topic --events-per-sec` on the host. `rolling_metrics.py` on Compose Spark. Metrics in scaffold: active rides/zone, cancel rate, surge (demand z-score vs batch baseline).

## Warehouse & UI

`warehouse/ddl/*.sql`, `warehouse/load.py`, `dashboards/superset/`, `dashboards/explore/`. Partitions: ADR-0005.

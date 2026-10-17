# Architecture

FlowState is a Lambda pipeline on Indian urban mobility data. CityPulse (EU FP7, 2014–2017) is the reference architecture, not a code donor. Decisions: [ADR-0001](adr/0001-citypulse-reference-architecture-only.md), [ADR-0002](adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md).

```mermaid
flowchart LR
  src1["Namma Yatri"] --> ing["Ingest (uv, Python)"]
  src2["Namma Metro + BMRCL RTI"] --> ing
  src3["BMTC GTFS + DGCA + OpenSky"] --> ing
  ing --> hdfs["HDFS Data Lake (Docker)"]
  hdfs --> mr["Hadoop MapReduce baseline"]
  hdfs --> spark["Spark batch analytics"]
  mr --> bench["Benchmark 1M / 10M / 100M"]
  spark --> bench
  spark --> hive["Hive OLAP (city, vehicle_type)"]
  hive --> dash["Grafana"]
  replay["Kafka producer (calibrated resampler)"] --> kafka["flowstate.*.v1 topics"]
  kafka --> sss["Structured Streaming windows"]
  sss --> sink["Live metrics sink"]
  sink --> dash
  bench --> report["Final report"]
```

CityPulse → FlowState component map (Kafka topics, windows, Hive partitions, dashboards): **[ADR-0002](adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md)**. Repo ranking and reading list: [agents/citypulse.md](agents/citypulse.md).

## Runtime

Compose cluster + host uv: [ADR-0003](adr/0003-compose-cluster-host-uv.md). Operator detail: [dev-modes.md](dev-modes.md).

## Open infra (deliberate)

MapReduce on YARN in the bde2020 images is an open spike. See [team.md](team.md) and [agents/spikes.md](agents/spikes.md).

The Hive warehouse directory is `hdfs://namenode:9000/user/hive/warehouse`. Derby is the metastore database.

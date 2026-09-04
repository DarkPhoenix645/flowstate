# ADR-0002: Lambda pipeline on Kafka, Spark, HDFS, and Hive

## Status

Accepted

## Context

CityPulse used an AMQP bus, semantic/RDF streams, and related Java tooling. FlowState targets Indian urban mobility data and coursework-style big-data tooling that students can run in Docker.

The project needs one clear stack for the batch layer, the speed layer, the lake, and the warehouse.

## Decision

Implement a Lambda-style pipeline. CityPulse → FlowState mapping (normative for report wording):

| CityPulse concept | FlowState equivalent |
|---|---|
| Data bus (AMQP) | Kafka topics `rides.raw`, `rides.enriched` |
| Aggregated / annotated streams | Spark Structured Streaming rolling windows |
| Event detection nodes | One streaming job per metric (surge, active rides, cancellations) |
| GDI / historical store | HDFS data lake + Hive, partitioned by `city` / `vehicle_type` |
| Resource manager | YARN in the Hadoop Compose images |
| City Dashboard | Superset / Grafana over Hive (batch) + streaming sink (live) |
| KAT analytics | Spark batch diagnostics (heatmaps, cancellation root cause, cross-modal) |

Stack summary:

| Role | Technology |
|------|------------|
| Data bus | Kafka |
| Batch analytics | Spark batch jobs |
| Baseline batch | Hadoop MapReduce (ADR-0007) |
| Lake | HDFS |
| Warehouse | Hive |
| Live metrics | Spark Structured Streaming |
| Dashboards | Superset (+ explore) |

## Consequences

- New storage or bus choices need a new ADR. Do not add Postgres, Flink, or host-native clusters as the primary path without one.
- Hive warehouse-on-HDFS remains an open spike. Derby-local warehouse in the scaffold is deliberate until that spike lands.
- Cite CityPulse as real-time stream processing and large-scale data analytics (speed + batch). Do not run CityPulse repos (ADR-0001).

## Sources

`docs/architecture.md`, `docs/agents/citypulse.md`, `docs/agents/workstreams.md`

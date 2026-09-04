# CityPulse org — reading notes

Decision: [ADR-0001](../adr/0001-citypulse-reference-architecture-only.md). Component map: [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md).

Org: `github.com/orgs/CityPulse`. EU FP7 CityPulse (2014–2017): *"Real-Time IoT Stream Processing and Large-scale Data Analytics for Smart City Applications"*. ~27 public repos; Java, AMQP, RDF/semantic streams (Esper, SPARQL); Aarhus sensor data.

## Repos ranked for FlowState

| Repo | What | Relevance |
|---|---|---|
| **CityPulse-City-Dashboard** | Dashboard; historical + live | **High.** Live vs historical split for dashboards. |
| **Event-Detector** | Pluggable event-detection nodes on annotated/aggregated streams | **High.** One Structured Streaming metric ≈ one node. |
| **KAT 1.0 / 2.0** | ML/analytics over sensors | **Medium.** Conceptually Spark diagnostics. Little code reuse. |
| **CityPulse-3D-Map** | 3D traffic/parking/pollution/noise | **Low–Medium.** Map/heatmap inspiration only. |
| **Social-Media-Analyser** | Twitter | **Low.** Skip. |
| Resource-Manager, GDI, data-bus | Sensor registry, geo DB, AMQP | **Low code, high concept** — see ADR-0002. |

Also: **RiverBench/dataset-citypulse-traffic** — replay-shaped traffic stream reference for Kafka replay patterns.

## Reading (three items)

1. Event-Detector README — node pattern + aggregated/annotated flow.
2. City-Dashboard README — historical vs live.
3. Org framing quote for the report (batch = large-scale analytics, speed = real-time stream).

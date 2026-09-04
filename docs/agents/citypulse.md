# CityPulse org — what it is (not what to run)

Org: `github.com/orgs/CityPulse`. **EU FP7 CityPulse** (2014–2017): *"Real-Time IoT Stream Processing and Large-scale Data Analytics for Smart City Applications"*. 27 public repos, Java-heavy, AMQP buses, RDF/semantic streams (Esper, SPARQL), Aarhus sensor data.

**Do not plan to run their code.** Decade-old abandoned Java/AMQP/RDF. Almost zero overlap with HDFS/Spark/Kafka/Hive. Value is **architectural**: ingestion → data bus → real-time aggregation/event detection → historical store → dashboard. That is FlowState's shape.

## Repos ranked for FlowState

| Repo | What | Relevance |
|---|---|---|
| **CityPulse-City-Dashboard** | Dashboard; cleaned summarized historical for live + post-mortem | **High.** Study live vs historical split for the dashboard workstream. |
| **Event-Detector** | Pluggable event-detection nodes; continuous queries (Esper) on annotated/aggregated streams (traffic jam, parking change) | **High.** Each node ≈ one Structured Streaming metric. Pattern: one logic class per metric, registered on a bus. |
| **KAT 1.0 / 2.0** | ML/analytics over sensors | **Medium.** Conceptually Spark diagnostics (correlation, hotspots). Little code reuse. |
| **CityPulse-3D-Map** | 3D traffic/parking/pollution/noise | **Low–Medium.** Map/heatmap inspiration only (we use Superset/Grafana/Plotly). |
| **Social-Media-Analyser** | Twitter | **Low.** Skip. |
| Resource-Manager, GDI, data-bus repos | Sensor registry, geo DB, AMQP | **Low code, high concept:** bus ≈ Kafka; GDI ≈ HDFS+Hive; resource manager ≈ YARN. |

Also: **RiverBench/dataset-citypulse-traffic** — static CityPulse traffic republished as a replayable stream. Reference for "replay historical ride events through Kafka".

## Reading (three items)

1. Event-Detector README — node pattern + aggregated/annotated flow.
2. City-Dashboard README — historical vs live.
3. Org framing quote for the report (batch = large-scale analytics, speed = real-time stream).

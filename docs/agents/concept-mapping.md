# CityPulse → FlowState concept mapping

Use in the report.

| CityPulse concept | FlowState equivalent |
|---|---|
| Data bus (AMQP) | **Kafka** topics (`rides.raw`, `rides.enriched`) |
| Aggregated/annotated streams | Spark Structured Streaming **rolling windows** |
| Event detection nodes | One streaming job per metric (surge, active rides, cancellations) |
| GDI / historical store | **HDFS data lake + Hive** partitioned by city / vehicle type |
| Resource manager | **YARN** in the Hadoop Compose images |
| City Dashboard | **Superset/Grafana** over Hive (batch views) + streaming sink (live views) |
| KAT analytics | Spark batch diagnostics (heatmaps, cancellation root cause, cross-modal correlation) |

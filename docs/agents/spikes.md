# Spikes

Half-day each. Hive-on-HDFS and YARN-MR gate later milestones.

Related decisions (already accepted — spikes prove or refine them):

| Spike theme | ADR |
|---|---|
| One metric per streaming job / Event-Detector | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md) |
| Live vs historical dashboard | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md), [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md) |
| MapReduce on YARN | [ADR-0007](../adr/0007-mapreduce-spark-benchmark-pair.md) |
| Host data readable in Spark containers | [ADR-0003](../adr/0003-compose-cluster-host-uv.md) |

1. **Event-Detector README + layout** (speed layer)  
   Turn the node pattern into the streaming design-doc skeleton (annotated → aggregated → event).

2. **City-Dashboard README** (analytics / dashboards)  
   Replicate historical vs live in Superset: Hive-backed tabs + streaming-sink live tab.

3. **Hive-on-Docker + HDFS warehouse** (data & lake)  
   `beeline` querying HDFS-backed tables in Compose. `infra/hive/hive-site.xml` uses a local Derby warehouse on purpose. Target: `hive.metastore.warehouse.dir = hdfs://namenode:9000/user/hive/warehouse`.

4. **YARN + Maven jars** (batch baseline)  
   Prove a MapReduce job runs on bde2020 YARN. `make mr-package` builds in Docker.

5. **Watermarks & late data** (speed layer)  
   Replay through Kafka: event-time vs processing-time. When decided, write or supersede ADR-0008; do not only leave a chat note.

6. **Host ingest parquet vs Spark container** (data & lake / Spark)  
   Files under `./data` must be readable at `/opt/data` in Spark services (mounts, permissions).

# Spikes

Half-day each. Hive-on-HDFS and YARN-MR gate later milestones.

1. **Event-Detector README + layout** (speed layer)  
   Adopt "one metric node per streaming job" and annotated → aggregated → event as the streaming design-doc skeleton.

2. **City-Dashboard README** (analytics / dashboards)  
   Historical vs live. Replicate in Superset: Hive-backed tabs + streaming-sink live tab.

3. **Hive-on-Docker + HDFS warehouse** (data & lake)  
   `beeline` querying HDFS-backed tables in Compose. Finickiest part. `infra/hive/hive-site.xml` currently uses a local Derby warehouse **on purpose**. Target: `hive.metastore.warehouse.dir = hdfs://namenode:9000/user/hive/warehouse`.

4. **YARN + Maven jars** (batch baseline)  
   bde2020 images include YARN. Prove a MapReduce job actually runs. `make mr-package` builds the jar in Docker (Temurin 8 in that image — no host JDK).

5. **Watermarks & late data** (speed layer)  
   Replay through Kafka: event-time vs processing-time decides whether rolling numbers mean anything. Document the choice in this folder when decided.

6. **Host ingest parquet vs Spark container** (data & lake / Spark)  
   Files under `./data` must be readable at `/opt/data` in Spark services (mounts, permissions). There is no host Spark path to keep in sync.

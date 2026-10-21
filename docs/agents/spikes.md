# Spikes

Half-day each. YARN-MR gates later milestones. Hive-on-HDFS is done.

Related decisions (already accepted — spikes prove or refine them):

| Spike theme                                   | ADR                                                                                                                      |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| One metric per streaming job / Event-Detector | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md)                                                                   |
| Live vs historical dashboard                  | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md), [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md) |
| MapReduce on YARN                             | [ADR-0007](../adr/0007-mapreduce-spark-benchmark-pair.md)                                                                |
| Spark reads the HDFS lake                     | [ADR-0003](../adr/0003-compose-cluster-host-uv.md)                                                                       |

1. **Event-Detector README + layout** (speed layer)  
   Turn the node pattern into the streaming design-doc skeleton (annotated → aggregated → event).

2. **City-Dashboard README** (analytics / dashboards)  
   Replicate historical vs live in Grafana: Hive-backed views + streaming-sink live views.

3. **Hive-on-Docker + HDFS warehouse** (data & lake)  
   Done. `hive.metastore.warehouse.dir` is `hdfs://namenode:9000/user/hive/warehouse`.
   The authority is `fs.defaultFS` on the namenode (`hdfs://namenode:9000`). The path is Hive's default warehouse directory.
   Derby stays the metastore database. The JDBC URL is `jdbc:derby:;databaseName=/opt/hive/data/metastore_db;create=true`.
   Hive clients and the namenode use replication 1. Compose has one datanode.
   Tez runs in local mode. The proof `SELECT` used that local Tez process.
   A new `hive_metastore` volume sets database `default` to this URI.
   A volume created while the warehouse path was `/opt/hive/data/warehouse` keeps `default` on local disk. Remove that volume and start Hive again.
   Proof, 2026-10-16: Beeline `INSERT` then `SELECT` on `default.hdfs_probe`. The namenode stored `hdfs://namenode:9000/user/hive/warehouse/hdfs_probe/000000_0`. HiveServer2 and the metastore listed that path. `DROP TABLE` removed it.

4. **YARN + Maven jars** (batch baseline)  
   Prove a MapReduce job runs on bde2020 YARN. `make mr-package` builds in Docker.

5. **Watermarks & late data** (speed layer)  
   Replay through Kafka: event-time vs processing-time. When decided, write or supersede ADR-0008; do not only leave a chat note.

6. **Spark reads the HDFS lake** (data & lake / Spark)  
   Batch jobs read `hdfs://namenode:9000/lake/...`. Mount `infra/hive/core-site.xml` and `hdfs-site.xml` into Spark. Set `HADOOP_CONF_DIR`. That ships `dfs.replication=1`. Keep the `./data` mount until the stream-sink moves onto HDFS (#22). Amend ADR-0003 in the same branch. Record: [work-plan.md](../work-plan.md).

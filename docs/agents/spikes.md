# Spikes

Half-day each. YARN-MR gates later milestones. Hive-on-HDFS is done.

Related decisions (already accepted — spikes prove or refine them):

| Spike theme                                   | ADR                                                                                                                      |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| One metric per streaming job / Event-Detector | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md)                                                                   |
| Live vs historical dashboard                  | [ADR-0008](../adr/0008-speed-layer-streaming-shape.md), [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md) |
| MapReduce on YARN                             | [ADR-0007](../adr/0007-mapreduce-spark-benchmark-pair.md), [ADR-0015](../adr/0015-yarn-mapreduce-submit.md)              |
| Spark reads the HDFS lake                     | [ADR-0003](../adr/0003-compose-cluster-host-uv.md)                                                                       |

1. **Event-Detector README + layout** (speed layer)  
   Annotated envelope → window aggregate → one metric event. One file per node. The note is [event-detector.md](event-detector.md). It does not replace ADR-0008. `rolling_metrics.py` stays a stub until #22. Human sign-off still owns close on #10.

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
   ResourceManager and NodeManager are in Compose (bde2020 3.2.1). `make mr-package` builds the jar in Docker. `make yarn-probe` submits the Hadoop examples `pi` job. Spark stays `spark://spark:7077`. DemandCount is #24. Decision: [ADR-0015](../adr/0015-yarn-mapreduce-submit.md).
   Proof, 2026-10-24: `make yarn-probe` after `make up`. NodeManager registered at 2048 MB / 4 vcores. `application_1792786538759_0001` (`QuasiMonteCarlo`, MAPREDUCE) final state SUCCEEDED. Jar `demand-count-0.1.0.jar` built by `make mr-package`. Human sign-off still owns close on #9.

5. **Watermarks & late data** (speed layer)  
   Replay through Kafka: event-time vs processing-time. When decided, write or supersede ADR-0008; do not only leave a chat note.

6. **Spark reads the HDFS lake** (data & lake / Spark)  
   Conf is in Compose. `HADOOP_CONF_DIR=/etc/hadoop` on Spark and Hive ([ADR-0014](../adr/0014-hadoop-client-conf-at-etc-hadoop.md)). Same `core-site.xml` and `hdfs-site.xml`. ADR-0003 revised 2026-10-24. Keep the `./data` mount until #22. Human sign-off still owns close on #7.  
   Proof, 2026-10-24, `make spark-hdfs` after `make up`: `fs.defaultFS=hdfs://namenode:9000`, `dfs.replication=1` after the HDFS FileSystem class loads `hdfs-site.xml`, `ls hdfs://namenode:9000/lake` shows `namma_yatri`, write to `/tmp/spark_hdfs_probe` at replication 1 (`_SUCCESS` and both parquet parts). Hadoop does not put `dfs.replication` on a bare Spark `Configuration` until that FileSystem init. Record: [work-plan.md](../work-plan.md).

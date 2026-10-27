# Runtime

Spark, Hadoop, Hive, Kafka, Grafana, and Postgres run **only** in Docker Compose. Do not install Hadoop/Spark/JDK on the host.

| Piece                              | Where it runs                          | How                                                               |
| ---------------------------------- | -------------------------------------- | ----------------------------------------------------------------- |
| Ingest, amplify, tests, lint       | Host `uv`                              | `make setup`, `make ingest`, `make test`                          |
| Kafka producer                     | Host `uv`                              | `POST /api/runs` kind `replay`, or `make stream-produce` (one shot) → `127.0.0.1:9094` |
| Spark batch + Structured Streaming | Compose `spark` + `spark-worker`       | API analytics runs, `make stream-consume`                         |
| MapReduce compile                  | `maven:3.9-eclipse-temurin-8` one-shot | `make mr-package`                                                 |
| MapReduce submit                   | `resourcemanager` container            | `make yarn-probe` ([ADR-0015](adr/0015-yarn-mapreduce-submit.md)) |
| Hive DDL                           | `hiveserver2`                          | `make hive-ddl`                                                   |

`make setup` / `python -m flowstate.config --check` prints data dir, Kafka bootstrap, Spark master URL.

## Kafka listeners

Host producer must **not** use `kafka:9092`. That hostname only resolves inside Compose.

- Containers → `kafka:9092`
- Host → `127.0.0.1:9094` (`FLOWSTATE_KAFKA_BOOTSTRAP_SERVERS` may say `localhost`; the client rewrites it). Kafka is not published on IPv6.

Spark services set `FLOWSTATE_DATA_DIR=/opt/data`, `FLOWSTATE_KAFKA_BOOTSTRAP_SERVERS=kafka:9092`, and `HADOOP_CONF_DIR=/etc/hadoop` (the Hive `core-site.xml` and `hdfs-site.xml`). Jobs inside the network see the mount, the internal Kafka listener, and `dfs.replication=1`. Path: [ADR-0014](adr/0014-hadoop-client-conf-at-etc-hadoop.md).

## Compose links

Host `uv` is outside Compose. It writes `./data` and publishes to Kafka.

```mermaid
flowchart TB
  host["Host uv"]

  subgraph compose [Docker Compose]
    nn["namenode\n9870 UI, 9000 RPC"]
    dn["datanode"]
    rm["resourcemanager\n8088 UI"]
    nm["nodemanager\n8042 UI"]
    kafka["Kafka\n9092 containers, 9094 host, 9093 controller"]
    sm["spark master\n8080 UI, 7077, 4040"]
    sw["spark-worker"]
    hm["hive-metastore\n9083"]
    hs["hiveserver2\n10000 JDBC, 10002"]
    gf["grafana\n3000"]
    pg["postgres\n5432"]
  end

  host -->|"localhost:9094"| kafka
  host -->|"127.0.0.1:5432"| pg
  dn -->|"HDFS RPC"| nn
  nm -->|"YARN"| rm
  rm -->|"hdfs://namenode:9000"| nn
  nm -->|"hdfs://namenode:9000"| nn
  sw -->|"spark://spark:7077"| sm
  sm -->|"hdfs://namenode:9000"| nn
  sw -->|"hdfs://namenode:9000"| nn
  sm -->|"kafka:9092"| kafka
  sw -->|"kafka:9092"| kafka
  hs -->|"thrift://hive-metastore:9083"| hm
  hm -->|"hdfs://namenode:9000"| nn
  hs -->|"hdfs://namenode:9000"| nn
```

Bind mounts, not network links: `./data` is `/opt/data` in the Spark master and the worker. `./data/lake` is `/mnt/lake` on the namenode. `./src` is `/opt/src` in Spark and in HiveServer2.

Grafana listens on host port 3000. It has no warehouse connection and no stream connection yet. Hive warehouse files are on HDFS at `hdfs://namenode:9000/user/hive/warehouse`. Derby is the metastore database.

Postgres listens on `127.0.0.1:5432`. It stores model config ([ADR-0018](adr/0018-postgres-for-model-config.md)). Mobility data stays on disk and HDFS. Derby remains the Hive metastore. After `make up`, run `make config-seed`. The API is `make api` on `127.0.0.1:8090`. It serves model config, the demo routes, and `/api/runs`. The `runner` service publishes `127.0.0.1:8091`. Field reference: [model-config.md](model-config.md). Job backend: [ADR-0020](adr/0020-job-backend.md).

Port 9092 is published on loopback. Kafka advertises it as `kafka:9092`. Use that name only inside Compose. The host producer uses `localhost:9094`. Namenode 9870/9000 also bind to `127.0.0.1`. HDFS permissions are off, so those ports must not listen on the LAN.

## Compose services

| Service         | Ports                                          | Role                                     |
| --------------- | ---------------------------------------------- | ---------------------------------------- |
| namenode        | 127.0.0.1:9870 UI, 127.0.0.1:9000 RPC          | HDFS. Loopback only. Permissions are off |
| datanode        | —                                              | HDFS blocks                              |
| resourcemanager | 127.0.0.1:8088 UI                              | YARN ResourceManager. Loopback only      |
| nodemanager     | 127.0.0.1:8042 UI                              | YARN NodeManager. Loopback only          |
| kafka           | 127.0.0.1:9092 containers, 127.0.0.1:9094 host | KRaft, no ZooKeeper                      |
| spark           | 8080 UI, 7077, 4040                            | Spark master                             |
| spark-worker    | —                                              | Spark worker                             |
| hive-metastore  | 9083                                           | Derby metastore database                 |
| hiveserver2     | 10000, 10002                                   | beeline / JDBC                           |
| grafana         | 3000                                           | dashboards                               |
| postgres        | 127.0.0.1:5432                                 | Model config only. Not the lake          |

Tags are pinned in `docker-compose.yml`. The Hive warehouse directory is `hdfs://namenode:9000/user/hive/warehouse`. MapReduce submit is `make yarn-probe` ([ADR-0015](adr/0015-yarn-mapreduce-submit.md)). A `hive_metastore` volume from before that URI keeps database `default` on local disk. Remove the volume and start Hive again.

## Data mount

Host ingest writes `./data/...`. Spark containers still mount that at `/opt/data`. After #7, batch jobs read `hdfs://namenode:9000/lake/...`. Keep the mount until #22 moves the stream-sink onto HDFS. Do not treat `/opt/data` as the batch input.

## Ride-hail lake

`make lake` puts the staged ride-hail parquet on HDFS. Run `make up` first so the namenode and the datanode are up. A login shell in the namenode does not see `hdfs`. The binary is `/opt/hadoop-3.2.1/bin/hdfs`.

```bash
docker compose --env-file infra/kafka/cluster.env exec -T namenode \
  /opt/hadoop-3.2.1/bin/hdfs dfs -ls /lake/namma_yatri
```

The lake directory is `hdfs://namenode:9000/lake/namma_yatri/`. MapReduce submit reads that directory. Amplify writes `hdfs://namenode:9000/lake/rides_amplified/` beside it. `./data/lake` is the copy-in mount (`/mnt/lake` on the namenode). It is not the lake. `make lake` removes the ride-hail copy-in after HDFS has the files. `data/lake/hive-load/` stays. The host staged files stay in `data/staged/namma_yatri/`.

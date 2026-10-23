# ADR-0014: Hadoop client conf at `/etc/hadoop`

## Status

Accepted. 2026-10-24.

## Context

Spark and Hive need the same two Hadoop *client* files: `core-site.xml` (`fs.defaultFS`) and `hdfs-site.xml` (`dfs.replication=1`). Hive 4 and the first #7 Spark mount put those files at `$HADOOP_HOME/etc/hadoop`, which was `/opt/hadoop/etc/hadoop`.

That path is the Apache Hadoop tarball layout. It is not the Linux FHS `/etc` directory. FHS puts host and service configuration under `/etc`. `/opt` holds the add-on package. The Spark image has no Hadoop install. The bind mount created `/opt/hadoop` only to hold two xml files.

The namenode and datanode images already generate their *server* conf at `/etc/hadoop` from `infra/hadoop/hadoop.env`.

## Decision

Hadoop client conf in Compose is `/etc/hadoop`. Set `HADOOP_CONF_DIR=/etc/hadoop` on Spark master, Spark worker, Hive metastore, and HiveServer2. Mount `infra/hive/core-site.xml` and `infra/hive/hdfs-site.xml` at that directory.

Do not mount those files onto the namenode or the datanode. Those processes keep the image-generated `/etc/hadoop`. Overlaying it would replace the live server conf.

Do not use `$HADOOP_HOME/etc/hadoop` as the client path.

Hive product conf stays at `/opt/hive/conf/hive-site.xml`. That file is not Hadoop conf.

The host `./data` mount at `/opt/data` is a different decision. It stays until #22.

## Consequences

- Recreate Spark and Hive containers after this change.
- A later Hadoop client (MapReduce on YARN, #9) uses the same directory.
- Two xml trees can exist at the same path in different containers. Daemon files come from the bde2020 entrypoint. Client files come from `infra/hive/`.

## Sources

[ADR-0003](0003-compose-cluster-host-uv.md), issue #7, FHS 3.0 (`/etc`, `/opt`)

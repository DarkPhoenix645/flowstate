# ADR-0015: MapReduce submit on bde2020 YARN

## Status

Accepted. 2026-10-24.

## Context

[ADR-0007](0007-mapreduce-spark-benchmark-pair.md) keeps a Maven MapReduce module and submits it on YARN in Compose. Compose had a namenode and a datanode only. `make mr-package` builds a jar. It does not start a YARN application.

Spark stays on `spark://spark:7077`. The MapReduce analytics jobs are #24. Issue #9 asks for a ResourceManager and a NodeManager from the bde2020 3.2.1 images, and for one MapReduce application whose YARN state is SUCCEEDED.

[ADR-0014](0014-hadoop-client-conf-at-etc-hadoop.md) says a later Hadoop client uses `/etc/hadoop`. The namenode and the datanode already generate that directory from `infra/hadoop/hadoop.env`. An overlay of the Hive client files would replace the live server conf.

## Decision

Add Compose services `resourcemanager` and `nodemanager`.

- Images: `bde2020/hadoop-resourcemanager:2.0.0-hadoop3.2.1-java8` and `bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8`.
- Both read `infra/hadoop/hadoop.env`. The image entrypoint writes `/etc/hadoop`. Do not mount `infra/hive/core-site.xml` or `infra/hive/hdfs-site.xml` on these daemons.
- Hostnames are `resourcemanager` and `nodemanager`.
- ResourceManager UI `8088` and NodeManager UI `8042` bind to `127.0.0.1`. The UI has no authentication. HDFS permissions are off.
- No HistoryServer. The timeline service stays off.

YARN container sizes fit one Compose host. NodeManager memory is 2048 MB. The scheduler maximum allocation is 1024 MB. The map container, the reduce container, and the application master are 512 MB each. The upstream bde2020 file sets NodeManager memory to 16384 MB. That value does not get a container on this host.

`mapreduce.framework.name` is `yarn`. The shuffle aux service is `mapreduce_shuffle`. Its class is `org.apache.hadoop.mapred.ShuffleHandler`.

`make yarn-probe` submits the Hadoop MapReduce examples `pi` job (1 map, 1 sample) with `/opt/hadoop-3.2.1/bin/yarn jar` inside the `resourcemanager` container. The application name is `QuasiMonteCarlo`. The probe passes when the final state is SUCCEEDED. This job is not the #24 analytics jar. #24 still reads `hdfs://namenode:9000/lake/rides_amplified/` at bench time.

Spark master stays `spark://spark:7077`. Do not set the Spark master to `yarn`.

## Consequences

- Recreate the namenode, the datanode, the ResourceManager, and the NodeManager after a change to `hadoop.env`. `make up` does that. `make down-v` is not required. Volumes stay.
- A host JDK stays out of scope. `make mr-package` still uses the Maven image.
- #24 submits the Q1–Q5 MapReduce jobs to this ResourceManager.

## Sources

[ADR-0007](0007-mapreduce-spark-benchmark-pair.md), [ADR-0014](0014-hadoop-client-conf-at-etc-hadoop.md), issue #9, bde2020 `docker-hadoop` tag `2.0.0-hadoop3.2.1-java8`

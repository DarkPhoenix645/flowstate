# Architecture Decision Records

Accepted choices for FlowState. Read these before you change stack, schema, or runtime shape. Spec detail and open spikes: [../agents/](../agents/). Glossary: [../../CONTEXT.md](../../CONTEXT.md).

| ADR                                                   | Title                                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------------------ |
| [0001](0001-citypulse-reference-architecture-only.md) | CityPulse as reference architecture only                                 |
| [0002](0002-lambda-pipeline-kafka-spark-hdfs-hive.md) | Lambda pipeline + CityPulse → FlowState map                              |
| [0003](0003-compose-cluster-host-uv.md)               | Compose cluster and host uv                                              |
| [0004](0004-make-task-runner.md)                      | Make as the task runner                                                  |
| [0005](0005-canonical-ride-schema.md)                 | Canonical ride schema and Hive partitions                                |
| [0006](0006-amplify-for-scale-benchmarks.md)          | Amplify for scale benchmarks                                             |
| [0007](0007-mapreduce-spark-benchmark-pair.md)        | MapReduce baseline beside Spark                                          |
| [0008](0008-speed-layer-streaming-shape.md)           | Speed layer shape (topic contract, scenario engine)                      |
| [0009](0009-grafana-city-dashboard.md)                | Grafana for City Dashboard                                               |
| [0010](0010-bengaluru-calibrated-resampler.md)        | Bengaluru calibrated multimodal resampler                                |
| [0011](0011-bengaluru-source-table-v2.md)             | Bengaluru source table v2                                                |
| [0012](0012-bmtc-boarding-prior.md)                  | BMTC boarding prior as `FLOWSTATE_` settings (#40)                        |
| [0013](0013-amplify-thins-the-alignment-window.md)    | Amplify thins the alignment window                                       |
| [0014](0014-hadoop-client-conf-at-etc-hadoop.md)      | Hadoop client conf at `/etc/hadoop`                                      |
| [0015](0015-yarn-mapreduce-submit.md)                 | MapReduce submit on bde2020 YARN                                         |
| [0016](0016-demo-surface.md)                          | Demo surface (twin catalog, replay, fork)                                |
| [0017](0017-producer-as-a-realtime-twin-source.md)    | Producer as a realtime twin source                                       |
| [0018](0018-postgres-for-model-config.md)             | Postgres for model config                                                |
| [0019](0019-analytics-catalog.md)                     | Analytics catalog                                                        |

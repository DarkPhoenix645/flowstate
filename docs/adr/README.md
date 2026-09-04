# Architecture Decision Records

Accepted choices for FlowState. Read these before you change stack, schema, or runtime shape. Spec detail and open spikes: [../agents/](../agents/). Glossary: [../../CONTEXT.md](../../CONTEXT.md).

| ADR | Title |
|---|---|
| [0001](0001-citypulse-reference-architecture-only.md) | CityPulse as reference architecture only |
| [0002](0002-lambda-pipeline-kafka-spark-hdfs-hive.md) | Lambda pipeline + CityPulse → FlowState map |
| [0003](0003-compose-cluster-host-uv.md) | Compose cluster and host uv |
| [0004](0004-make-task-runner.md) | Make as the task runner |
| [0005](0005-canonical-ride-schema.md) | Canonical ride schema and Hive partitions |
| [0006](0006-amplify-for-scale-benchmarks.md) | Amplify for scale benchmarks |
| [0007](0007-mapreduce-spark-benchmark-pair.md) | MapReduce baseline beside Spark |
| [0008](0008-speed-layer-streaming-shape.md) | Speed layer shape |

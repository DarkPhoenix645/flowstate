# Agent / spec corpus

Humans: skip this folder unless you need a contract, spike note, or CityPulse reading list. Onboarding: [../README.md](../README.md). Decisions: [../adr/](../adr/).

Agents: contracts and open spikes live here. Locked architecture choices live in `docs/adr/` — do not restate them. Do not implement jobs until asked.

| File                             | Contents                                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| [citypulse.md](citypulse.md)     | Org facts, ranked repos, reading list (decision: [ADR-0001](../adr/0001-citypulse-reference-architecture-only.md))                                |
| [event-detector.md](event-detector.md) | Streaming node layout (decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md))                                                    |
| [watermarks.md](watermarks.md)         | Watermark, windows, and late rows for #22 (decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md))                                  |
| [city-dashboard.md](city-dashboard.md) | Historical Hive dashboard vs live stream-sink dashboard (decision: [ADR-0009](../adr/0009-grafana-city-dashboard.md))                          |
| [schema.md](schema.md)           | Column dictionary (decision: [ADR-0005](../adr/0005-canonical-ride-schema.md))                                                                    |
| [workstreams.md](workstreams.md) | Paths, files, done-when (stack: [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md))                                                 |
| [spikes.md](spikes.md)           | Open half-day spikes                                                                                                                              |
| [scaffold.md](scaffold.md)       | Layout, compose caveats, imports (runtime: [ADR-0003](../adr/0003-compose-cluster-host-uv.md), Make: [ADR-0004](../adr/0004-make-task-runner.md)) |

Forward work (lake layout, host vs HDFS, Spark-over-HDFS, WebHDFS): [../work-plan.md](../work-plan.md).

Also under this folder (skills config, not FlowState specs): `issue-tracker.md`, `triage-labels.md`, `domain.md`.

# Agent / spec corpus

Humans: skip this folder unless you need a contract, spike note, or CityPulse reading list. Onboarding: [../README.md](../README.md). Decisions: [../adr/](../adr/).

Agents: contracts and open spikes live here. Locked architecture choices live in `docs/adr/` — do not restate them. Do not implement jobs until asked.

| File                             | Contents                                                                                                                                          |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| [citypulse.md](citypulse.md)     | Org facts, ranked repos, reading list (decision: [ADR-0001](../adr/0001-citypulse-reference-architecture-only.md))                                |
| [schema.md](schema.md)           | Column dictionary (decision: [ADR-0005](../adr/0005-canonical-ride-schema.md))                                                                    |
| [workstreams.md](workstreams.md) | Paths, files, done-when (stack: [ADR-0002](../adr/0002-lambda-pipeline-kafka-spark-hdfs-hive.md))                                                 |
| [spikes.md](spikes.md)           | Open half-day spikes                                                                                                                              |
| [scaffold.md](scaffold.md)       | Layout, compose caveats, imports (runtime: [ADR-0003](../adr/0003-compose-cluster-host-uv.md), Make: [ADR-0004](../adr/0004-make-task-runner.md)) |

Also under this folder (skills config, not FlowState specs): `issue-tracker.md`, `triage-labels.md`, `domain.md`.

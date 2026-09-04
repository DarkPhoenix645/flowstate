# ADR-0007: MapReduce baseline beside Spark

## Status

Accepted

## Context

The batch layer needs Spark analytics for the main descriptive and diagnostic jobs. The report also needs a classic Hadoop baseline at the same aggregations and scales.

## Decision

Keep a Maven MapReduce module under `src/flowstate/batch/mapreduce/` (Hadoop 3.2.1 `provided`, aligned with bde2020 3.2.1 images).

- Mirror Spark aggregations that matter for the headline (for example zone × hour demand, cancellation counts).
- Build the jar with `make mr-package` in Docker (Temurin 8). Submit on YARN in Compose.
- Compare Spark vs MapReduce at 1M / 10M / 100M through `benchmarks/runner.py`.

Proving YARN submission on bde2020 images is an explicit spike. It gates later batch milestones.

## Consequences

- Spark remains the primary analytics path. MapReduce is the baseline and teaching contrast, not a second product surface.
- Aggregation definitions must stay comparable across the two engines for fair timings.
- Host JDK is out of scope (see ADR-0003).

## Sources

`docs/agents/workstreams.md`, `docs/agents/spikes.md`, `docs/team.md`

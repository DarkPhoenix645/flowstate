# ADR-0001: CityPulse as reference architecture only

## Status

Accepted

## Context

The CityPulse GitHub org (EU FP7, 2014–2017) documents a smart-city pipeline: ingest → data bus → real-time aggregation and event detection → historical store → dashboard. The public repos are old Java, AMQP, and RDF stacks. They do not match HDFS, Spark, Kafka, or Hive.

The team needs a citation and a component map for the report. It does not need a runnable CityPulse deployment.

## Decision

Treat CityPulse as an architecture reference only.

- Map CityPulse component boundaries onto FlowState (see ADR-0002).
- Do not run, fork, or port CityPulse repositories as part of this project.
- Cite CityPulse in the report as real-time stream processing and large-scale data analytics (speed layer + batch layer).

## Consequences

- Agents and humans read CityPulse READMEs for patterns (Event-Detector nodes, live vs historical dashboard), not for code reuse. Repo ranking: `docs/agents/citypulse.md`.
- Spikes that study CityPulse stay design-only unless a FlowState ticket says otherwise.
- Report language stays aligned with CityPulse boundaries without promising CityPulse runtime compatibility.

## Sources

`docs/agents/citypulse.md`, `docs/architecture.md`

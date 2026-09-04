# ADR-0008: Speed layer shape

## Status

Accepted

## Context

CityPulse Event-Detector used pluggable nodes on aggregated or annotated streams. FlowState needs a speed layer that replays historical rides through Kafka and computes live metrics without host Spark.

Watermark and late-data policy on replay is still a spike. The structural choices below are locked.

## Decision

- Producer on the host (`producer.py`) replays staged rides into `rides.raw` with `--events-per-sec` and event-time interpolation.
- Consumers run on Compose Spark (`rolling_metrics.py` and follow-on jobs).
- Prefer **one streaming job per metric** (surge, active rides, cancellations), following the Event-Detector node pattern.
- Default windows in the scaffold: 1-minute tumbling, 5-minute sliding, watermark 10 minutes on `timestamp`.
- RiverBench citypulse-traffic is the reference for replay-shaped datasets, not a runtime dependency.

Dashboard work splits historical Hive-backed views from live stream-sink views (City-Dashboard pattern).

## Consequences

- Do not collapse all live metrics into one opaque job without a strong reason and a new ADR.
- Until the watermark spike closes, treat the 10-minute watermark as the working default and document changes under `docs/agents/` or a superseding ADR.
- Live dashboard tabs read the stream sink. Batch tabs read Hive.

## Sources

`docs/agents/citypulse.md`, `docs/agents/spikes.md`, `docs/agents/workstreams.md`, `docs/team.md`

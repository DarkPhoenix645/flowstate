# ADR-0008: Speed layer shape

## Status

Accepted. Revised 2026-10-24. Window and watermark defaults are unchanged. Bus boardings are fields on the scheduled-stop event. There is no separate boarding topic. Node files and HDFS sinks are in [event-detector.md](../agents/event-detector.md).

## Context

CityPulse Event-Detector used pluggable nodes on aggregated or annotated streams. FlowState needs a speed layer that emits calibrated multimodal events through Kafka, supports what-if stimulation, and computes live metrics without host Spark. The producer and the event envelope are specified in [resampling-plan.md](../resampling-plan.md) §8–9.

## Decision

- **Topics (v1):** `flowstate.ridehail.activity.v1`, `flowstate.metro.station_hour.v1`, `flowstate.bus.scheduled_stop.v1`, `flowstate.aviation.arrival.v1`, `flowstate.aviation.departure.v1`, `flowstate.scenario.command.v1`, and `flowstate.simulation.audit.v1`. There is no separate bus-boarding topic, no single mixed-mode topic, and no enrichment topic.
- **Producer:** host-side, conditionally stimulable. Modes: historical reconstruction, deterministic replay, live simulation on the wall clock, and scenario forks. Scenario commands arrive on the command topic; effects apply only to synthetic output after the command time and are tagged with a `scenario_id`. Responses are `assumption_driven` unless a held-out test validates the specific coefficient.
- **Envelope:** every event carries `schema_version`, `event_id`, `city_id`, `mode`, `event_type`, `event_time`, `emitted_at`, `location_ref`, `scenario_id`, and a provenance block (`status` in `observed | observed_proxy | derived | synthetic`, source ids, model/profile versions, seed, evidence grade, `outside_training_window`). Metro records use explicit `entries_count` / `exits_count`. A bus record is one scheduled stop call plus a `ridership` object (`boardings`, `alightings`, `onboard_leaving`). The stop call is `derived`. The ridership object is `synthetic` and `assumption_driven`. Idempotent message keys; event ids reproducible under replay.
- **Consumers:** one Structured Streaming job per metric on Compose Spark. The first node is `rolling_metrics.py` (active rides / zone) via `make stream-consume`. Cancel rate and surge are later files. The layout is [event-detector.md](../agents/event-detector.md). Checkpoints are `hdfs://namenode:9000/checkpoints/<job>`. The stream-sink archive is `hdfs://namenode:9000/stream-sink/<job>/`. Do not use `/opt/data` as the live sink.
- **Windows:** 1-minute tumbling, 5-minute sliding, watermark 10 minutes on `event_time`. The watermark spike stays open.
- **Audit:** every run logs start/stop, model and profile hashes, seed, query coverage, and QA failures to the audit topic.
- The OpenSky airport endpoints are batch endpoints (nightly updates, previous days only). They are never a live flight feed; live injections come through scenario commands.

## Consequences

- Do not collapse live metrics into one opaque job without a strong reason and a new ADR.
- Dashboards must filter by provenance status and must not sum observed aggregates with their synthetic children. Bus boardings are synthetic fields on a derived stop call. Do not present them as BMTC counts.
- The canonical lake schema (ADR-0005) is unchanged. The Kafka contract is this envelope, not `RIDE_COLUMNS` JSON; `ride_event_schema.py` moves to the envelope in the resampler tickets.
- Until the watermark spike closes, the 10-minute watermark stays the working default.
- The host directory `data/stream-sink` is not the live sink.

## Sources

`docs/resampling-plan.md` §8–9, [ADR-0010](0010-bengaluru-calibrated-resampler.md), `docs/agents/citypulse.md`, `docs/agents/spikes.md`, `docs/agents/workstreams.md`, `docs/team.md`

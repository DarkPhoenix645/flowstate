# ADR-0010: Bengaluru multimodal RNN resampler

## Status

Accepted

## Context

The product question is cross-modal and predictive. Example: an aviation event at Bengaluru airport, then expected taxi surge in that region. A taxi-only generator cannot answer that. Batch jobs and speed-layer jobs must infer on the **same** process.

Seeds are mixed grain (trip, day, schedule, month). The resampler must learn correlations across modes, then emit **event-grain** rows for taxi, metro, bus, and aviation. Coarse seeds (Namma daily totals, DGCA monthly pairs, GTFS timetable) are training context, not the emit grain.

Delhi NCR Uber (row 1) is a taxi-only city slice. It is not training data for this resampler.

## Decision

One RNN **resampler** generates Bengaluru mobility events for all four `mode` values: `ride_hail`, `metro`, `bus`, `aviation`. Both paths consume that generator:

| Path | Sink | Make entry |
| --- | --- | --- |
| Batch / lake | `data/staged/rides_amplified/` then HDFS | `make amplify` (ADR-0006 scale `N`) |
| Speed layer | Kafka (working default: `rides.raw`) | `make stream-produce` |

Rules:

- Same model, same `--seed`, same draws. Only `N`, sink, and `--events-per-sec` change.
- `city` is `bengaluru`. Do not train on NCR Uber or Mumbai.
- The model must be usable as a **conditional** emitter: given recent aviation / metro / bus context (and zone), draw taxi (`ride_hail`) events, including fare/surge-relevant fields. The inverse (taxi context → other modes) may come later. The airport-landing → taxi-price question is in scope.
- Event `timestamp` sits in the **Bengaluru alignment window** (from 2024-10-26).
- Each emit row uses the canonical schema (ADR-0005) with `mode` set. Extra columns may sit beside it (flight vs metro vs GTFS native fields).
- Until T4 (#5) closes, the **working Kafka default** is T4 option 1: all modes on `rides.raw` with `mode` set. Separate topics may replace this only if T4 picks them **and** a join key (`timestamp`, `pickup_zone` / airport zone, `city`) still lets a streaming job condition taxi on aviation. Do not invent `rides.enriched`.
- Taxi mix stays seed-faithful for hour-of-day and cancel rates (rows 2 and 8; row 7 for lat/lon). Metro / bus / aviation emits must stay faithful to their seeds at the grain those seeds support (daily totals, schedule shape, monthly OD), then be **unfolded** to event grain by the resampler.
- DGCA has no per-flight landings. Aviation stream events are synthetic event-grain rows conditioned on Bengaluru aviation structure. They are not a live DGCA tap.
- GTFS has no ridership. Bus events are schedule-conditioned synthetics (trip/stop as events), not ticket counts.
- LSTM vs GRU stays an implementation choice. Behaviour is locked, not the layer count.
- NCR Uber stays `city=ncr` taxi-only and out of this resampler.

ADR-0006 still names scale `N` and `rides_amplified/`. `N` counts multimodal events from this resampler, not a second taxi-only bootstrap.

This ADR supersedes the ADR-0008 producer rule “replay staged rides” for Bengaluru Kafka emit. Metric jobs, windows, and watermarks in ADR-0008 stay. The surge job (T29) must be able to condition on aviation (and metro/bus) events in the same windows.

## Setup for later (do not skip)

1. **T3 (#4)** — Zone ids must be able to name an airport / catchment zone. Without that, “surge in that region” has no key.
2. **T4 (#5)** — Close Kafka shape with the join constraint above. Working default is option 1.
3. **T12–T14** — Land Bengaluru seeds (ride-hail, Namma, BMTC, DGCA `BENGALURU`) as staged parquet. The resampler trains on those, not on NCR.
4. **T32 (#34)** — Implement the shared module. `amplify.py` and `producer.py` call it. Tests: same `--seed` ⇒ same mode mix; a synthetic aviation event at the airport zone raises taxi event rate or fare in that zone vs a held-out draw.
5. **T21 / T29** — Streaming analytics run on the resampler stream, not on raw Jan/Jul 2024 taxis joined to metro days.

## Consequences

- Predictive cross-modal questions are in product scope. Taxi-only amplify is not the Bengaluru story.
- Spark/Hive and streaming see one multimodal distribution.
- Reports must quote `--seed`, `N`, city, and that aviation landings in the stream are synthetic.

## Sources

`docs/adr/0005-canonical-ride-schema.md`, `docs/adr/0006-amplify-for-scale-benchmarks.md`, `docs/adr/0008-speed-layer-streaming-shape.md`, GitHub T4 (#5), T32 (#34), `mobility_pipeline_dataset_analysis.md`

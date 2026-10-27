# ADR-0017: Producer as a realtime twin source

## Status

Accepted. 2026-10-27. Issue #39.

This ADR changes parts of [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0013](0013-amplify-thins-the-alignment-window.md), and [ADR-0016](0016-demo-surface.md). A newer accepted ADR wins. Each changed point is listed under "What this changes".

## Context

#39 built the producer. A review against the data and the demo plan found these problems.

- The producer split `--rows` by a fixed rule (50% ride-hail, 10% for each other mode). That rule is a second budget rule next to the resampler.
- The producer drew metro days from a fixed total of 200 people, not from the tracker.
- A date with no city-day row produced no ride-hail and no aviation.
- Bus stop calls carried the GTFS feed date. A replay of 2025-08-14 showed bus events on 2026-09-07.
- The resampler drew events for 101 ward ids that have no polygon and no pin. They hold 4.3% of ride-hail mass, and one id (`w_0`) holds 4.1%. Docs name these ids `out of scope`. No repo document says which place `m_*`, `t_*`, or `w_0` is.
- Each topic was sent in turn, so the modes did not meet at the play head.
- The producer did not publish the table that `parameters_ref` points at.
- `kia_catchment_wards` was empty. No ward centroid is within 7 km of VOBL.
- A metro disruption removed station-hour rows and moved nobody.

## Decision

### Volume

`--rows` is a count of events for the whole window. It must be at least 1,000 and at most 2,000,000. The split by mode is the measured natural share of the alignment window (`window.natural_shares`). That is the rule `make amplify` uses. Ride-hail takes the exact remainder. There is no scale profile.

A day that has no city-day row freezes at the latest source day with the same weekday. A metro day freezes the same way on the tracker. The aviation trend freezes at the window edge. Such a day sets `outside_training_window` when it is outside the window. The audit start record lists each frozen day and its source day.

### Place

- The ward universe is the ids with the city prefix (`b_`). That is 243 wards, all pinned. `m_*`, `t_*`, and `w_0` are not emitted, by the producer or by `make amplify`. The ward shares renormalise over the 243 wards.
- Every event carries `location_ref.at`. A ride-hail event also carries `location_ref.drop`. The drop is a second independent ward draw, not an observed trip.
- A ride-hail point is a deterministic point inside the ward polygon (`placement` is `ward_synthetic_point`). The same ward and event id give the same point. The ward centroid is the fallback.
- Metro uses the station pin. Bus uses the GTFS stop. Aviation uses the airport pin. `placement` says which.
- Bus stop calls are stamped on the replay day. Each carries `feed_version`, the real GTFS feed date. `outside_training_window` follows the stamped date.

### Order and runtime

The producer is a restartable process. The demo API starts it with a replay request file (`--request`). A scrub is a restart with `seek`. All topics are sent in one global order by (`event_time`, `event_id`). Each topic keeps its own non-decreasing order. The cap is `max_eps`. It drops nothing. Waits shorter than 2 ms are added up so the cap holds.

`live` plans today and sends each event when the wall clock reaches its `event_time`. Events already past are not sent. All four modes run in live.

A replay is paced by `acceleration`. A run succeeds only when every send is acked.

### Catchment

`kia_catchment_wards` holds every ward whose centroid is within 17.85 km of the VOBL pin, by haversine. That is seven wards.

| Ward | Name | Distance |
| --- | --- | --- |
| `b_6` | Kogilu | 13.76 km |
| `b_1` | Kempegowda Ward | 14.93 km |
| `b_2` | Chowdeswari Ward | 16.29 km |
| `b_7` | Thanisandra | 17.28 km |
| `b_8` | Jakkuru | 17.49 km |
| `b_5` | Yelahanka Satellite Town | 17.60 km |
| `b_3` | Someshwara | 17.83 km |

The next ward, `b_81` Horamavu, is 17.97 km. No centroid is within 7 km. The radius is a choice, not a catchment study.

### Metro disruption

`metro_disruption` scales the station-hour entries and exits by `max(multiplier, 0.2)`. `shift_shares` then sends removed riders to ride-hail and bus. The rest is not modelled.

| Destination | Share of removed riders | Plausible range |
| --- | --- | --- |
| Ride-hail | 0.15 | 0.08 to 0.25 |
| Bus | 0.30 | 0.20 to 0.45 |
| Not modelled | 0.55 | 0.40 to 0.65 |

All three are `assumption_driven`. No source measures a Bengaluru closure. The evidence is from other cities:

- Boston MBTA Orange Line closure, 2022: shuttle buses took about 40% of riders and other buses about 8%. About 42% were unaccounted for.
- NYC station closures: ride-hail use rose 2.8% to 3.3% (Pham, Ipeirotis, Sundararajan, ICIS 2019).
- Washington SafeTrack Surge 1 pre-survey: 3% said Uber/Lyft or walk or bike.
- Toronto TTC stated preference, major disruptions: 16% drive or taxi (Lin, 2017).
- Delhi Metro 2011 stated survey: bus almost half, auto-rickshaw 12% (Goel and Tiwari, 2016).

A single-station closure sits at the high end of "not modelled". A whole-line peak outage sits at the low end.

The riders reach the ward that contains the station pin (point in polygon). A pin outside every polygon takes the nearest ward centroid. Ten of 95 station codes take that fallback. Removed riders are converted to real riders with the day's metro thinning (`people` over the tracker total). Ride-hail lifecycles then scale with `amplification_factor`, as the flight response does. Each lifecycle is one booking plus one completion or cancel.

The bus leg adds boardings on sampled stop calls within 1 km of the station. The same riders alight at the last sampled stop call of the trip. Boardings stay equal to alightings on each trip. Onboard stays under the capacity cap. A thin bus sample may have no stop call near the station. Then nothing is added.

### Flight response

Unchanged from ADR-0016: `rides_per_arrival` is 4 and `rides_per_departure` is 2. They are placeholders, not passengers per aircraft. The applied yield is the stored yield times `amplification_factor`.

### Envelope

`location_ref` is `{at, drop?}`. Each place holds `ward_id` or `station_id` or `stop_id` or `airport`, plus `latitude`, `longitude`, and `placement`. This replaces the flat `location_ref` in ADR-0008. `schema_version` stays `1.0`, or `1.1` when `parameters_ref` is set. The Spark `StructType` is generated from the same model.

The audit start record carries `parameters`, a table keyed by `parameters_ref`. The first key is `bus_prior`.

## What this changes

- ADR-0008: `location_ref` is nested (`at`, `drop`). Commands do not arrive on `flowstate.scenario.command.v1` while a replay runs. A request file carries them, and a scrub restarts the process.
- ADR-0013, point 5: the producer stamps bus on the replay day. `make amplify` keeps the feed date. ADR-0013 point 2 and the natural-share rule now also apply to the producer.
- ADR-0016, scale section: ADR-0016 spreads mass over 309 wards. The emitted universe is 243.
- ADR-0016, map section: ride-hail on the map is still a ward tile. The event point is now inside the ward, not the centroid.
- ADR-0016, command table: the ride-hail leg of `metro_disruption` is on. The crosswalk is point in polygon, built by the producer, not hand assigned. The bus leg is on, with the limit above.
- ADR-0016: `kia_catchment_wards` is set.
- Issue #37 and the `wards.py` docstring say the resampler copies the ward pin onto each ride-hail event. The Kafka envelope now does that, as `location_ref.at`. The lake parquet rows still do not carry a point (ADR-0005).

## Consequences

- `make amplify` output changes: no `m_*`, `t_*`, or `w_0` ward. Run it again to refresh `/lake/rides_amplified/` and `rollup_conservation`.
- At the 1M profile a flight response is small. Ten extra arrivals add well under one lifecycle per ward-hour. The effect grows with `amplification_factor`.
- The producer holds the plan in memory. A window above 2,000,000 events must be replayed in shorter windows.
- #22 groups by `location_ref.at.ward_id`.

## Scope for v2

- Stream the plan one day at a time, so a long window needs no memory cap.
- A catchment study for VOBL, and a ward-level model of airport access.
- A measured shift from a real Bengaluru closure, and a per-line version of `shift_shares`.
- A passenger-per-aircraft yield with a cab share, with a source.
- A denser bus sample near a disrupted station.
- Per-day natural shares in a replay. The mode split uses window totals today.
- A command consumer for `flowstate.scenario.command.v1` in a running replay.
- Ward ids `m_*`, `t_*`, and `w_0`: identify them from the Namma Yatri source, then decide whether to model them.

## Sources

#39, [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0012](0012-bmtc-boarding-prior.md), [ADR-0013](0013-amplify-thins-the-alignment-window.md), [ADR-0016](0016-demo-surface.md), `docs/agents/watermarks.md`, `docs/agents/event-detector.md`, `docs/resampling-plan.md` §8 and §9.

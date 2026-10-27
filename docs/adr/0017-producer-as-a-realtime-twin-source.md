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
- Each restart of the producer cost about 70 s. A scrub is a restart in that model.
- The producer did not read `flowstate.scenario.command.v1`, so a command could not reach a running replay.
- `kia_catchment_wards` was empty. No ward centroid is within 7 km of VOBL.
- The daily aviation fit learned thin OpenSky windows as low days. The terminal means were 81 arrivals and 459 departures a day.
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

The producer is a long-running service (`make stream-serve`). It loads the inputs, the pins, and the fitted model once. A restart of a one-shot process cost about 70 s, of which about 30 to 55 s was the model fit. The service pays that once.

The service reads two topics. `flowstate.simulation.control.v1` is new. It carries `start`, `pause`, `resume`, `speed`, `seek`, `stop`, and `shutdown` (`ControlMessage`, schema in `control.py`). `flowstate.scenario.command.v1` carries scenario commands, as ADR-0008 says. A `start` message holds the replay request: window, seed, rows, acceleration, `max_eps`, optional seek, optional commands, and an optional session id. Each message gets a `control` record on the audit topic with the same `request_id`, `accepted`, the state, the play head, and the requested and effective acceleration. A `status` record goes out about every second while a session runs. A `start` record opens a session and a `stop` record closes it, with `sent`, `acked`, `failed`, and the QA failures.

All topics are sent in one global order by (`event_time`, `event_id`). Each topic keeps its own non-decreasing order. The cap is `max_eps`. It drops nothing. Waits shorter than 2 ms are added up so the cap holds. A control message cuts a wait short.

- **Pause and resume** hold and release the play head. Nothing is sent while paused.
- **Speed** sets `acceleration`. A live session ignores it.
- **Command** during a replay forks the stream from the play head. The fork is a full copy of the baseline after that point, with the effect applied. Events already sent do not change. A command that started earlier begins at the play head. A second `scenario_id` is rejected, and the audit counts it as a QA failure.
- **Seek** moves the play head. A move forward keeps the `run_id`. A move backwards starts a new one, because the topics already hold later events. Every event carries `run_id` (`<session>.<epoch>`). A consumer reads the newest `run_id` and treats the others as history. A Spark job that must see one clean series resets its checkpoint on a new `run_id`, or filters on it.
- **Start** while a session runs ends that session. A plan for the same window, seed, and rows is reused (two are kept).

`live` plans today and sends each event when the wall clock reaches its `event_time`. Events already past are not sent. All four modes run in live. A live session cannot seek.

`emitted_at` is set at send time in every mode. A session counts a send as acked only when the broker acks it. The one-shot CLI (`make stream-produce`) stays for bulk runs and tests. It uses the same planning code.

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

### Aviation coverage

One rule decides whether a day-direction of OpenSky data is covered (`aviation_coverage.py`). A day-direction is covered when no row came from a failed or quota window, and its count is at least half the 75th percentile of that direction's healthy daily counts. On the checked data that reference is about 350 events a day, and 200 of 353 arrival days and 375 of 535 departure days are covered.

The generator and the daily fit use the same rule. A gap is not emitted as `observed_proxy`, and it is not learned as a low day. A direction with no rows, or a gated window, takes its share of the day's budget from the fitted mean. It does not take it from the thin count. So a day with 3 arrival rows and 100 departure rows still gets about as many synthetic arrivals as departures. The fit needs 60 covered days for each direction. Fitted on covered days, the terminal means are 321 arrivals and 333 departures a day, in line with the 330 to 375 in `docs/data-sources.md`. The raw fit gave 81 and 459.

The flight covariate test in `resampler-evaluation.md` needs shared covered dates. It now has 42 and 38, and needs 60, so it does not run. `keep_aviation_terms` stays false.

### Flight response

Unchanged from ADR-0016: `rides_per_arrival` is 4 and `rides_per_departure` is 2. They are placeholders, not passengers per aircraft. The applied yield is the stored yield times `amplification_factor`.

### Envelope

`location_ref` is `{at, drop?}`. Each place holds `ward_id` or `station_id` or `stop_id` or `airport`, plus `latitude`, `longitude`, and `placement`. This replaces the flat `location_ref` in ADR-0008. `schema_version` stays `1.0`, or `1.1` when `parameters_ref` is set. The Spark `StructType` is generated from the same model.

The audit start record carries `parameters`, a table keyed by `parameters_ref`. The first key is `bus_prior`.

## What this changes

- ADR-0008: `location_ref` is nested (`at`, `drop`). The envelope gains `run_id`. The topic set gains `flowstate.simulation.control.v1`, so the set is eight topics. Commands now arrive on `flowstate.scenario.command.v1` during a replay, as ADR-0008 says.
- ADR-0016, replay and fork: a scrub is a seek inside the running service. It does not restart the process. A backwards seek starts a new `run_id`.
- ADR-0013, point 5: the producer stamps bus on the replay day. `make amplify` keeps the feed date. ADR-0013 point 2 and the natural-share rule now also apply to the producer.
- ADR-0016, scale section: ADR-0016 spreads mass over 309 wards. The emitted universe is 243.
- ADR-0016, map section: ride-hail on the map is still a ward tile. The event point is now inside the ward, not the centroid.
- ADR-0016, command table: the ride-hail leg of `metro_disruption` is on. The crosswalk is point in polygon, built by the producer, not hand assigned. The bus leg is on, with the limit above.
- ADR-0016: `kia_catchment_wards` is set.
- Issue #37 and the `wards.py` docstring say the resampler copies the ward pin onto each ride-hail event. The Kafka envelope now does that, as `location_ref.at`. The lake parquet rows still do not carry a point (ADR-0005).

## Consequences

- `make amplify` output changes: no `m_*`, `t_*`, or `w_0` ward. Run it again to refresh `/lake/rides_amplified/` and `rollup_conservation`.
- At 1M events a flight response is small. Ten extra arrivals add well under one lifecycle per ward-hour. The effect grows with `amplification_factor`.
- The producer holds the plan in memory. A window above 2,000,000 events must be replayed in shorter windows.
- A fork takes about 10 to 17 s at 200k events, and the play head waits for it.
- The `kia_catchment_wards` list and the metro shares are policy values. #75 (T50) moves them to a versioned store.
- #22 groups by `location_ref.at.ward_id`.

## Scope for v2

- Stream the plan one day at a time, so a long window needs no memory cap.
- A catchment study for VOBL, and a ward-level model of airport access.
- A measured shift from a real Bengaluru closure, and a per-line version of `shift_shares`.
- A passenger-per-aircraft yield with a cab share, with a source.
- A denser bus sample near a disrupted station.
- Per-day natural shares in a replay. The mode split uses window totals today.
- More than one session at a time, and authentication on the control topic.
- A topic and checkpoint reset on a new `run_id`, done by the service.
- A fork that is computed while the play head keeps moving. Today the play head waits while the fork builds.
- Ward ids `m_*`, `t_*`, and `w_0`: identify them from the Namma Yatri source, then decide whether to model them.

## Sources

#39, [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0012](0012-bmtc-boarding-prior.md), [ADR-0013](0013-amplify-thins-the-alignment-window.md), [ADR-0016](0016-demo-surface.md), `docs/agents/watermarks.md`, `docs/agents/event-detector.md`, `docs/resampling-plan.md` §8 and §9.

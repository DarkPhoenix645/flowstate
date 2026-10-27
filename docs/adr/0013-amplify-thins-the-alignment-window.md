# ADR-0013: Amplify thins the alignment window

## Status

Accepted. 2026-10-22. Point 5 is amended for the producer by [ADR-0017](0017-producer-as-a-realtime-twin-source.md).

## Context

ADR-0010 gives four operating modes. `make amplify` (#34) writes the historical reconstruction for the lake. The Kafka producer (#39) writes the live stream on the wall clock.

The first #34 writer walked source days at full volume until `N` was full. A 1,000,000-row corpus then held three ride-hail days. The weekday mix of the corpus did not match the weekday mix of the Bengaluru alignment window. Mode shares did match the source row mix (about 98.9% ride-hail). That mix is the measured natural share. It is not a floor.

The lake corpus must keep the calendar shape of the window at every `N` in `{1e6, 1e7, 1e8}` (ADR-0006). Live emit stays #39.

## Decision

### Reconstruction rule

`make amplify` writes a **thin copy** of every source day in the city alignment window.

1. Measure natural row volume per mode in the window. Missing dates stay missing.
2. Split `N` across modes with `largest_remainder` on those volumes. Ride-hail stays ~99% of rows when that is the source mix.
3. Split each mode budget across that mode's source days with `largest_remainder` on the day volumes. Every day with a positive clock gets a non-negative integer budget. Days with budget 0 emit nothing.
4. Unfold each day with `partial_rows` (or the metro pack) equal to that day's budget. Parent totals conserve the **emitted** slice, not the full source day.
5. Bus has one GTFS feed date, which can sit outside the window. Flag `outside_training_window`. Do not invent bus service dates. `make amplify` still follows this point. The producer stamps bus on the replay day (ADR-0017).
6. Whole bus trips can undershoot the bus budget. The ride-hail remainder absorbs leftover rows so the corpus has exactly `N` rows.

Do not walk full days until the budget fills. Do not put the leftover on one last day.

### Metro pack

A metro row is one station-hour, not one person. A uniform draw of station-hour rows drops people mass and can split entries from exits.

When a metro day has a row budget `k` and a people budget `P`:

- Draw `k` station-hour cells with probability proportional to station share × hour prior.
- Split `P` as entries and `P` as exits across those cells with `largest_remainder`.
- Entries on that day equal exits on that day.

`P` is the tracker total for that day, scaled by `k /` the day's natural record weight, then corrected so the integers sum. Do not keep the full tracker total on a thin day.

### Bus trips

Sample whole trips with probability proportional to the hour blend (metro hour-exit prior mixed with the ride-hail hour prior). Do not use a uniform permutation. Stop-call conservation per trip stays.

Synthetic boardings, alightings, and onboard counts use `ResampleConfig` constants from the #40 assumption set (4.6 million weekday boardings, capacity 41.3, load 1.4, cab share 0.22). #40 moves those constants to `FLOWSTATE_` settings. #34 does not wait for #40. The counts are not BMTC measurements.

### Aviation

Split the aviation budget across all observed local dates in the window. Subsample that day's OpenSky rows (keep source timestamps). Fill with synthetic rows only when the day budget is larger than the observed count. Do not take three full flight days and clip the bag.

### What this ADR does not decide

- Live / wall-clock emit, `event_time` versus `emitted_at`, scenarios: #39 and ADR-0008.
- People-weighted mode mix (metro tracker people versus ride-hail rows). Mode mix stays event-row natural share.
- Mapping the BMTC feed onto window days.

## Consequences

- A 1,000,000-row corpus has timestamps on (almost) every source date in the window, at lower per-day volume.
- The same seed and `N` still give byte-identical parts.
- #25 puts this corpus on the lake. It does not change the allocation rule.
- ADR-0012 is reserved for the #40 boarding prior. This record is 0013.

## Sources

[ADR-0006](0006-amplify-for-scale-benchmarks.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0008](0008-speed-layer-streaming-shape.md), `docs/resampling-plan.md` §9.1, `docs/resampler-evaluation.md`, issues #34, #39, #40, #25.

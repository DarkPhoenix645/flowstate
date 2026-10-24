# ADR-0012: BMTC boarding prior as FLOWSTATE_ settings

## Status

Accepted. 2026-10-24. Ticket #40.

## Context

`flowstate.bus.scheduled_stop.v1` carries a `ridership` object (`boardings`, `alightings`, `onboard_leaving`) on every scheduled stop call. GTFS supplies the schedule and stop locations only. No public stop-by-stop boarding series, route-by-hour count, or APC dataset exists for BMTC. Every passenger number the resampler emits is therefore synthetic and assumption-driven.

The 12 October 2026 research report (`docs/bmtc-boarding-prior.md`, snapshot PDF under `docs/assets/bmtc/`) reviewed the published Bengaluru evidence. Two quantities can be borrowed as checks: the hour-of-day peak windows and the citywide trip-length range. The scale, the stop-level allocation, and the physical occupancy cannot: they stay model choices and engineering guardrails.

Before this ADR the numbers sat as constants in `ResampleConfig` (ADR-0010). That blocks sensitivity runs and hides the evidence class from the audit record.

## Decision

The boarding prior is a set of `FLOWSTATE_` settings read by `Settings` (`src/flowstate/config.py`) and carried into the resampler through `ResampleConfig.from_settings`. Empty means the default. A present value overrides. The bus summary audit block (`replay.bus.prior` in the amplify manifest) quotes the values in force, the evidence class, and the target year or regime.

| Setting | Default | Evidence class |
| ------- | ------- | -------------- |
| `FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS` | `4600000` (test `4000000`, `4800000`; FY2019-20 replay `3310000`) | Model choice between the CAG FY2019-20 base (3.31m) and a 2025 press restatement (4.8m). Not a measured weekday mean. The CMP lakh/day series is not a growth curve for this window |
| `FLOWSTATE_ASSUMED_BUS_CAPACITY` | `41.3` | Dated fleet-average seats (Karnataka Economic Survey 2018-19), only while the field means seats. Never seats plus standing |
| `FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR` | `1.4` (test `1.0`, `1.2`) | Synthetic guardrail. Not a statutory or observed peak load. CAG load factors are ticket-revenue ratios |
| `FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER` | `1.0` | No citywide causal substitution coefficient for the Yellow Line period. Do not lower it without a new cited source |
| `FLOWSTATE_BUS_HOUR_PROFILE` | `metro_ridehail_blend` | Fallback until a versioned, digitised 24-hour BMTC table exists. The blend gets the 47-51% peak-share check as a warning; a versioned curve mode would enforce it |
| `FLOWSTATE_BUS_PEAK_SHARE_LOW` / `_HIGH` | `0.47` / `0.51` | Peer-reviewed combined-peak share (Gadepalli et al. 2022, ETM samples Jan 2020 and Mar 2021). A check, not a fitted weight. Do not invent 24 hourly weights from the summary |
| `FLOWSTATE_BUS_TRIP_LENGTH_KM_LOW` / `_HIGH` | `10.7` / `10.9` | Historical citywide averages (CMP citing RITES 2016; Gadepalli citywide Jan 2020). Soft target only. Not the pandemic 9.5 km or the metro-area 5.7 / 6.2 km figures |

Adopted and rejected candidates, with reasons, are the keep/adopt/reject table in `docs/bmtc-boarding-prior.md`. The rejected list is binding: the 2016 CMP public-transport share is not a bus share, the 65.6% metro-adopter prior-mode figure is not a substitution rate, GTFS stop or trip counts are not demand, the rejected taxi files cannot tune bus demand, and CAG revenue load factors are not physical occupancy. None of these may appear as parameters.

## Consequences

- The stop call stays `derived`. The `ridership` object stays `synthetic` and `assumption_driven` (ADR-0008). No separate boarding topic exists.
- Dashboards, the map, and the report must not add these boardings to observed metro or ride-hail totals, and must quote the evidence class when they show the numbers.
- A scenario that changes the weekday-boarding value replays with the same seed. The audit records the target regime: the 4.6m centre is a 2025/26 model choice; 3.31m is the FY2019-20 CAG replay.
- A later rebase of the centre needs a new cited source and a new retrieval date recorded here. Do not silently replace the centre when a newer press figure appears.
- The evidence gap closes only with BMTC's own records (ticketing by route, trip, and hour including pass validations; a fleet master with type-approval seat and standee capacities; stop-level counts; a post-Yellow-Line route comparison). Until then the stream is a calibrated synthetic generator.

## Sources

`docs/bmtc-boarding-prior.md` (from the report of 12 October 2026), [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0011](0011-bengaluru-source-table-v2.md), [ADR-0013](0013-amplify-thins-the-alignment-window.md). Report references [R1]-[R11] are listed in `docs/bmtc-boarding-prior.md`.

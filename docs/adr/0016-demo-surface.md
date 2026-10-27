# ADR-0016: Demo surface

## Status

Accepted. 2026-10-25. Issue #67. Amended by [ADR-0017](0017-producer-as-a-realtime-twin-source.md): the catchment wards, the metro-disruption legs, the ward universe, and in-ward event points.

This ADR supersedes the observed-only restrictions in the #67 body. Those restrictions treated provenance as a reason to hide a twin quantity. Inside the demo, resampler output is the state of the twin. Provenance stays on every event as metadata about how it was made (status, seed, model version, evidence grade). It does not decide what the surface may show. Coverage, scale, and code size decide that.

The report (#32) and [ADR-0010](0010-bengaluru-calibrated-resampler.md) keep their honesty rules. This ADR does not change them. [ADR-0008](0008-speed-layer-streaming-shape.md) is not revised here. The 1-minute tumble stays. The 5-minute slide stays withdrawn.

Stretch (1e9 rows) is a profile name only. [ADR-0006](0006-amplify-for-scale-benchmarks.md) still caps `make amplify` at 1e8, so 1B cannot be generated. See Consequences.

## Context

#67 locked a catalog that cut live cancel rate, live surge, bus counts, and cross-mode scenario effects because no observed series stands behind them. The demo cannot show the twin under that rule.

The demo is two tiers. The historical tier is the corpus in Hive, drawn through rollup tables. The live tier is a deterministic replay of a window of that corpus. A scenario command forks the window. Wall-clock live simulation stays a producer mode in #39. It is not a v1 demo surface.

## Decision

### Questions

1. See the twin across all four modes.
2. Replay a window live: play, pause, scrub, and change speed.
3. What changes after one command, baseline against fork, on every mode.
4. Do children sum to parents, and what provenance does each event carry.

### Scale profile

The alignment window is 2024-10-26 through 2026-04-15 (537 calendar days). The city-day clock has 536 days. 2026-02-14 is missing. Ride-hail is about 98.9% of rows. Rates below use the 536 clock days. Ward-hour figures spread that mass evenly over 309 wards and 24 hours. Real ward-hours are uneven.

| Profile | Rows | Ride-hail rows/day | Per ward-hour | Per ward-minute | One simulated day at 1,000 eps |
| ------- | ---- | ------------------ | ------------- | --------------- | ------------------------------ |
| dev | 1,000,000 | 1,845 | 0.25 | 0.004 | 0.03 min |
| demo | 100,000,000 | 184,515 | 24.9 | 0.41 | 3.1 min |
| stretch | 1,000,000,000 | 1,845,149 | 249 | 4.1 | 30.8 min (3.1 min at 10,000 eps) |

One simulated hour at the demo profile is 7,688 ride-hail events. That is 7.7 s of wall time at 1,000 eps, and 15.4 s with a fork. At stretch and 1,000 eps it is 77 s, or 154 s with a fork. At 10,000 eps those stretch times return to 7.7 s and 15.4 s. The KIA-catchment cell is not in this table. The catchment wards are not named yet.

### Replay and fork

A replay request is `{bundle_version, seed, window_start, window_end, acceleration, max_eps, commands[]}`. The producer records it on `flowstate.simulation.audit.v1`. The same request yields the same `event_time` order. The baseline replay matches the corpus slice for that window. The check is a per-day hash.

The producer paces by event-time acceleration. `max_eps` is a hard ceiling. When the ceiling binds, the simulation clock slows. The producer drops no event and reorders none. The strip shows requested acceleration and effective acceleration.

`ridehail_day_events` is not `event_time`-ordered today. #39 emits per-hour batches in order.

Scrub is a seek. The producer restarts at the chosen `event_time` with the same seed and the commands that apply at that time. Random access uses the plan's per-bucket seeds. The map may buffer a few minutes for a short step back.

A replay has one fork. Baseline events carry `scenario_id=null`. Fork events carry one `scenario_id`. Several commands may share that id. A second id is rejected. Both streams are full streams on the same topic, so the rate against `max_eps` doubles. Consumers compare by grouping on `scenario_id`. They do not join.

Live Grafana uses `event_time` as the axis. The dashboard range is pinned to the replay window. A wall-clock range shows nothing during replay.

### Command and scenario policy

One command on `flowstate.scenario.command.v1`. One control publishes it. Fields: `mode`, `scenario_type`, `location_ref`, `start`, `duration`, `multiplier`, `scenario_id`. `scenario_type` selects the policy row. It is not a second control.

The policy file is `src/flowstate/resample/scenario_policy.json`. It carries the plan §8 fields plus `kia_catchment_wards`. `bundle_hash` includes the file. v1 enables `flight_arrival`, `flight_departure`, `metro_disruption`, and `custom_shock`. `local_holiday_event` stays off. A holiday is a `custom_shock` with a note.

`kia_catchment_wards` is an addition to plan §8. #39 checks each id against the emitted ward ids before a flight scenario runs. The flight story needs those ids only. It does not need polygons.

The command changes synthetic output after the command time. It does not rewrite an observed row.

`intensity_multiplier` is the policy default when the command omits `multiplier`. A present `multiplier` replaces it. The applied scale is at least 0.2 and at most `maximum_multiplier` (3.0). `evidence_grade` is `assumption_driven` on every type.

Yields are stored at observed scale. The applied yield is the stored yield times `amplification_factor`.

`amplification_factor = (ride-hail rows per day at the profile) / 137,319`.

137,319 is the observed Normal `done_ride` per clock day: alignment-window `done_ride` 74,073,643 minus Purple 470,413, over 536 days (`docs/data-sources.md` A6, `docs/agents/staged-tables.md`). #39 replaces this denominator with the city-day sum of `booking + done_ride + cancel_ride` when it wires the policy. Until then the factor uses the completion clock.

| `scenario_type` | Effect |
| --------------- | ------ |
| `custom_shock` | Scale synthetic output of `mode` at `location_ref` for `duration`. `multiplier` is the scale, after the floor and the cap |
| `metro_disruption` | `multiplier` is a suppression factor in `(0, 1]`. The remaining station-hour fraction is `max(multiplier, 0.2)`. `shift_fraction` (0.2) of the removed mass moves to ride-hail and bus. The ride-hail leg stays off until the station-to-ward crosswalk exists |
| `flight_arrival` | Inject or scale synthetic flights at VOBL for `duration`. `multiplier` scales the flight rate and is capped by `maximum_multiplier`. Each injected flight adds `rides_per_arrival` (4) observed-scale lifecycles, times `amplification_factor`, spread across `kia_catchment_wards`, lagged by `0-60min`. The ride-hail response attaches to injected flights only. Baseline flights carry none |
| `flight_departure` | Same shape as arrival, with yield 2, lag `30-90min`, and its own flight-rate scale. Independent of arrival |

A lifecycle is one booking plus one completion or cancel, at that ward-hour's baseline mix. Cancel rate stays flat. This ADR adds no cancel-propensity coefficient.

Placeholders, chosen so a demo can see the effect, not estimates: `shift_fraction=0.2`, `maximum_multiplier=3.0`, `rides_per_arrival=4`, `rides_per_departure=2`.

Worked `flight_arrival`. Ten injected arrivals in one hour, four catchment wards, yield 4 before the factor. Added lifecycles per catchment ward-hour = `10 * 4 * amplification_factor / 4`. Each lifecycle is two events.

| Profile | Factor | Added lifecycles / ward-hour | Added events / ward-hour | Baseline events / ward-hour |
| ------- | ------ | ---------------------------- | ------------------------ | --------------------------- |
| dev | 0.0134 | 0.13 | 0.27 | 0.25 |
| demo | 1.34 | 13.4 | 26.8 | 24.9 |
| stretch | 13.4 | 134 | 268 | 249 |

The event lift is about 1.1 times the even-spread baseline at every profile. An absolute yield of 4 does not do that. At stretch it would be about 4% of a 249-event ward-hour. At dev it would dwarf a 0.25 baseline.

### Coverage-gated aviation

A gap is an observed-overlay gap only. The twin's aviation series has no holes. A window that fails the coverage gate counts as not fetched. The generator emits synthetic flights there, labelled `synthetic`.

Today it does not do that. `normalize` marks every CSV row `observed_proxy`, including a thin fetched window (`src/flowstate/normalize/tables/flights.py`). A direction with no rows is absent. In `unfold.aviation_events` that direction's weight is ~0, so the resampler emits nothing there. A thin window therefore draws as a low `observed_proxy` bar. A missing window draws as no baseline.

Build item, resampler and #39: a failed or sparse window emits synthetic flights, labelled `synthetic`, and does not emit the thin rows as `observed_proxy`. Departures are degraded at the old end of the window (`docs/data-sources.md`, checked 2026-10-11). Those windows take this path. They are not limited to Aug–Sep arrivals.

### Station geography and wards

The 309 figure is the ward count in `trends_cumulative_ward_new_key.json` (ward × day counts, 2022-09-01 through 2023-07-06). That file has no boundaries. Emitted ward ids are the distinct `ward_num` values in `ward_hour`. That file has no boundaries either. `wards_bengaluru_bbmp_2022.geojson` has 243 polygons. It does not cover the emitted ids. v1 does not draw those polygons.

Ride-hail on the map is a ranked tile grid, one tile per emitted ward id, area or color by intensity. A name is shown when the ward join has one.

Metro uses the station geography table where lat/lon is present. The code file is 95 names and has no coordinates. Pins come from the Places cache. The ride-hail shift of `metro_disruption` needs a ward on each operational station (about 80). That link is a hand-assigned crosswalk of station id to ward id. It does not need polygons. Until the crosswalk exists, the shift stays off. Owner: the map tickets in #41–#51. #39 reads the crosswalk.

Bus uses the GTFS stops (about 9,960), which already have coordinates. Aviation is VOBL.

### Detectors

#29 and #30 stay separate jobs and separate sinks.

Cancel rate (#29) is cancels divided by bookings, per ward and 1-minute window. There is no driver/rider split. Cancel reasons stay `unknown`. A ride-hail scale and a flight lifecycle use the ward-hour's baseline mix, so cancel rate stays flat. That flat line is the sanity signal.

Surge (#30) is the z-score of ward demand against the #18 baseline. Demand is the hour sum of ride-hail activity. #30 reads `flowstate.ridehail.activity.v1` only. It does not read the aviation topics. The flight response is already on the ride-hail events.

[ADR-0008](0008-speed-layer-streaming-shape.md) allows one window in Spark: the 1-minute tumble. It does not allow a per-job hour window. This ADR does not change that. #30 tumbles 1 minute on the topic, sums those counts to the hour inside the job, and writes only the hour row. There is no 1-minute surge column. The job does not read the active-rides sink.

The baseline file is keyed by ward id × hour-of-day × day-type (weekday or weekend). Zone means ward id. #18 builds it from the twin corpus at the demo scale and seed. A baseline from observed clocks, from the stitched archive, or without day-type is invalid. The file stores `bundle_version`, `seed`, and the scale profile. Another scale or seed invalidates it. Without day-type, weekend hours sit low and weekday hours sit high, and the acceptance check below fails.

An append sink writes the hour row after the hour ends, plus the 1-minute watermark. That lag is the wall time in the scale section: 7.7 s at the demo profile and 1,000 eps, 15.4 s with the fork. The active-rides row is the 1-minute reaction. Surge is the hourly trend. The flight lag of 0–60 min sits on that same hour scale.

A 1-minute cancel rate or active-rides count is null unless the expected count in that minute is at least 5. At the demo profile the even-spread minute mean is 0.41, so most minute cells are null. The live panel defaults to the hour sum of active rides and of cancel rate, and to the hour surge row. The guard matters on the dev corpus.

Each streaming job caps itself at one executor core. That cap is an assumption for the demo profile: 1,000 eps base, one fork, about 2,000 eps into each job. It was not measured. At stretch, a fork at 10,000 eps base is 20,000 eps per job on that one core. Stretch cannot be generated under ADR-0006, so this ADR does not claim the cap holds there. If the worker cannot run three jobs, #22, #29, and #30 run as one Spark application with three queries and three sinks. #11 is merged. This ADR does not reopen it.

Sinks keep `scenario_id`. They also carry `evidence_grade`, `bundle_version`, and the scale profile. #30 acceptance adds: under baseline, on the demo window, the hour sink is not mostly nulls and not mostly extreme z-scores.

### Historical Grafana

Grafana stays two dashboards ([ADR-0009](0009-grafana-city-dashboard.md)). Historical panels read Hive rollups. They do not query raw corpus rows. #27 owns the rollups. A rollup is a Hive table or CTAS over partitioned parquet. No new job framework.

On a date where the twin was conditioned on an observed total, the twin matches that total by construction. The panel says so. The overlay is an index, or the panel prints the amplification factor. No silent dual axis. Tracker rows and RTI rows are never summed.

`resampler-evaluation.md` stores hold-out scores and charts. It does not store a hold-out simulation corpus. A twin-versus-real overlay on Aug–Sep 2025 is out of v1.

| Panel | Rollup | Grain | Overlay |
| ----- | ------ | ----- | ------- |
| Ride-hail lifecycle | `rollup_ridehail_day` | day: bookings, completions, cancels | City-day clock, as an index, on conditioned dates |
| Metro station-hour | `rollup_metro_station_hour` | station-hour entries and exits | RTI station-hours, same index rule |
| Metro daily total | `rollup_metro_day` | day | Tracker daily total, as an index |
| Bus | `rollup_bus_hour` | hour of day: boardings, alightings, peak `onboard_leaving`, mean `onboard_leaving` | None. The panel states the #40 prior |
| Aviation | `rollup_aviation_day` | day, by direction and provenance | None. `observed_proxy` and `synthetic` are separate series. A failed gate is synthetic, not a hole in the twin and not a low observed bar |
| Ride-hail hour shape | `rollup_ridehail_hour` | hour of day | City-hour archive as an index, shape only |

#25 runs `conservation_check` over the corpus, writes `rollup_conservation`, and the lake put is not complete until that table exists. #27 shows it as one stat panel. This is the scale showcase. The #18 baseline job is the other full-corpus batch workload.

### Live Grafana

Three rows. Each row is the city total, plus one ward variable. The variable defaults to the sum of `kia_catchment_wards`. The map covers every ward. A panel does not draw 309 series.

- Active rides, from `stream-sink/active_rides/`. Hour sum of the 1-minute rows is the default. This row is the instant reaction.
- Cancel rate, from `stream-sink/cancel_rate/`. Same hour default.
- Surge, from `stream-sink/surge/`. Hour grain only.

Each row splits baseline against the one fork by `scenario_id`.

### Map

The demo API aggregates the envelope per tick (ward, station, stop, airport) and returns a small sample of raw events for the inspector. The map does not draw one mark per event.

The scenario control is `baseline | fork | difference` over that same aggregate. Difference is fork minus baseline. It is the "what changed" view for metro, bus, and aviation, which have no live Grafana row.

Mode and provenance filters compose in the API.

The strip shows seed, bundle version, scale profile, requested and effective acceleration, and the status mix per mode. It shows aviation as `observed_proxy` against `synthetic`. `outside_training_window` appears only when the replay window crosses 2026-04-15. The strip says the view is a synthetic twin.

### Inspector

Events do not carry a parameters blob. The envelope gains `parameters_ref`, a key into the bundle parameter table. `schema_version` for events that carry it is `1.1`. ADR-0008 is not edited. This ADR adds the field. The demo API resolves the key. The inspector renders key/value pairs. It has no per-mode layout.

The #40 bus prior is the first `parameters_ref` entry. A value that varies per event (synthetic fare) stays an ordinary envelope field.

### Coverage

| Aspect | Historical | Live | Map |
| ------ | ---------- | ---- | --- |
| Ride-hail bookings, completions, cancels | `rollup_ridehail_day` | Cancel rate uses bookings and cancels. Active rides uses the activity count | Ward tile intensity |
| Metro entries and exits | Station-hour and daily rollups | None | Station points. Difference view for a fork |
| Bus boardings, alightings, onboard | `rollup_bus_hour` (counts plus peak and mean onboard) | None | Stop intensity. Inspector sample can show onboard. Difference view for a fork |
| Aviation arrivals and departures | `rollup_aviation_day` | None. #30 does not read these topics | VOBL. Difference view for a fork |
| Scenario effects | None. The corpus has no fork | All three rows, by `scenario_id` | `baseline \| fork \| difference` |
| Conservation | `rollup_conservation` stat | None | None |
| Provenance | Aviation series split. Other panels print the factor and the conservation note | `evidence_grade` on the sink | Strip and inspector |

No aspect is without a surface. Metro, bus, and aviation have no live Grafana row. The map difference view is their fork surface. Ride-hail is on all three surfaces at different grains (day, ward-hour, ward tick).

### What v1 cuts

- Wall-clock live simulation as a demo surface
- `local_holiday_event` as its own scenario type
- A 5-minute sliding window, and a 1-minute surge column
- A parameters blob on every event
- A twin-versus-real hold-out overlay
- A per-mode command form
- More than one fork id in a replay
- A 309-series live panel
- BBMP polygons as the ride-hail map
- A recommended mode, an itinerary, or an observed fare
- Drawing bus boardings as a BMTC count

## Acceptance criteria

- [ ] This ADR is the catalog. It names the four questions, the six historical panels and their rollups, the three live rows, the command fields, the policy file, and the map layers. v1 has nothing else on the demo surface.
- [ ] One command schema. `scenario_type` selects the policy. Flight commands inject or scale synthetic flights, and the ride-hail response attaches only to injected flights, scaled by `amplification_factor`.
- [ ] #29 and #30 stay separate jobs. Surge is hour-grain, summed from 1-minute tumbles, keyed against ward × hour-of-day × day-type. #30 reads ride-hail only.
- [ ] Replay and fork are demo surface. At most one `scenario_id` besides null. Live panels use `event_time`. The map has baseline, fork, and difference.
- [ ] #27, #28, #39, #41, #42, #44, #48, and #51 follow this ADR. They do not add a surface this ADR does not name.

## Consequences

- Stretch is 1e9 rows. At about 57 bytes per row that is about 57 GB. ADR-0006 caps `make amplify` at 1e8 (about 5.7 GB). Stretch is not available until that cap is amended. Demo scale is 1e8.
- #22, #29, and #30 implement the minute guard, the demo-scale core cap, and the sinks named here. #30 writes hour rows only.
- #39 implements ordered emission, the policy file, the amplification factor, the replay request, the single fork, and the coverage-gate aviation fix in the shared resampler path.
- #23 and #27 load Hive and build the rollups. #27 does not query raw rows from a panel.
- #25 owns `conservation_check` and `rollup_conservation`. The lake put is incomplete without that table. #27 draws the stat panel.
- The map tickets own the station-to-ward crosswalk and the tile grid.
- `docs/work-plan.md` carries the twin statement. Report text stays on ADR-0010.

## What the twin models

The twin emits ride-hail bookings, completions, and cancels. It emits metro station-hour entries and exits. It emits synthetic bus boardings, alightings, and onboard counts on a derived stop call. It emits aviation arrivals and departures. It emits scenario effects from the versioned policy.

The twin does not emit a true origin-destination, an observed fare, a driver-versus-rider cancel split, an airport metro line, or real BMTC ridership. Plan §2 and §11 state those non-claims. The report still uses them.

## Sources

#67, [ADR-0006](0006-amplify-for-scale-benchmarks.md), [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0009](0009-grafana-city-dashboard.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0012](0012-bmtc-boarding-prior.md), `docs/resampling-plan.md` §4.7 and §8–9, `docs/agents/watermarks.md`, `docs/agents/event-detector.md`, `docs/data-sources.md`, `docs/agents/staged-tables.md`.

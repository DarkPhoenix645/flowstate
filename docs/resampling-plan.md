# Final multimodal resampling plan (Bengaluru)

Normative v1.0 specification for the Bengaluru resampler and the Kafka producer. Decided 2026-10-10. Counts below were checked against the files on disk on 2026-10-11.

- Scope: Bengaluru auto ride-hail, metro, airport aviation events, schedule-derived bus activity.
- Decisions: [ADR-0010](adr/0010-bengaluru-calibrated-resampler.md), [ADR-0011](adr/0011-bengaluru-source-table-v2.md), [ADR-0008](adr/0008-speed-layer-streaming-shape.md).
- Source facts: [data-sources.md](data-sources.md). Excluded files: [rejected-sources.md](rejected-sources.md).
- Core contract: a real-time stream of clearly labelled synthetic mobility events calibrated to real aggregate data. Generated records are never presented as observed trips, real-time ridership, or validated causal responses.

## Counts checked 2026-10-11

These corrections are authoritative in [data-sources.md](data-sources.md):

- The metro "local vs public file" blocker is **resolved**: the local CSV is hash-identical to the current public file; 25 rows dated after the last upstream commit (2026-07-16) are invalid and excluded; the 343-day window overlap is confirmed.
- The RTI ∩ OpenSky intersection is **48 days** (RTI dates), not 61.
- `station_codes.csv` has **95** code→name entries (one malformed line), not 198. BMTC has **9,960** stops, not 9,961.
- OpenSky **departures are degraded at the oldest end** of the alignment window (79 flights in the 2024-10-26→27 window against ~330–375/day at mid and recent dates). Arrivals are available across the window. The departure backfill carries a per-window coverage gate.

## 1. Executive decision

Build a calibrated event resampler, not a neural mobility forecaster. Three layers: observed aggregate clocks, thin shape priors, seeded synthetic event unfolding. Publish through a conditionally stimulable Kafka producer; use historical data through the last valid ride-hail date; emit on the current wall-clock schedule with synthetic provenance.

| Decision | Final rule |
| --- | --- |
| City scope | Bengaluru only. NCR, Delhi, Chennai, Mumbai, and every rejected synthetic dataset are excluded from training and runtime. |
| Ride-hail clock | Namma Yatri city-day `booking_type=Normal` `done_ride`. Purple excluded from the main model. |
| Metro clock | BMRCL RTI station-hour workbooks are observed hourly evidence for their covered dates. The daily tracker is a separate daily signal only after its date/row integrity is resolved (resolved 2026-10-11, see addenda). |
| Aviation | Keep OpenSky arrival events; extend the historical extract to departures across the alignment window. `firstSeen`/`lastSeen` are estimated-time proxies. |
| DGCA | Monthly passenger traffic as context / consistency check. Never expanded into daily observations; passengers never equated with flights. |
| Model | Negative-binomial marginal count models plus residual dependence / copula coupling; Dirichlet-multinomial disaggregation. No RNN or Hawkes in v1. |
| Spatial layer | Historical ward-hour counts as a spatial prior only. No precise ride-hail coordinates until ward boundaries and in-ward sampling are validated. |
| Bus | Scheduled stop/trip events from the BMTC feed. Passenger demand stays assumption-based synthetic. |
| Streaming | Conditionally stimulable producer with deterministic replay, injected scenario commands, schema versioning, idempotent keys, explicit observed/proxy/synthetic provenance. |
| Training horizon | Alignment window 2024-10-26 → 2026-04-15. Freeze evaluation at 2025-07-31, test Aug–Sep 2025, then refit accepted parameters through the last valid date. |

Success for v1: plausible, reproducible, conditionally responsive streams whose daily/interval counts reconcile to the selected inputs, whose learned associations beat simple baselines on held-out periods where the data genuinely overlaps, and whose metadata makes it impossible to mistake synthetic records for observed ones. It does not reconstruct individual real journeys.

## 2. Product contract and non-goals

The system transforms sparse real aggregates, an imperfect hourly archive, schedule data, and scenario commands into a continuous stream of mode-specific synthetic activity. It is a synthetic simulation stream with a live clock, not a live sensor feed.

Record classes: **observed** (source observations with identity, event-time semantics, retrieval/coverage metadata), **derived** (normalized counts with transformation lineage), **synthetic** (generated records calibrated to observations or assumption-driven; always flagged, with model/config version), **scenario** (synthetic response to an injected event, keyed by `scenario_id`).

Non-goals: no reconstruction of individual real trips, itineraries, real fares, or booking histories; no claim that a generated ride has a physically true origin/destination or that a flight caused a particular ground trip; no bus passenger-count model presented as measured ridership; no "airport-adjacent metro station response" claim for 2025 (the airport Blue Line was under construction); no real-time flight feed claim from the OpenSky historical endpoints (nightly batch, previous days only).

## 3. Dataset inventory and final disposition

The accepted v1 source registry. "Conditional" means adopted for a defined role but must pass its named QA gate. Full windows, hashes, and roles: [data-sources.md](data-sources.md); the locked table is [ADR-0011](adr/0011-bengaluru-source-table-v2.md).

| Dataset | Grain and coverage | V1 role and treatment |
| --- | --- | --- |
| Namma Yatri cumulative city trends | city × day; 2022-09-01 → 2026-04-15; 1,323 BLR dates | Primary auto clock. Filter BLR + `Normal`; `done_ride` for totals. Operator-published aggregate, not independently audited. |
| Namma Metro daily tracker | day; 438 rows; 343 in-window (identity resolved 2026-10-11; rows dated after 2026-07-15 excluded) | Conditional daily metro target/baseline. Missing dates stay missing. |
| BMRCL RTI August station-hour | station × day × 24 buckets; 2025-08-01 → 08-18; 1,356 rows | Observed hourly profile. Account for Yellow Line opening 2025-08-11 (station count 69 → 83). |
| BMRCL RTI August entry-exit OD | station-pair × hour; 27,517 × 85 | OD shape prior only after schema/conservation checks. Not row-per-passenger trips. |
| BMRCL RTI September station-hour | station × day, Entry + Exit sheets; 2025-09-01 → 09-30; 2,491 rows/sheet | Observed hour-grain validation window. Entry/Exit separate. |
| Station code map | 95 code→name entries (one malformed line); no coordinates | Join key only. A versioned station geography table is built before geo-dependent generation. |
| OpenSky VOBL arrivals | event; 21,043 arrivals; 2025-08-01 → 09-30 | Observed aviation event proxies. Extend across the window. |
| OpenSky VOBL departures (to fetch) | event; target 2024-10-26 → 2026-04-15 | Required new event set, distinct from arrivals. Coverage-gated: departures are degraded at the oldest end (addenda). |
| DGCA domestic city-pair | month × directional pair; 2015-04 → 2026-05; 19 shared months | Monthly context and consistency check only. |
| Namma Yatri stitched city-hour | 66,726 rows; observed 2024-02-23 → 2026-05-10; 720 commits / 717 dates / 801-day span | Thin shape prior only (~1% of volume, near-zero correlation). Never scaled to city volume. |
| Namma Yatri stitched ward-hour | 3,043,468 rows; 459 commits / 456 dates; ends 2025-06-10 | Spatial allocation prior only. |
| Namma Yatri cumulative ward trends | ward × day; 309 wards; 2022-09 → 2023-07-06 | Coarse fallback prior, shrunk toward citywide, tagged extrapolated. |
| BMTC GTFS-like feed | schedule; 9,960 stops with coordinates; feed 2026-09-07 | Bus schedules / stop calls, not demand. Unofficial; upstream warns timings may be wrong and some routes missing. |
| Significant dates | 137 dates; 114 labels; from 2024-10-31 | Calendar candidate features only after locality classification. Never a default shock on every row. |

## 4. Detailed dataset decisions and caveats

### 4.1 Namma Yatri city-day clock

Use `Normal` as the main auto series. `done_ride` is the completed count; searches, bookings, cancellations are distinct daily metrics audited before any synthetic lifecycle. `drvr_cancel`/`rider_cancel` are zero-filled: excluded from fitting; no driver-vs-rider split is generated. `earning / done_ride` is named `earning_per_completed_ride_proxy`, never an observed fare. Purple is an accessibility product outside the main clock; recompute its share from the exact model window before publishing. "Complete daily series" means the date-level file is continuous, not trip-level coverage or an accuracy audit.

### 4.2 Daily Namma Metro tracker

Resolved 2026-10-11 (addenda): local file hash-identical to the public file; rows dated after the last upstream commit (2026-07-16) are invalid and excluded; formulas asserted; the 343-day overlap is confirmed. Missing dates stay missing; `Total` is never added to its components.

### 4.3 BMRCL RTI station-hour files

Adopted for observed station-level hourly counts on their actual coverage (48 days). Normalize to a long table with interval boundaries preserved (the final "23:00 to last train" bucket is its own interval). Check station-day coverage, missing cells (distinct from zeroes), duplicates, negative values, entry/exit semantics, cross-sheet totals. The 10–25% tracker gap stays a hypothesis until station sets and definitions are compared. The Yellow Line opened 2025-08-11: encode an effective-date regime and validate pre/post separately. The OD workbook is a probabilistic prior only.

### 4.4 OpenSky arrival and departure events

Retain the 21,043 arrivals as `observed_proxy` events. Extend arrivals and fetch departures across the alignment window (arrival feasibility verified; departure degradation per the addenda). Canonicalize both; keep `(icao24, firstSeen)` per direction; log duplicates and nulls; UTC kept, IST bins derived; per-window query status persisted (200 / 404-empty / failed / quota are distinct). No passenger counts, occupancy, or verified touchdown/wheels-off. Budget: 269 windows per direction at 30 credits (~8,070 each); ~15.2k incremental after reusing cached Aug–Sep arrivals; checkpoint every window.

### 4.5 DGCA monthly passenger traffic

19 shared months: too short for a high-capacity seasonal model or a monthly causal story. Filter to Bengaluru-involving domestic pairs with a documented directional rule; no double counting; no mixing with international quarterly figures. Exploratory low-frequency covariate only if it improves an out-of-sample score. ODbL 1.0 with DGCA / Ministry attribution in the manifest.

### 4.6 Namma Yatri city-hour and ward-hour stitch

720 commits on 717 dates across an 801-day span: 84 missing dates, 3 duplicate dates, 1 empty commit skipped. City-hour: 4,883 of 95,775 repeated keys change on overwrite (5.1%); ward-hour: 174,618 of 5,665,652 (3.1%). Last-write-wins revises history: build coverage manifests from commit IDs and changed keys; never infer one-complete-observation-per-day. The city-hour slice is ~1% of city-day volume with near-zero correlation; the ~22% cab share is slice-internal, an uncertain prior with sensitivity analysis. For honest temporal evaluation, build as-of-cutoff stitches: a revision is usable only if the snapshot carrying it existed by the model cutoff.

### 4.7 BMTC feed, station geometry, and other spatial inputs

GTFS is schedule conditioning, not observed activity. Validate every trip against calendars, stop_times, stop ids, coordinates, plausible durations. The stop call is `derived`. Boardings and alightings on that call are synthetic. The station-code file is names only: build a versioned station geography table (ids, coordinates, lines, effective operational dates). Do not place ride-hail points from a ward id until a validated BBMP ward polygon/centroid layer exists; until then emit ward ids and omit coordinates. Classify significant-date labels (Bengaluru-local / citywide holiday / nonlocal / uncertain); a coefficient is used only with enough in-sample support and held-out performance. Weather, strikes, and real metro disruption labels are absent.

## 5. Excluded datasets

Rows 1 (NCR Uber), 2, and 8 (Bengaluru booking CSVs): permanently dropped from training, calibration, and the source table (flat rates, Poisson-like noise, round totals). Delhi metro synthetic, Indian flight-delay synthetic, Cityflow bus, Bangalore traffic simulation, Bangalore Rapido, Indore Ola, Rapido all-data, Ola bike requests, row-1 clones, relabelled Seattle/Grab and DC bikeshare: excluded. XRides 2013 and Rapido 2018 coordinates: no v1 role. Delhi, Chennai, Kolkata, Mumbai groups: out of scope. Audit trail: [rejected-sources.md](rejected-sources.md). `DATASET=all` cannot download or validate a rejected source.

## 6. Mandatory reconciliation before model fitting

| Gate | Required test / correction | Release condition |
| --- | --- | --- |
| G1 Metro identity/date | Hash the file; compare with upstream; gaps, duplicates, future dates; rebuild the shared-date join | No unresolved future date; overlap regenerated from the actual file (done 2026-10-11) |
| G2 Hourly archive arithmetic | Commit-level manifest; missing/duplicate dates; overwrite statistics | Accounts for 720/717/801/84/3 or corrected values |
| G3 Shared-time coverage | Intersections by actual date/hour/station; coverage masks per source | No nominal-window claims; RTI documented as 48 days |
| G4 Metro source comparability | Common-definition totals; active station sets; entry/exit measures; Yellow Line regime | 10–25% gap explained or explicitly unresolved; no invented scale factor |
| G5 Aviation timestamp/query audit | Raw UTC kept; response status, coverage, duplicates, timezones per direction | All windows complete or explicitly failed/unknown; no proxy labelled true touchdown |
| G6 No look-ahead leakage | As-of-cutoff reconstruction; test dates excluded from profile fitting; selection separated from final refit | Holdout estimates nothing in the scored experiment |
| G7 Metric semantics | Counts vs field glossary; metro identities asserted | Only supported ratios fitted; zero-filled splits and fare claims excluded |
| G8 Spatial and service validity | Station map, operational dates, ward boundaries, GTFS calendars | No event at a nonexistent/closed station; no coordinates without validated anchors |

## 7. Final modelling architecture

### 7.1 Layer 1 — aggregate clock and dependence

No neural RNN, LSTM, transformer, or Hawkes model on the v1 critical path: the evidence is mostly aggregate counts. Fit NB2 marginals for (a) Namma Yatri Normal completed rides and (b) the validated metro tracker total. After the backfill, add daily arrival and departure counts as candidate covariates. Weekday effects, low-dimensional trend, verified local holiday indicators, Yellow Line regime flag.

```
Y[m,d] ~ NegativeBinomial2(mu[m,d], phi[m])
log(mu[m,d]) = intercept[m] + weekday[m,d] + smooth_trend[m,d]
             + verified_local_event[m,d]
             + gamma_arr[m] * log(1 + arrivals[d])
             + gamma_dep[m] * log(1 + departures[d])
             + network_regime[m,d]
```

Flight terms are association/prediction terms, not causal effects: include only on time-held-out improvement. Incomplete query windows never become zero-flight days. Couple auto and metro residuals after calendar/trend structure via a Gaussian copula (or one common low-dimensional latent factor), fitted on shared valid dates only; missing metro dates are not imputed as zero; AR(1) residual extension is an optional comparison. In reconstruction mode a known source total replaces the resampled total; the generator distinguishes "conditioned on observed total" from "sampled total".

### 7.2 Aviation at daily and monthly grain

Arrival and departure counts aggregated to Bengaluru local day are event-count covariates after the full window is fetched and coverage-checked. Event rows are retained for replay and hourly profiles. DGCA stays a separate low-frequency series for description and consistency. Preregistered same-day and small lag sets; selection on training only; no fishing.

### 7.3 Layer 2 — temporal shape prior

Smoothed 24-bin profiles separately for auto and cab where the split exists, with weekday/weekend shrinkage toward the global profile. Metro: RTI Entry and Exit separately, by station and day-type, pooled by hierarchy with the active network as of each date.

```
p_hour ~ Dirichlet(kappa * profile_hour)
counts_hour | N_day, p_hour ~ Multinomial(N_day, p_hour)
assert sum(counts_hour) == N_day
```

`kappa` selected by profile stability / held-out fit. Fallback hierarchy: station-by-day-type → line-by-day-type → system-wide day-type global, with explicit confidence tags.

### 7.4 Layer 3 — event unfolding by modality

**Ride-hail (auto and cab).** Anchor synthetic auto completed counts to NY Normal daily `done_ride` in reconstruction mode; sample only in simulation mode. Divide totals across hours with the stitched profile, preserving the daily sum exactly. Generate booking/activity rows from daily `booking`/`done_ride`/`cancel_ride` targets after an arithmetic check; do not force `booking = completed + cancelled` unless verified; if counts do not reconcile, emit separate metric event types. Cancel reasons are a synthetic categorical prior only, never fit from rejected CSVs; driver/rider split stays absent or `unknown`. Cab events, if required, sample a cab-to-auto ratio from a broad configurable prior informed by the post-2025-06 slice; the ~22% is slice-internal. No `fare` as observed: `synthetic_fare_inr` from a versioned assumed distribution; `earning_per_completed_ride_proxy` is a weak calibration target only.

**Metro.** Preserve observed RTI aggregates as `observed_aggregate`; do not duplicate them. For synthetic days, start from a known or sampled daily total and allocate across hours/stations with RTI profiles and the station network active on that date; label extrapolated stations. Emit `metro.station_hour` records with entries/exits; no per-person OD journeys in v1. RTI station-hour totals and tracker dailies are never additive feeds.

**Bus.** One topic, `flowstate.bus.scheduled_stop.v1`. Each record is one validated GTFS stop call (`Asia/Kolkata`, route/trip/stop ids, feed version) plus a synthetic ridership object on that same record. There is no `bus.boarding_estimate` topic. Scheduled times are not vehicle positions. The stop call is `derived`. The ridership object is `synthetic` and `assumption_driven`.

The ridership object has `boardings`, `alightings`, and `onboard_leaving`, all integers. For each trip, in stop-time order: boardings along the trip equal alightings along the trip, `onboard_leaving` is never negative, and `onboard_leaving` never exceeds `assumed_bus_capacity * assumed_max_load_factor`. Both capacity and load factor are required fields of the model bundle. They are assumptions. They are not BMTC measurements.

The resampler builds the counts as follows. It does not fit them to a bus boarding series. None exists in the accepted sources.

1. **Supply.** The GTFS trip and its ordered stop sequence are the only bus facts. Trip frequency is a service plan. It is not a person count.
2. **When.** The hour weight is a mixture of two shapes already in the bundle: the metro system exit profile for that day-type, and the Namma Yatri city-hour completed-ride profile. The mixture weights are config (`bus_hour_weight_metro`, `bus_hour_weight_ridehail`). They sum to 1. They are not estimated.
3. **How many.** A config field `assumed_weekday_boardings` is the citywide synthetic boarding total for a weekday. Other day-types scale it by config multipliers. The total is allocated across that day's trips in proportion to the hour weight. This total is not calibrated to BMTC. Replace the number only with a sourced range.
4. **Where along the trip.** v1 uses one load shape: boardings concentrated on the early stops, alightings on the late stops, with the peak `onboard_leaving` inside the capacity cap. Stop attraction is uniform. A later bundle may replace attraction with a sourced land-use or stop-importance weight. Frequency is not that weight.
5. **Metro corridors.** Each stop can carry a distance to the nearest metro station once the station geography table exists. `metro_substitution_multiplier` defaults to 1. A value below 1 is allowed only as a versioned assumption tied to a cited source. The Yellow Line regime flag is available to that multiplier. It does not change the multiplier by itself.
6. **Scenarios.** A scenario command may scale the synthetic ridership fields after the command time. It must not alter the GTFS stop call.

Dashboards may plot the synthetic boardings. They must not label them as BMTC ridership or add them to observed metro or ride-hail totals.

**Aviation.** Observed historical arrival/departure proxies keep their source timestamps and status. Outside fetched windows, flight events are synthetic draws conditioned on calendar/time-of-day profiles and scenario commands; DGCA constrains monthly context only. Arrivals and departures are distinct types with distinct profile/response parameters. The runtime never presents the batch API as a live feed.

## 8. Conditional stimulation and scenario engine

The producer is conditionally stimulable: an injected command generates a baseline stream and a separate conditional stream. Historical observations are immutable; intervention effects apply only to synthetic output after the command time.

| Scenario | Evidence status in v1 | Final behavior |
| --- | --- | --- |
| Flight arrival at KIA | Daily association testable after backfill; hourly cab response weakly supported; no airport metro station in 2025 | Configurable bounded response to synthetic airport-access cab/auto; metro response disabled by default or explicitly assumed |
| Flight departure from KIA | Separate timing/counts after backfill; no observed linked cab trip | Distinct configured response window/profile, independent of arrival effects; calibrate only daily association that passes holdout |
| Metro disruption | No disruption labels in accepted sources | Scenario rule suppresses affected synthetic station-hours and shifts a configured share to auto/cab/bus; fractions are versioned assumptions |
| Local holiday/event | Mixed calendar source; few learnable events | Feature only after locality classification and support; otherwise metadata or configured multiplier |
| Custom shock | No empirical basis | User supplies mode, geography, start, duration, magnitude; response is `assumption_driven`, tied to scenario id |

`scenario_policy` fields: `scenario_type`, `enabled`, `affected_modes`, `time_lag_profile`, `intensity_multiplier`, `shift_fraction`, `maximum_multiplier`, `evidence_grade`, `evidence_window`, `notes`. Changing a scenario is a config/version change. Use "scenario response" wording unless a measured, confounder-adjusted relationship survives temporal holdout.

## 9. Stream contract and runtime behavior

### 9.1 Modes of operation

| Mode | Input totals | Timestamps and guarantee |
| --- | --- | --- |
| Historical reconstruction | Valid observed aggregates where present; synthetic disaggregation fills lower grain | Historical event_time; emission time is replay time; exact parent-to-child conservation |
| Deterministic replay | Fixed model bundle, scenario config, seed, clock | Identical inputs/config/seed → identical sorted output |
| Live simulation (default demo) | Current-date synthetic totals from the frozen model; scenario commands if received | Wall-clock cadence; `provenance.status=synthetic` throughout |
| Scenario fork | Cloned baseline seed/config plus injected commands after the trigger | Baseline and intervention share initial seed/parameters; events tagged with scenario id |

The ride-hail clock ends 2026-04-15: emission after that is extrapolation from the frozen model. Freeze the trend at the terminal level; widen uncertainty; set `outside_training_window`.

### 9.2 Topics

| Topic | Record semantics |
| --- | --- |
| `flowstate.ridehail.activity.v1` | Synthetic booking/completed/cancelled activity. Never source-proven trips. |
| `flowstate.metro.station_hour.v1` | Station × interval counts, entries/exits separate, observed or synthetic provenance. |
| `flowstate.bus.scheduled_stop.v1` | One GTFS stop call (`derived`) plus synthetic `boardings`, `alightings`, and `onboard_leaving` (`assumption_driven`). |
| `flowstate.aviation.arrival.v1` / `flowstate.aviation.departure.v1` | Observed historical proxies during fetched windows; synthetic outside. |
| `flowstate.scenario.command.v1` | Validated stimulation events. |
| `flowstate.simulation.audit.v1` | Run start/stop, hashes, seed, coverage, counts, QA, backpressure. |

### 9.3 Common event envelope

```json
{
  "schema_version": "1.0",
  "event_id": "deterministic-or-unique-id",
  "city_id": "blr",
  "mode": "ride_hail | metro | bus | aviation",
  "event_type": "completed_ride | booking_created | station_hour | scheduled_stop | arrival | departure",
  "event_time": "2026-10-10T18:00:00+05:30",
  "emitted_at": "2026-10-10T18:00:01+05:30",
  "count": 1,
  "location_ref": {"ward_id": null, "station_id": null, "stop_id": null, "airport": "VOBL"},
  "scenario_id": null,
  "provenance": {
    "status": "observed | observed_proxy | derived | synthetic",
    "source_ids": ["namma_yatri_city_day_v1"],
    "model_bundle_version": "resampler-1.0.0",
    "profile_version": "profiles-1.0.0",
    "seed_id": "seed-or-run-id",
    "evidence_grade": "aggregate_calibrated | shape_prior | assumption_driven",
    "outside_training_window": true
  }
}
```

Metro records use explicit `entries_count`/`exits_count`, interval start/end, and source observation id. Bus records add `ridership.boardings`, `ridership.alightings`, `ridership.onboard_leaving`, and `ridership.provenance` (`status=synthetic`, `evidence_grade=assumption_driven`). The parent event provenance for the stop call stays `derived`. Generated booking entities carry a synthetic booking id; no real-world person identifier. Idempotent message keys; event ids reproducible from run seed, scenario id, topic, and deterministic index under replay.

### 9.4 Runtime controls

The producer loads a validated, immutable model bundle (parameters, coverage manifest, profiles, GTFS version, scenario policy, code version, hashes). `flowstate.provenance.require_valid()` gates every dataset read; an incomplete OpenSky extract is never valid. `DATASET=all` is not an unattended quota-consuming backfill: checkpoint and resume by completed window. Interval generation is idempotent with bounded batches and backpressure; event time and emission time stay distinct; observed aggregates and synthetic children are separated so dashboards cannot double-count; every generated child carries its parent aggregate/source id; seeds are explicit and stable per run/scenario/bucket; commands and outputs are replayable from the audit topic.

## 10. Validation and acceptance criteria

### 10.1 Evaluation protocol

Two stages: a strict leakage-free historical test, reported first; then an optional final refit on the full valid window for current-time simulation.

| Track | Fit / validation data | What is scored |
| --- | --- | --- |
| A. Daily demand / auto-metro dependence | Fit 2024-10-26 → 2025-07-31; holdout 2025-08-01 → 09-30; observed shared dates only; pre/post Yellow Line split at 08-11 | MAE, Poisson deviance or CRPS, interval coverage, residual autocorrelation, observed-vs-simulated dependence; seasonal-naive and calendar-only baselines |
| B. Metro hourly/station profile | Fit RTI profile on Aug 1–18 only; validate on September; common station universe; Entry/Exit separate | Station-hour profile error, total conservation, station calibration, weekday/hour stability |
| C. Aviation effect exploration | Aggregate arrivals/departures to local bins after backfill; fit on training dates; Aug–Sep bins are held-out inputs | Incremental predictive value for daily auto/metro; exploratory citywide metro hourly response; date-blocked uncertainty |
| D. Hourly ride-hail shape | Thin archive with vehicle split; as-of-cutoff versions for backtests | Profile stability, share uncertainty, sensitivity of output to shape/cab ratio |
| E. Synthetic contract | Reconstruction, replay, scenario tests on fixed seed/config | 100% integer sum conservation; replay equality; no duplicates; flags present; ids valid; no false observed labels |

### 10.2 Baselines and pass/fail rule

Baselines: weekday-specific median / seasonal-naive lag-7; calendar-only NB2 without flight predictors or coupling. Primary score: held-out CRPS where supported, else NB log score; MAE and deviance secondary; correlation alone never passes. Promotion: keep aviation terms or richer dependence only on ≥5% held-out primary-score improvement over the best simple baseline without materially worse calibration; report date-block bootstrap uncertainty. Dependence: report weekday/trend-adjusted residual correlation with an interval; preserve in the sampler only if stable under reasonable splits and reproduced within uncertainty; else shrink toward zero. Metro hourly: September must beat a simple August-derived baseline on a declared metric; report pre/post Yellow Line coverage. Scenario: bounded, repeatable, schema-valid, directional only per declared policy; a passed software test never validates a real-world effect.

### 10.3 Minimum automated tests

Metro component identities; NY key uniqueness and Normal/Purple filtering; correct denominator for every published share; actual date/time intersections per source pair (missing, duplicate, out-of-range, future-dated); UTC→IST around midnight, month boundaries, query boundaries; distinct arrival/departure types; allocator conservation (nonnegative integers, exact parent sums); no output station/route/stop before its effective date; explicit event classes (no synthetic marked observed; no aggregate counted with its children); deterministic seed/replay equality; scenario commands never mutate historical records; missing RTI hours and snapshot dates stay missing; no partial OpenSky extract accepted as complete.

## 11. What the report can and cannot claim

| Claim | Permitted wording |
| --- | --- |
| Daily auto/metro relationship | "Conditional daily dependence from shared observed aggregate dates, evaluated on a temporal holdout." Stronger only if overlap and scores support it. |
| Daily flight/auto after backfill | "We test whether daily arrivals/departures add predictive signal for daily auto activity." Association, not causation. |
| Aviation/metro hourly | "An exploratory citywide station-hour association over the available RTI dates." Never "airport metro response". |
| Cab mode share | "The thin archived slice has an estimated cab share near 22%; used as an uncertain shape prior." Never "Bengaluru's true cab share is 22%". |
| Ride-hail event attributes | "Calibrated to platform-level aggregate counts/rates." Never "learned from real bookings" or "observed fare". |
| Bus | "Schedule-derived stop events; passenger quantities are assumption-based." Never "real bus ridership". |
| Live stream | "Real-time stream of synthetic events on the current simulation clock." Never "real-time observed ridership". |

## 12. Implementation order and ticket boundaries

| WP | Deliverables | Done when |
| --- | --- | --- |
| WP0 source-table amendment / QA gate | Drop fake rows and out-of-scope cities; add accepted sources with roles and limits (ADR-0011) | G1–G8 pass or report exact residuals; rejected sets inactive |
| WP1 normalizers and analysis tables | Canonical city-day, metro-day, station-hour, OD profile, flight events, coverage reports (ticket T33) | Reproducible deterministic tables with schemas, counts, hashes |
| WP2 model baseline | NB2 dailies with calendar controls; residual coupling; flight ablation; held-out baselines (T32) | Validated model or documented fallback; no unearned causal claim |
| WP3 shape and event unfolding | City-hour and station-hour profiles; ward allocation; deterministic ride-hail metrics/events (T32) | Parent totals preserved; provenance correct; cab/fare marked assumed |
| WP4 BMTC schedule events + aviation backfill | Validate GTFS; fetch/checkpoint full-window arrivals and departures (T14, T34) | Coverage manifests prove observed vs missing vs synthetic |
| WP5 stimulable producer | Command schema; scenario policy; forks; idempotent keys; audit (T35) | End-to-end scenario demo; consumers filter by provenance |
| WP6 validation/report release | Holdout results, evidence-grade matrix, limitations, attributions, reproducibility (T31) | Claims match validation; release includes config, manifest, seed |

Release criteria: no model output released until metro dates and archive coverage are reconciled (done 2026-10-11 for metro); no default airport-metro-response demo — the first demo is a flight injection changing a configured synthetic airport-access ride-hail response, marked assumption-driven unless validated; baseline-vs-scenario comparison, conservation check, and provenance view in the demo; after model selection the final refit may use all validated data through 2026-04-15 with holdout results frozen.

## 13. Provenance manifest and reproducibility

Every normalized dataset and model bundle carries: `dataset_id`/version (schema, revision, path, hash); origin (producer vs archive vs authoritative URL/resource id); retrieval and query (retrieved_at, script version, endpoint, parameters, windows, timezone, quota/status); grain and semantics (row meaning, intervals, field dictionary, timestamp meaning, count definition, active universe); coverage (min/max, counts, missing, duplicates, future dates, nulls, pairwise overlaps); transformation (script commit, filters, joins, formulas, before/after counts, overwrite strategy, derived columns); role and limitations; rights and attribution; model bundle (training window, features, metrics, coefficient/profile hashes, seed policy, code commit, input-manifest hashes). A lightweight provenance graph runs from raw source to generated topic; `flowstate.provenance.require_valid()` stays the gate.

## 14. Residual risks and controls

| Risk | Why it matters | Control |
| --- | --- | --- |
| Sparse metro coverage | 343 shared daily dates; count may shift after cleanup | Paired valid dates only; propagate missingness; CIs; baseline-first |
| Short hourly metro test | Aug 1–18 + Sep with network expansion mid-window | Exploratory hour level; matched stations; regime indicators |
| Thin ride-hail hourly slice | ~1% of volume, near-zero correlation | Shape only; uncertain vehicle share; never multiply to city scale |
| No airport metro connection in period | Cannot observe KIA-station metro arrivals | Airport-station claims and direct response disabled by default |
| No individual trip data | No OD, fare variance, mode-choice transitions | Synthetic attributes labelled as assumptions; aggregate interval records preferred |
| Metro input mismatch | RTI vs tracker 10–25% | Never merge/sum; identify differences or keep two measurements |
| Future extrapolation from Apr 2026 | Emitting in Oct 2026 is outside the ride-hail window | Freeze/regularize trend; widen uncertainty; outside-window flag |
| GTFS schedule quality | Unofficial, may be wrong/incomplete | Validate the feed. Stop call stays `derived`. Passenger fields stay synthetic and quote the assumed citywide total, capacity, and load factor. |
| OpenSky batch/quota failure | Partial pulls masquerading as absence | Checkpoint per window; store status; valid only on full coverage |
| Departure backfill degradation | Oldest windows return ~11% of expected departures | Per-window coverage gate; sparse ≠ zero; sensitivity analysis on flight covariates |

## 15. Final implementation checklist

- [x] Record the three-layer resampler in ADR-0010. A neural model is not part of v1.
- [x] Amend the locked source table; remove synthetic rows and out-of-scope cities (ADR-0011).
- [x] Fix the metro local/public identity and date range; exclude future-dated rows; recompute overlaps.
- [ ] Generate the snapshot coverage manifest for the 720-commit city-hour archive and its overwrite history.
- [ ] Normalize the RTI station-hour and OD workbooks; establish the 48-day nominal overlap and record-level intersection.
- [ ] Add station operational-date metadata and the Yellow Line regime marker (2025-08-11); no airport metro station before operation.
- [ ] Backfill OpenSky arrivals and departures over the alignment window with resumable windows and auditable completeness.
- [ ] Fit simple count baselines; evaluate flight predictors and auto-metro coupling on the frozen holdout.
- [ ] Build hour/station/ward profiles with Dirichlet-multinomial sampling; enforce exact integer conservation.
- [ ] Generate ride-hail activities, bus scheduled stops, and aviation events with source/assumption tags.
- [ ] Implement stimulation commands and scenario forks; record commands, seeds, versions, output manifests.
- [ ] Freeze evaluation results; refit after selection; present the live topic as synthetic activity.

## 16. Public references

Source facts and file paths are in [data-sources.md](data-sources.md). Excluded datasets are in [rejected-sources.md](rejected-sources.md).

- [R1] Namma Yatri Open Data — https://nammayatri.in/open/
- [R2] thecont1, Namma Metro ridership tracker — https://github.com/thecont1/namma-metro-ridership-tracker
- [R3] OpenCity, BMRCL station-wise ridership (RTI; credit Vivek Mathew; CC BY-NC-SA 4.0) — https://data.opencity.in/dataset/bmrcl-station-wise-ridership-data
- [R4] OpenSky REST API documentation — https://openskynetwork.github.io/opensky-api/rest.html
- [R5] Vonter, india-aviation-traffic (DGCA; ODbL 1.0) — https://github.com/Vonter/india-aviation-traffic
- [R6] Vonter, BMTC GTFS — https://github.com/Vonter/bmtc-gtfs
- [R7] PIB, Yellow Line inauguration (10 August 2025; public operations 11 August 2025)
- [R8] Indian Express, airport Blue Line status (August 2026)
- [R9] Schäfer et al. (2014), "Bringing Up OpenSky", IPSN, pp. 83–94 — https://doi.org/10.1145/2591513.2591540
- [R10] GTFS Schedule Reference — https://gtfs.org/documentation/schedule/reference/
- [R11] DigitalIndiaArchiver/NammaYatriStats — https://github.com/DigitalIndiaArchiver/NammaYatriStats
- [R12] significant-dates-in-bangalore-india (Kaggle) — https://www.kaggle.com/datasets/maheshshantaram/significant-dates-in-bangalore-india

Rights note: non-commercial college/research use. Preserve BMRCL/OpenCity attribution and the OpenSky citation now; complete license/version review before public redistribution.

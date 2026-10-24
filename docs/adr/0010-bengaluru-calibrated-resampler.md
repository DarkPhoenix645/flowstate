# ADR-0010: Bengaluru calibrated multimodal resampler

## Status

Accepted. Revised 2026-10-11. Bus prior revised 2026-10-24 (ADR-0012).

## Context

The product wants a stream and a lake of multimodal Bengaluru mobility events, calibrated to real data, with the same process feeding batch and streaming. What exists as evidence is mostly aggregate counts: Namma Yatri city-day autos, a daily metro tracker, 48 days of station-hour metro, monthly DGCA, and OpenSky arrival events for Aug–Sep 2025. There are no ride-level bookings, no per-trip fares, and no bus ridership. A neural point-process model would add parameters without matching the grain of the evidence.

## Decision

Build a **calibrated event resampler** in three layers:

1. **Layer 1, aggregate clocks (day grain).** Negative-binomial (NB2) marginals for Namma Yatri Normal `done_ride` and the validated metro tracker total, with weekday effects, smooth trend, verified local event flags, a Yellow Line regime flag (opening 2025-08-11), and flight counts as candidate covariates. Couple auto and metro residuals with a Gaussian copula (or a simple common latent factor) fitted on shared valid dates only. An RNN, LSTM, or Hawkes model is a future candidate, not v1.
2. **Layer 2, shape priors.** Hour-of-day profiles from the stitched city-hour archive (shape only, never scale; it is ~1% of city volume). Station-hour metro profiles from the RTI workbooks, Entry and Exit separate, with a shrinkage hierarchy and the station network active on each date. Cab share (~22%) is an uncertain prior from the thin post-2025-06 slice, not a citywide fact.
3. **Layer 3, event unfolding.** Dirichlet-multinomial allocation that preserves parent totals exactly. Ride-hail activity events from daily `booking` / `done_ride` / `cancel_ride` targets; cancel reasons stay `unknown` (source split fields are zero-filled); fare, if required, is `synthetic_fare_inr` from a versioned assumption, never an observed fare. Metro station-hour records from known daily totals. Bus events are BMTC GTFS stop calls. Each call carries synthetic `boardings`, `alightings`, and `onboard_leaving` from the bus-load feature in [resampling-plan.md](../resampling-plan.md) §7.4. Those counts are not observed ridership. The prior behind them is [ADR-0012](0012-bmtc-boarding-prior.md): the hour shape (combined peaks 08:00-11:00 and 17:00-20:00 at 47-51%) and the trip-length band (10.7-10.9 km citywide) are borrowed from published Bengaluru evidence as a warning check and a soft target. The scale (`FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS`), the seat count (`FLOWSTATE_ASSUMED_BUS_CAPACITY`), and the load-factor guardrail (`FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR`) are env-backed model choices. `FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER` stays 1.0. Aviation events are observed OpenSky proxies inside fetched windows and synthetic draws outside.

Operating modes: historical reconstruction (exact parent totals), deterministic replay (fixed seed and config), live simulation on the wall clock, and scenario forks. The same model bundle, seed policy, and profiles serve `make amplify` and the Kafka producer. `make amplify` reconstruction thins every source day in the alignment window ([ADR-0013](0013-amplify-thins-the-alignment-window.md)). Live wall-clock emit is the producer ticket (#39), not amplify. Synthetic bus boardings read the #40 `FLOWSTATE_` settings through `ResampleConfig.from_settings` (ADR-0012).

Provenance: every event carries `observed | observed_proxy | derived | synthetic`, source ids, model/profile versions, and an evidence grade. Synthetic records are never presented as observed trips. Emission after 2026-04-15 is extrapolation: trend frozen at the terminal level, uncertainty widened, `outside_training_window` flagged.

Evaluation: fit 2024-10-26 → 2025-07-31; hold out Aug–Sep 2025 (48 RTI days, pre/post Yellow Line split); keep aviation terms or richer dependence only if the held-out primary score (CRPS, else NB log score) improves ≥5% over the best simple baseline without worsening calibration. After model selection, refit through 2026-04-15 with the holdout results frozen.

Streaming contract: the `flowstate.*.v1` topic set and the event envelope are [ADR-0008](0008-speed-layer-streaming-shape.md) (revised). Scenario commands and their assumption-driven responses are ADR-0008 scope.

## Consequences

- No RNN/Hawkes on the v1 critical path. Reintroducing one needs evidence that beats the NB2 baseline under the same holdout rule.
- The residual-coupling estimate is the report's cross-modal quantity. It is association, not causation, unless a held-out test says more.
- Missing dates, missing station-hours, and failed OpenSky windows stay missing. Nothing is zero-filled to look complete.
- Reports must quote seed, model bundle version, and which windows are observed versus synthetic.
- ADR-0006's `N` still counts multimodal events from this resampler.

## Sources

`docs/resampling-plan.md`, `docs/data-sources.md`, `docs/resampler-evaluation.md`, [ADR-0011](0011-bengaluru-source-table-v2.md), [ADR-0008](0008-speed-layer-streaming-shape.md), [ADR-0013](0013-amplify-thins-the-alignment-window.md)

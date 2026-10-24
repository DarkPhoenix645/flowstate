# BMTC synthetic boarding prior

This document holds the reference part of the FlowState research report "Synthetic BMTC Boardings", dated 12 October 2026. The report PDF is the snapshot: [assets/bmtc/FlowState_BMTC_Synthetic_Boardings_Report_2026-10-12.pdf](assets/bmtc/FlowState_BMTC_Synthetic_Boardings_Report_2026-10-12.pdf). Decision record: [ADR-0012](adr/0012-bmtc-boarding-prior.md). Ticket: #40.

The prior calibrates the synthetic fields `boardings`, `alightings`, and `onboard_leaving` on `flowstate.bus.scheduled_stop.v1`. Those fields are assumption-driven synthetic values. They are not BMTC ridership. Dashboards must not add them to observed metro or ride-hail totals.

## Recommended v1 configuration

Every value is a `FLOWSTATE_` setting. Empty means the default. A present value overrides. The resampler quotes the values in force in the bus summary audit block (`replay.bus.prior` in the amplify manifest).

| Setting | Default | Sensitivity | Evidence class |
| ------- | ------- | ----------- | -------------- |
| `FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS` | `4600000` | `4000000`, `4800000`; FY2019-20 replay `3310000` | Model choice inside a press-reported 4.8m (2025) and the CAG 3.31m (FY2019-20). Not a measured weekday mean |
| `FLOWSTATE_ASSUMED_BUS_CAPACITY` | `41.3` | none | Karnataka Economic Survey 2018-19 average seats, through 30 Nov 2018. Seats only, never seats plus standing |
| `FLOWSTATE_ASSUMED_MAX_LOAD_FACTOR` | `1.4` | `1.0`, `1.2`, `1.4` | Engineering guardrail. Not measured occupancy. CAG load factors are ticket-revenue ratios |
| `FLOWSTATE_METRO_SUBSTITUTION_MULTIPLIER` | `1.0` | do not lower without a new cited source | No citywide causal coefficient for the Yellow Line period |
| `FLOWSTATE_BUS_HOUR_PROFILE` | `metro_ridehail_blend` | switch to a versioned BMTC curve only after a digitised 24-hour table exists | Fallback blend. The Gadepalli curve is the preferred external check, not yet a weight vector |
| `FLOWSTATE_BUS_PEAK_SHARE_LOW` | `0.47` | the published band | Peer-reviewed summary (Gadepalli et al. 2022) |
| `FLOWSTATE_BUS_PEAK_SHARE_HIGH` | `0.51` | the published band | Peer-reviewed summary (Gadepalli et al. 2022) |
| `FLOWSTATE_BUS_TRIP_LENGTH_KM_LOW` | `10.7` | that pair only | Historical citywide (CMP citing RITES 2016) |
| `FLOWSTATE_BUS_TRIP_LENGTH_KM_HIGH` | `10.9` | that pair only | Historical citywide (Gadepalli et al., Jan 2020) |

`assumed_weekday_boardings` means the expected sum of generated `boardings` across stop calls for the simulated service day. It is not unique persons, not journeys, and not passenger-kilometres.

## Daily scale

![Reported daily BMTC scale by source](assets/bmtc/scale.pdf)

Points are plotted by source, not by date, and are deliberately not joined: the periods, datasets, and definitions differ. The dashed line separates the historical series from the current-scale reference and the FlowState scenario. Hollow markers mark press restatements and model choices.

| Period | Value | Definition / source context | Evidence class | Use |
| ------ | ----- | --------------------------- | -------------- | --- |
| 2014-15 | 51.30 lakh/day | BMTC passengers carried per day, CMP table | Official report citing BMTC | Historical context |
| 2015-16 | 50.74 lakh/day | Same CMP series | Official report citing BMTC | Historical context |
| 2016-17 | 45.34 lakh/day | Same CMP series | Official report citing BMTC | Historical context; secondary series differ |
| 2017-18 | 44.37 lakh/day | CMP and CAG report this value | Official report / CAG statistic | Historical anchor |
| 2019-20 | 33.10 lakh/day | CAG BMTC passenger base | Official audit statistic | Best audited pre-pandemic anchor for this FY |
| Jan 2020 | 3.04m/day | Average daily ridership in a one-week ETM analysis window | Peer-reviewed study output | Separate study estimate; incomplete pass capture |
| 2025 (press) | 48 lakh/day | Ministerial statement reported for BMTC | Press restatement of official statement | Contemporary scale check, not an audited weekday mean |

The FlowState alignment window is 26 October 2024 to 15 April 2026, so the pre-scheme FY2019-20 count is not automatically the right scale. For a first 2025/26 scenario use 4.6m as the provisional centre and test 4.0m and 4.8m. This is a model choice within the reported current scale, not a measured weekday average. When replaying FY2019-20, use 3.31m. Store the target regime and year, and make sure day-type multipliers do not double-count calendar effects [R1, R5, R6]. The older CMP lakh/day series is not a growth curve for this window. Do not join the points into a smooth trend.

## Fleet, seating, capacity, and load factor

What is actually sourced:

- CAG reports 6,799 buses in the BMTC fleet on 31 March 2022 (ordinary and premium) and 5,547 schedules. This is an audit-period count, not a current fleet mix by type [R1].
- Karnataka Economic Survey 2018-19 reports an average seating capacity of 41.3 and 45.00 lakh daily passengers through 30 November 2018. The passenger figure is partial-year; 41.3 is seats, not total occupancy [R2].
- AIS-052 (Rev.1) says seating and standee capacities should appear in the vehicle body type-approval certificate. Generic micro/mini/midi/standard categories are not a verified crosswalk to BMTC ordinary, Vayu Vajra, Vajra, feeder, or specific EV variants [R8].

![CAG load factors by financial year](assets/bmtc/loadfactor.pdf)

The CAG load factors are calculated from ticket revenue. They are historical operational indicators, not observed peak onboard headcounts. 2020-22 is a separate demand regime [R1].

The 1.4 guardrail: the suggested 1.4 multiplier traced back to a social-media claim about 65-70 passengers and road-transport rules. That does not establish a statutory limit. It may remain as a configurable engineering guardrail, clearly labelled synthetic, and sensitivity-tested at 1.0, 1.2, and 1.4.

| Load-factor setting | 1.0 | 1.2 | 1.4 (v1 guardrail) |
| ------------------- | --- | --- | ------------------ |
| Implied cap on `onboard_leaving` (passengers) | 41.3 | 49.6 | 57.8 |

The cap arithmetic is 41.3 x factor, derived, not sourced. If `assumed_bus_capacity` means seats, 41.3 is a dated fleet-average prior. If it means total standing-plus-seated capacity, 41.3 is not semantically correct. Resolve the meaning before changing the value. Never describe tested load-factor values as measured BMTC occupancy.

## Modal share: context, not a multiplier

- The Bengaluru Comprehensive Mobility Plan uses a 2016 household interview survey. Its all-trip summaries put public transport at roughly 32%, two-wheelers about 27%, walking about 26%, auto-rickshaws about 7%, and cars/taxis/shared taxis about 7%. The public-transport category combines bus, minibus, school bus, chartered bus, and metro; it is not a bus-only share [R3].
- A 2025 metro/feeder integration study reports that 65.6% of respondents had used a regular city bus before adopting metro. This is a prior-mode share among surveyed metro adopters, not the fraction of all bus passengers who switched, and not a causal estimate of net bus ridership lost [R11].

Decision: no current standalone bus/metro/auto split suitable for scaling generated boardings was found. Keep the 2016 survey as context only. Do not use it to manufacture route-level counts.

## Metro opening and bus demand

Gadepalli et al. (2022) compared the metro with the BMTC network, service, and ticketing data for Phase-1 conditions, with demand observations for January 2020 and March 2021 [R4]. Their findings:

- 1,903 of 2,203 routes (86%) had some spatial overlap with the metro, but overlap alone did not imply parallel passenger travel.
- Only 734 of those routes had passenger trips with both ends within 400 m of metro stations.
- 132 routes (6% of total) had at least 10% of demand travelling at least 2 km parallel to the metro; 27 routes had at least 25% of demand in that category.
- From January 2017 to January 2020 the operational fleet grew 12% and citywide effective kilometres grew 6%; effective-km growth in the metro influence area was about 2%. Demand there was described as marginally lower, presumably related to the metro, but no citywide substitution coefficient was established.

These results predate the Yellow Line, which opened on 11 August 2025 (station count 69 to 83 in the RTI data). FlowState has a structural change in metro observations at that date but no matched BMTC route-level panel to estimate diversion.

Decision: keep `metro_substitution_multiplier` = 1.0 citywide. If route-level analysis is ever done, estimate effects only on routes with matched origin-destination evidence and a credible comparison [R4, R7].

## Ticketing, APC, and route/stop counts

- The 2022 study used ETM backups with route-wise origin, destination, ticket issue time, and fare. It captured about 1.7 million ticketed users per day pre-COVID but omitted student and employee pass users, estimated at around half of total bus demand. Bus-bus and bus-metro transfers were unavailable, so one journey could appear as several legs [R4].
- No public downloadable BMTC dataset or API with stop-by-hour observations, route-by-hour boardings, APC counts, or complete ticket/pass validations was identified [R9].
- The accepted source is the unofficial Vonter/bmtc-gtfs feed (feed date 2026-09-07; about 9,960 stop records in the project inventory). The maintainers warn that some routes may be missing and some timings inaccurate [R9, R10].

Decision: treat GTFS as possible stop calls and a schedule scaffold only. Do not set boarding weights from stop-time or route counts and call them observed demand. Do not label generated boardings or alightings as APC.

## Trip length and alighting placement

![BMTC sample trip-length distributions from fare-derived distances](assets/bmtc/triplen.pdf)

Percentages are as published and may not total 100 because of rounding [R4].

| Source / sample | Average | Note |
| --------------- | ------- | ---- |
| CMP, citing RITES study 2016 [R3] | 10.7 km | Citywide historical cross-check |
| Gadepalli et al., citywide Jan 2020 [R4] | 10.9 km | Fare-derived |
| Gadepalli et al., citywide Mar 2021 [R4] | 9.5 km | Pandemic-affected |
| Metro influence area, Jan 2020 / Mar 2021 [R4] | 5.7 / 6.2 km | Subset; do not apply citywide |

Decision: use 10.7-10.9 km as a historical citywide cross-check and the published bins as a soft constraint where route distances can be computed from ordered stops or paths. Do not infer stops per trip from kilometres without geometry or inter-stop distances. Do not use the pandemic 9.5 km figure or the metro-influence-area figures as the citywide target. If route paths are missing or malformed, keep the uncertainty instead of inventing a precise origin-destination matrix.

## Hour-of-day demand prior

![Published peak and combined-peak windows](assets/bmtc/hours.pdf)

Only the windows are shown. No hourly curve has been digitised, and none should be inferred from this chart [R4].

| Indicator | Published result |
| --------- | ---------------- |
| Morning peak | 08:00-10:00 |
| Evening peak | 17:00-19:00 |
| Combined peak windows | 08:00-11:00 and 17:00-20:00 |
| Share of daily demand in combined windows | 47-51% |
| Mar 2021 scheduled services terminating by 18:00 | 1,236 of 5,438 (23%) |

The paper ties the sharp fall in demand after about 19:00 partly to scheduled service availability. This curve is better than shaping bus demand from metro exits and ride-hail alone, but it is not a full raw citywide dataset: ETM tickets exclude pass users, the observations are old, and only a curve and summary are published.

Decision: the BMTC-specific curve is the preferred external shape constraint. If exact 24-hour weights can be reliably digitised, preserve provenance and version the extraction. Until then keep `metro_ridehail_blend` as the fallback, with a check that the generated combined peak windows are consistent with the 47-51% band. The check is a warning under the blend, not a build failure. A versioned BMTC curve mode would enforce the band. Do not invent all 24 hourly percentages from the summary.

## Keep, adopt, reject

| Candidate | Decision | Reason |
| --------- | -------- | ------ |
| 4.6m weekday baseline for 2025/26 | Adopt provisionally, not as a published statistic | Current press scale is 4.8m daily; no weekday-only series. Test 4.0m and 4.8m |
| 3.31m/day in FY2019-20 | Adopt for the historical FY2019-20 scenario | CAG-reported passenger base; not the current policy regime |
| 41.3 average seats | Adopt only if the parameter means seats | Dated fleet-average seat figure; not total passenger capacity |
| 1.4 as official legal / observed load factor | Reject | Social-media claim is insufficient; CAG load factor is revenue-based, not peak occupancy |
| 2016 32% public-transport share as bus share | Reject | CMP category combines several public-transport modes |
| 65.6% prior bus users among metro adopters as substitution | Reject | Conditional metro-adopter survey, not a citywide net change |
| Metro ridership growth reduces bus boardings | Reject | No matched causal citywide estimate; modes can substitute, complement, or generate trips |
| GTFS route/stop counts as passenger demand | Reject | Schedule is not passenger observation |
| Early stops board, late stops alight as local behaviour | Reject as an empirical claim | No BMTC stop-level evidence; as a generator convention it stays labelled synthetic and checked for artefacts |
| Generated taxi files rejected by FlowState | Reject completely | The project source audit identifies generated or mislabelled files; they cannot tune bus demand |
| CAG revenue load factors as physical occupancy | Reject | Revenue-based operational indicator, not onboard headcounts |

## Evidence gaps and how to close them

- Current baseline by day type: daily passenger totals or validated ticket/pass data for a representative 8-12 week period, with weekday, Saturday, Sunday, holiday, school-day, and major-event flags.
- Route-time demand: de-identified ETM ticket issue records by route, trip, and hour, including pass validations; document whether "trip" means a ticketed leg or a complete journey.
- Vehicle capacity mapping: a fleet master keyed by registration or variant with seat and standee capacities from type-approval certificates.
- Stop-level counts / APC: passenger counts or manual load surveys keyed by route, direction, stop, time, and trip ID, ideally including the maximum-load point.
- Updated household travel survey: current mode share and trip distances with separate bus, metro, auto, and taxi/ride-hail categories and a stated geography.
- Post-Yellow-Line route comparison: comparable BMTC route-level counts before and after 11 August 2025, controlling for Shakti, fares, service supply, work and education patterns, and route changes.

Until these exist, treat generated stop values as synthetic calibrated events, not observations. Use ranges and scenario runs for uncertain priors and keep the evidence label attached to every configuration value.

## Evidence and citation rules for future updates

- Official statistic: government audit, operator report, official survey or report, or technical standard. Cite the period, geography, and definition.
- Survey estimate / empirical study: state the sample, year, target population, and known bias. A result among metro adopters is not a citywide mode share.
- Press restatement of an official statement: useful for a current headline figure but weaker than a published daily series with definitions. Mark it as such.
- Model choice / missing evidence: mark as an assumption. An engineering guardrail is not measured just because it is numerically precise.

For FlowState changes, cite the source in the relevant ADR or configuration explanation, record the retrieval date and source file or hash, and keep separate fields for target year and regime, central value, sensitivity range, and evidence class. Do not silently rebase history as new reports arrive.

## References

- [R1] CAG, Principal Accountant General (Audit-I), Karnataka. Performance Audit on Functioning of BMTC. Sent to government 26 Sep 2024; tabled 12 Dec 2024; audit period 2017-18 to 2021-22. https://cag.gov.in/ag1/karnataka/en/audit-report/details/121112
- [R2] Government of Karnataka. Karnataka Economic Survey 2018-19, Appendix 11.8, "Operational Performance Statistics of BMTC" (values through 30 Nov 2018). https://static.investindia.gov.in/s3fs-public/2019-07/Karnataka%20Eco%20Survey%202018-19_0.pdf
- [R3] DULT, Government of Karnataka, with partner agencies. Comprehensive Mobility Plan for Bengaluru (2020). Household interview survey 2016; bus trip length attributed to the RITES Study, 2016. https://dult.karnataka.gov.in/assets/front/pdf/Comprehensive_Mobility_Plan.pdf
- [R4] Gadepalli, R., Gumireddy, S., Bhamidipati, S., & Cats, O. (2022). Impact of introducing a metro line on urban bus services. Case Studies on Transport Policy, 10(2), 940-947. DOI 10.1016/j.cstp.2022.03.007. https://repository.tudelft.nl/file/File_4c6065de-6639-45eb-9340-75c69b12956e
- [R5] The Times of India. "Transport minister slams calls to end BMTC monopoly, terms move anti-public." October 2025. Ministerial statement of about 4.8 million BMTC daily riders; press restatement. https://timesofindia.indiatimes.com/city/bengaluru/transport-minister-slams-calls-to-end-bengaluru-metropolitan-transport-corporation-monopoly-terms-move-anti-public/articleshow/124560415.cms
- [R6] The Times of India. "Rs 12,000 crore fuels Shakti's 2-Year smooth run in Karnataka." 12 June 2025. Statewide RTC patronage; policy-regime context, not BMTC-only ridership. https://timesofindia.indiatimes.com/city/bengaluru/rs-12000-crore-fuels-shaktis-2-year-smooth-run-in-karnataka/articleshow/121786207.cms
- [R7] The Times of India. "Bengaluru Metro sets new record: Namma Metro hits 10.5 lakh daily ridership; Yellow Line's 1st day sees 83,000 commuters." August 2025. Context for the Yellow Line regime change, not bus losses. https://timesofindia.indiatimes.com/city/bengaluru/yellow-line-helps-namma-metro-ridership-cross-10-lakh-mark-in-bengaluru/articleshow/123265415.cms
- [R8] ARAI / MoRTH. AIS-052 (Rev.1), Code of Practice for Bus Body Design. https://cdnbbsr.s3waas.gov.in/s3e58aea67b01fa747687f038dfde066f6/uploads/2023/11/202311301582343149.pdf
- [R9] FlowState project source inventory, data-sources.md (reviewed 11 Oct 2026). Internal project document.
- [R10] Vonter/bmtc-gtfs, unofficial BMTC GTFS source. Feed date 2026-09-07; schedule-only role. https://github.com/Vonter/bmtc-gtfs
- [R11] "Evaluating multimodal integration of metro and feeder bus services in Bengaluru using integrated analytical and perception based methods." Multimodal Transportation (2025), DOI 10.1016/j.multra.2025.100287. Prior-mode result is conditional on metro-service respondents. https://doi.org/10.1016/j.multra.2025.100287


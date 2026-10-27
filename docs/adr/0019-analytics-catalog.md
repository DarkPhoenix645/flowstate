# ADR-0019: Analytics catalog

## Status

Accepted. 2026-10-28.

## Context

The batch jobs need a fixed list of questions before any Spark or MapReduce code is written. A later job must not invent a question. A finding on the amplified corpus is a property of the twin. It is not new evidence about Bengaluru.

Fare is not observed. The ride-hail cancel fields are zero-filled. The city-hour flight slice is thin. Those facts remove three questions: booking value by vehicle, a driver-versus-rider cancel split, and an hour-grain flights-to-ride-hail response.

## Decision

The catalog has six questions. Q1–Q5 read `/lake/rides_amplified/` and state the corpus rows, the seed, and the bundle hash. Q6 reads the stream archive.

| Id | Question | Job | Grain | Limit |
| --- | --- | --- | --- | --- |
| `demand_hotspots` | Which wards and hours carry the most ride-hail demand, on weekdays and on weekends? | #18. MapReduce #24 uses the same key | ward × hour-of-day × day-type | A property of the twin. Also the #30 surge baseline |
| `cancellation_analysis` | Where and when do ride-hail bookings end in a cancel? | #19. MapReduce #24 uses the same grain | ward × hour-of-day × day-type × vehicle_type | Cancels over bookings. Reasons stay unknown. No driver-versus-rider split |
| `ridership_trends` | How do metro and aviation volumes change over time? | #20. MapReduce #24 uses the same grain | day and month. Metro station-hour peak share | Yellow Line before and after 2025-08-11 only where RTI covers the station |
| `ridehail_metro` | Do ride-hail and metro volumes move together day to day? | #21. MapReduce #24 uses the same grain | day over the 372 shared days. Month with DGCA over 19 months | Weekday-adjusted association with an interval. Not cause |
| `flights_ridehail` | Do flight days line up with ride-hail days? | #21. MapReduce #24 uses the same grain | day, covered days only | Exploratory. Prints n and an interval. [ADR-0017](0017-producer-as-a-realtime-twin-source.md) needs 60 shared covered dates. The data has 42 and 38. No predictive claim |
| `scenario_effect` | What changed after a command, baseline against fork, in every mode? | #90. Not a MapReduce job | hour, per mode and place | Reads the stream archive. Needs the #89 sinks. `assumption_driven`. Not a learned effect |

Clock for every grain is Asia/Kolkata. `hour_of_day` is 0–23. `day_type` is `weekday` for Monday–Friday and `weekend` for Saturday and Sunday.

### Shared demand key

Q1 on Spark and on MapReduce shares one key: `ward_id`, `hour_of_day`, `day_type`.

The count is the number of ride-hail rows. One row is one booking. Spark and MapReduce use this count and no other. The surge baseline (#30) is this table for the demo corpus. It is not a seventh question.

### MapReduce pair

#24 writes the Q1–Q5 tables. The columns match this catalog. Spark stays the primary path. MapReduce is the baseline. Q6 is not in that jar. Q6 compares a baseline with a fork. It is not an engine pair.

### Run vocabulary

The API calls one unit of work a run. A run has a `run_id` and a kind: `amplify`, `analytics`, `benchmark`, `replay`, or `stream`.

The event envelope keeps `run_id` as `<session>.<epoch>` ([ADR-0017](0017-producer-as-a-realtime-twin-source.md)). For a replay run, the API `run_id` is the session part. The envelope field is not renamed.

### Result tables

One Hive table per question, partitioned by `run_id`.

| Question | Table |
| --- | --- |
| `demand_hotspots` | `analytics_demand_hotspots` |
| `cancellation_analysis` | `analytics_cancellation_analysis` |
| `ridership_trends` | `analytics_ridership_trends` |
| `ridehail_metro` | `analytics_ridehail_metro` |
| `flights_ridehail` | `analytics_flights_ridehail` |
| `scenario_effect` | `analytics_scenario_effect` |

Every row carries the run parameters: `corpus_rows`, `seed`, `bundle_hash`, `window_start`, `window_end`.

Question columns:

- `demand_hotspots`: `ward_id`, `hour_of_day`, `day_type`, `demand_count`.
- `cancellation_analysis`: `ward_id`, `hour_of_day`, `day_type`, `vehicle_type`, `bookings`, `cancels`, `cancel_rate`. `cancel_rate` is `cancels / bookings`. A row with `status` `completed` is not a cancel. There is no reason column.
- `ridership_trends`: `grain` (`day`, `month`, or `station_peak`), `period_start`, `mode` (`metro` or `aviation`), `volume`, `station_id`, `peak_share`, `payment_method`, `line_phase` (`before_open`, `after_open`, or null). A column is null when that grain does not use it. `payment_method` is set only when the metro source has it. `line_phase` uses 2025-08-11 and only stations the RTI covers.
- `ridehail_metro`: `grain` (`day` or `month`), `period_start`, `n`, `estimate`, `interval_low`, `interval_high`. The estimate is a weekday-adjusted association. The label is not cause.
- `flights_ridehail`: `period_start`, `n`, `estimate`, `interval_low`, `interval_high`. Covered days only. The row states the 42 and 38 covered-date counts against the required 60.
- `scenario_effect`: `replay_run_id`, `mode`, `place_id`, `hour_of_day`, `direction`, `baseline_value`, `fork_value`, `difference`, `ratio`, `evidence_grade`, `cancel_rate_baseline`, `cancel_rate_fork`. `difference` is fork minus baseline. `ratio` is fork over baseline, null when the baseline is 0. `evidence_grade` is `assumption_driven` for a flight response and for a metro shift. `cancel_rate_baseline` and `cancel_rate_fork` are set on ride-hail rows and null on the other modes. No column calls the difference observed or learned.

### Q6 place grain

The place is the archive sink key for that mode.

| Mode | `place_id` | Value |
| --- | --- | --- |
| ride-hail | ward id | active-rides sink count for that ward-hour |
| metro | station id | `entries_count` + `exits_count` |
| bus | stop id | boardings |
| aviation | airport code | event count. `direction` is `arrival` or `departure` |

`direction` is null except for aviation.

### Grafana link

One dashboard uid per question: `analytics-<question>` with the ids in the table above. One variable, `run_id`.

The deep link is `{grafana}/d/{uid}?var-run_id={run_id}`. `{grafana}` is the deploy Grafana origin. The scenario-effect link also sets the dashboard time range to the replay `window_start` and `window_end`.

## Consequences

- #18, #19, #20, #21, #24, #90, and #91 implement this list. They do not add a question.
- #24 mirrors Q1–Q5. It does not implement Q6.
- #21 writes two tables. #20 writes one table with a `grain` column.
- A new analytical question needs a new ADR before a job or a panel.

## Sources

Issue #85, [ADR-0007](0007-mapreduce-spark-benchmark-pair.md), [ADR-0010](0010-bengaluru-calibrated-resampler.md), [ADR-0016](0016-demo-surface.md), [ADR-0017](0017-producer-as-a-realtime-twin-source.md)

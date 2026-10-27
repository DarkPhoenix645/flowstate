# Watermark and late data

Decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md), revised 2026-10-24. This note is the #11 policy. #22 uses only this policy. Job code stays with #22. The producer rules below stay with #39.

The watermark is the maximum `event_time` seen, minus the delay. Spark stores that value in the checkpoint. The value does not move backward. A row older than the watermark is dropped. Do not widen the delay to hide producer disorder. The producer emits each topic in `event_time` order, and the delay stays small.

## Checked names

These names are in the repo today. Do not invent a second name.

| Policy term | Field | Where |
| --- | --- | --- |
| Activity type | `event_type` | Envelope. Ride-hail values are `booking_created`, `completed_ride`, and `cancelled_ride`. |
| Scenario | `scenario_id` | Envelope. The corpus builder sets it to null. A non-null value on a corpus row fails the resampler check. Baseline events carry null. |
| Training window | `provenance.outside_training_window` | Envelope boolean. The corpus stores the strings `true` and `false`. The mapper converts them. |
| Drop counter | `stateOperators[].numRowsDroppedByWatermark` | Spark 3.5.6 `StateOperatorProgress` JSON. It is not a top-level progress field. The class adds each batch through `newNumRowsDroppedByWatermark`. Log the JSON field. It is the running total on the stateful operator. |

`emitted_at` is never a window key and never a watermark key.

Broker auto-create is on (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`). The image default is one partition. Do not rely on auto-create.

## Decisions

1. Clock. Use `event_time`.

2. Windows. Use a 1-minute tumbling window only. This replaces the ADR-0008 5-minute slide for #22. #22 is a count. A 5-minute or hourly total is the sum of the 1-minute rows. Roll that sum up in the Hive or Grafana view, not in Spark. Spark aligns windows to UTC. Cut hourly or daily buckets in IST in the view. The map does not read the sink.

3. Delay. The watermark delay is 1 minute. The job can change it. The default is 1 minute. This replaces the 10-minute ADR-0008 default. The live freshness target is about 2 minutes: window end, plus the delay, plus the 30-second trigger. On replay the delay costs milliseconds of wall time.

4. Late rows. Drop them. Do not write a side output. Do not set allowed lateness. An append sink cannot revise a written row, and nothing reads a side output. Log `numRowsDroppedByWatermark` from query progress, on the state operator as above.

5. Sink. Append parquet at `hdfs://namenode:9000/stream-sink/<job>/`. Checkpoint at `hdfs://namenode:9000/checkpoints/<job>/`.
   - Group by `location_ref.at.ward_id`, `provenance.status`, `scenario_id` (null stays null), and `event_type`. The count column is `activity_count`. `event_type` is a dimension. It is not cancel-rate logic. #29 can derive its ratio later.
   - Flatten the window struct to `window_start` and `window_end`.
   - Set `spark.sql.shuffle.partitions` to 4. Use a 30-second processing-time trigger for live runs. Use `availableNow` for replay and tests. Set `maxOffsetsPerTrigger` in the range 10,000 to 30,000.
   - One Make target clears the checkpoint directory, the sink directory, and the topic together.
   - Sink partitioning waits for the Grafana read path (#28).

## Producer invariants

Carry these into #39, #17, and #67.

P1. Emit each topic in non-decreasing `event_time`. Sort each source day by (`event_time`, `event_id`). The current `ridehail_day_events` order writes bookings for hours 0-23, then completions, then cancels, with random seconds inside the hour. Completions and cancels would be late under this delay. Assert that order on regression. Count regressions in the audit topic QA failures.

P2. Baseline and intervention forks share a topic. Merge them by (`event_time`, `event_id`) when they are emitted together. Or run one fork at a time against a reset checkpoint. Do not emit one fork after another into the same live checkpoint. The second fork would be entirely late.

P3. Create the topic with a Make target, with 1 partition. The producer refuses to run when the partition count is anything else. Do not rely on auto-create.

P4. Live mode sets `event_time` to the wall clock (#39). A row after 2026-04-15 gets `provenance.outside_training_window`. Do not shift historical timestamps onto the wall clock. Set `emitted_at` at send time in every mode.

P5. Replay does not rewrite `event_time`. `--events-per-sec` is a cap only.

## #22 acceptance

- Import the Spark `StructType` from `ride_event_schema.py` (#39). Do not hand-write a copy.
- Replay test: fixed seed, `availableNow`. `sum(activity_count)` equals the events produced, except the final open windows (the last minute or two of event time). `numRowsDroppedByWatermark` is 0.
- Recovery test: stop a worker during a run, then start the job again from its checkpoint. Do not use the reset target. Sink totals equal a clean run on the same seed.
- Out of scope: cancel rate, surge, an enrichment topic, a side output, and a 5-minute series.

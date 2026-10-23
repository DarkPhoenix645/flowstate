# Event-Detector node layout

Decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md). This note names the nodes. It does not replace the topic list, the envelope, or the watermark default. Watermarks stay with #11. Job code stays with #22, #29, and #30.

CityPulse ran one event-detection node per pattern on annotated or aggregated streams. FlowState uses the same shape. One Structured Streaming job is one node. Do not put two metrics in one file.

## Stages

| CityPulse stage   | FlowState                                                                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Annotated stream  | The ADR-0008 Kafka envelope. The producer (#39) writes it. A node does not add fields and does not publish a new topic.                                |
| Aggregated stream | The window inside that node. Until #11 closes, use the ADR-0008 default: 1-minute tumbling, 5-minute sliding, watermark of 10 minutes on `event_time`. |
| Event             | One metric row in that node's stream-sink directory.                                                                                                   |

There is no enrichment topic. There is no mixed-mode topic.

## Nodes

| Node                | File                                         | Submit                                    | Reads                                                                                                | Writes                                           |
| ------------------- | -------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| Active rides / zone | `src/flowstate/streaming/rolling_metrics.py` | `make stream-consume`                     | `flowstate.ridehail.activity.v1`                                                                     | `hdfs://namenode:9000/stream-sink/active_rides/` |
| Cancel rate         | `src/flowstate/streaming/cancel_rate.py`     | a later Make target, not `stream-consume` | `flowstate.ridehail.activity.v1`                                                                     | `hdfs://namenode:9000/stream-sink/cancel_rate/`  |
| Surge               | `src/flowstate/streaming/surge.py`           | a later Make target, not `stream-consume` | `flowstate.ridehail.activity.v1`, `flowstate.aviation.arrival.v1`, `flowstate.aviation.departure.v1` | `hdfs://namenode:9000/stream-sink/surge/`        |

Checkpoint for a job is `hdfs://namenode:9000/checkpoints/<job>/`. The `<job>` names are `active_rides`, `cancel_rate`, and `surge`.

`/opt/data` is not the live sink. `Settings.stream_sink_dir` (`data/stream-sink` on the host) is not the live sink. #22 writes the HDFS paths above.

The map does not read these directories. It reads the Kafka envelope through the demo API.

## What each node computes

Active rides / zone (#22). Count synthetic activity events by `location_ref.ward_id` in the window. This file does not compute cancel rate or surge. The metric is simulated activity, not observed ridership. Keep `provenance.status` on the sink row.

Cancel rate (#29). Cancelled activity divided by total activity, per zone and window, from the same ride-hail topic. Do not fold it into `rolling_metrics.py`. Do not split driver versus rider. Those source fields are zero-filled.

Surge (#30). Its own job. It may use the #18 zone × hour baseline. Group sink rows by `scenario_id` so baseline and intervention stay separable. A flight response is `assumption_driven` unless a held-out test validated that coefficient. Do not claim an airport metro station.

## Topics a node does not read

Subscribe only to the topics in the table. That is the filter. Do not read a combined stream and drop rows.

| Topic                             | v1 node                                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------------------- |
| `flowstate.ridehail.activity.v1`  | Active rides, cancel rate, and surge                                                     |
| `flowstate.aviation.arrival.v1`   | Surge only                                                                               |
| `flowstate.aviation.departure.v1` | Surge only                                                                               |
| `flowstate.metro.station_hour.v1` | None                                                                                     |
| `flowstate.bus.scheduled_stop.v1` | None                                                                                     |
| `flowstate.scenario.command.v1`   | None. The producer (#39) applies the command and sets `scenario_id` on synthetic output. |
| `flowstate.simulation.audit.v1`   | None. Audit only.                                                                        |

Aviation events are not ride-hail counts. Surge reads them as the flight context. The ride-hail response stays on the ride-hail topic.

## Rule for a new node

1. Add one file under `src/flowstate/streaming/`.
2. Give it one Make target. Do not add it to `make stream-consume`.
3. List its topics in the table above.
4. Watermark on `event_time`. Use the #11 policy. Until that spike closes, use the ADR-0008 default.
5. Write parquet under `hdfs://namenode:9000/stream-sink/<job>/`. Checkpoint under `hdfs://namenode:9000/checkpoints/<job>/`.
6. Do not publish a Kafka topic.

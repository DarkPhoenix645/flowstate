# Event-Detector node layout

Decision: [ADR-0008](../adr/0008-speed-layer-streaming-shape.md). This note names the nodes. It does not replace the topic list or the envelope. The watermark policy is [watermarks.md](watermarks.md). Job code stays with #22, #29, and #30.

CityPulse ran one event-detection node per pattern on annotated or aggregated streams. FlowState uses the same shape. One Structured Streaming job is one node. Do not put two metrics in one file.

## Stages

| CityPulse stage   | FlowState                                                                                                                                              |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Annotated stream  | The ADR-0008 Kafka envelope. The producer (#39) writes it. A node does not add fields and does not publish a new topic.                                |
| Aggregated stream | The window inside that node. Use [watermarks.md](watermarks.md): 1-minute tumbling on `event_time`, watermark delay 1 minute, drop late rows. |
| Event             | One metric row in that node's stream-sink directory.                                                                                                   |

There is no enrichment topic. There is no mixed-mode topic.

## Nodes

| Node                | File                                         | Submit                                    | Reads                                                                                                | Writes                                           |
| ------------------- | -------------------------------------------- | ----------------------------------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| Active rides / zone | `src/flowstate/streaming/rolling_metrics.py` | `POST /api/runs` job `active_rides` | `flowstate.ridehail.activity.v1` | `hdfs://namenode:9000/stream-sink/active_rides/` |
| Cancel rate         | `src/flowstate/streaming/cancel_rate.py`     | `POST /api/runs` job `cancel_rate`  | `flowstate.ridehail.activity.v1` | `hdfs://namenode:9000/stream-sink/cancel_rate/`  |
| Surge               | `src/flowstate/streaming/surge.py`           | `POST /api/runs` job `surge`        | `flowstate.ridehail.activity.v1` | `hdfs://namenode:9000/stream-sink/surge/`        |

Checkpoint for a job is `hdfs://namenode:9000/checkpoints/<job>/`. The `<job>` names are `active_rides`, `cancel_rate`, and `surge`.

`/opt/data` is not the live sink. `Settings.stream_sink_dir` (`data/stream-sink` on the host) is not the live sink. #22 writes the HDFS paths above.

The map does not read these directories. It reads the Kafka envelope through the demo API.

## What each node computes

Active rides / zone (#22). Count synthetic activity events in the window. Group by `location_ref.at.ward_id`, `provenance.status`, `scenario_id`, and `event_type`. The count column is `activity_count`. This file does not compute cancel rate or surge. The metric is simulated activity, not observed ridership. Policy: [watermarks.md](watermarks.md).

Cancel rate (#29). Cancelled activity divided by total activity, per zone and window, from the same ride-hail topic. Do not fold it into `rolling_metrics.py`. Do not split driver versus rider. Those source fields are zero-filled.

Surge (#30) is its own job. It uses the #18 ward × hour-of-day × day-type baseline and reads the ride-hail topic only.

Group sink rows by `scenario_id` so baseline and intervention stay separable. A flight response is `assumption_driven` unless a held-out test validated that coefficient. Do not claim an airport metro station.

## Topics a node does not read

Subscribe only to the topics in the table. That is the filter. Do not read a combined stream and drop rows.

| Topic                             | v1 node                                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------------------- |
| `flowstate.ridehail.activity.v1`  | Active rides, cancel rate, and surge                                                     |
| `flowstate.aviation.arrival.v1`   | None. The ride-hail response is already on the ride-hail topic.                          |
| `flowstate.aviation.departure.v1` | None. The ride-hail response is already on the ride-hail topic.                          |
| `flowstate.metro.station_hour.v1` | None                                                                                     |
| `flowstate.bus.scheduled_stop.v1` | None                                                                                     |
| `flowstate.scenario.command.v1`   | None. The producer (#39) applies the command and sets `scenario_id` on synthetic output. |
| `flowstate.simulation.audit.v1`   | None. Audit only.                                                                        |
| `flowstate.simulation.control.v1` | None. The producer reads it.                                                             |

Every event carries `run_id`. A backwards seek in the producer starts a new one, and the topics then hold earlier `event_time` values after later ones. A job reads the newest `run_id` and resets its checkpoint when the `run_id` changes. ([ADR-0017](../adr/0017-producer-as-a-realtime-twin-source.md))

Aviation events are not ride-hail counts. Surge reads them as the flight context. The ride-hail response stays on the ride-hail topic.

## Rule for a new node

1. Add one file under `src/flowstate/streaming/`.
2. Start it with `POST /api/runs`. Do not add a Make target.
3. List its topics in the table above.
4. Watermark on `event_time`. Use [watermarks.md](watermarks.md).
5. Write parquet under `hdfs://namenode:9000/stream-sink/<job>/`. Checkpoint under `hdfs://namenode:9000/checkpoints/<job>/`.
6. Do not publish a Kafka topic.

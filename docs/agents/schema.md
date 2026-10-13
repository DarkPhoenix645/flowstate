# Canonical ride schema

Decision and rules: [ADR-0005](../adr/0005-canonical-ride-schema.md). Amplify scale: [ADR-0006](../adr/0006-amplify-for-scale-benchmarks.md). Multimodal emit: [ADR-0010](../adr/0010-bengaluru-calibrated-resampler.md). Zone ids: issue #4.

Code: `src/flowstate/schema.py`. `RIDE_COLUMNS`, `ride_json_schema()`, and `ride_spark_schema()` use the same fields in the same order. Staged parquet is written by `src/flowstate/staged.py`.

Kafka does not use this column set. `src/flowstate/streaming/ride_event_schema.py` is the [ADR-0008](../adr/0008-speed-layer-streaming-shape.md) envelope.

Extra source columns may sit beside these columns in a staged file. `city` and `vehicle_type` stay in the file. They are not path segments.

| Column | Meaning |
|---|---|
| `trip_id` | Stable id (source id or synthetic) |
| `city` | City id. v1 events use `bengaluru`. The column accepts more cities. |
| `mode` | `ride_hail`, `metro`, `bus`, `aviation` |
| `vehicle_type` | Hive partition key. Values are below. |
| `timestamp` | Event time as ISO-8601 text with a numeric offset. The JSON field, the Spark field, and the parquet column are strings. |
| `pickup_zone` | String. The zone id for that mode. Ids are below. |
| `drop_zone` | String. Same ids as `pickup_zone`. Null when the grain has one place. |
| `status` | `completed` or `cancelled` |
| `fare` | Number. Null when the source has no fare. This is not `earning / done_ride`. |
| `cancel_reason` | `unknown` on a cancelled ride-hail event. Null when the event is not cancelled. |
| `source` | Dataset tag (`namma_yatri`, `bmrcl_rti`, `bmtc_gtfs`, `opensky`, `synthetic`, …) |

## vehicle_type

| Value | Rule |
|---|---|
| `auto` | Assigned for ride-hail when the source names no vehicle class. The city-day clock is this case. The city-hour archive spells the token `Auto`. That token folds to `auto`. |
| `cab` | The city-hour archive spells this `Cab`. That token folds to `cab`. |
| `train` | Assigned for metro. |
| `bus` | Assigned for bus when the source names no vehicle class. GTFS `route_type` `3` is not this value. |
| `airplane` | Assigned for aviation. |
| pass-through | Any other non-empty source token is copied unchanged. The archive bucket `All` stays `All`. An empty token uses the assigned value for the mode. A passed token `3` stays `3`. |

## Zone strings

`pickup_zone` and `drop_zone` are strings. Each string is the id for that mode. The id name is not a column.

| mode | id the string carries | example |
|---|---|---|
| ride_hail | `ward_id` | `b_112` (Namma Yatri `ward_num`) |
| metro | `station_id` | `BYPH` (code in `station_codes.csv`) |
| bus | `stop_id` | `21629` (GTFS `stop_id`) |
| aviation | `airport` | `VOBL` |

These columns do not hold coordinates.

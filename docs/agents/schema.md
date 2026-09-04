# Canonical ride schema

Shared by ingest parquet, Kafka JSON, and streaming. Code: `src/flowstate/schema.py` (`RIDE_COLUMNS`). Keep names in sync.

| Column | Meaning |
|---|---|
| `trip_id` | Stable id (source id or synthetic) |
| `city` | e.g. bengaluru, mumbai |
| `mode` | ride_hail, metro, bus, aviation, … |
| `vehicle_type` | auto, cab, … Hive partition key |
| `timestamp` | **Event time** (pickup / event), ISO-8601 in JSON |
| `pickup_zone` | Zone / stop / grid id |
| `drop_zone` | Zone / stop / grid id |
| `status` | completed, cancelled, … |
| `fare` | Numeric; null if n/a |
| `cancel_reason` | driver, customer, …; null if not cancelled |
| `source` | Dataset tag (kaggle, bmrc, synthetic, …) |

Extra source columns may live beside these in staged files but Kafka events should use this set. Amplify must preserve hour-of-day mix and cancellation rates of the seed.

Hive: `PARTITIONED BY (city, vehicle_type)` — those two are partition columns in warehouse DDL, still present in the file schema for local parquet.

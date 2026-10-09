# Canonical ride schema

Decision and rules: [ADR-0005](../adr/0005-canonical-ride-schema.md). Amplify scale: [ADR-0006](../adr/0006-amplify-for-scale-benchmarks.md). Multimodal emit: [ADR-0010](../adr/0010-bengaluru-rnn-stream-resampler.md).

Code: `src/flowstate/schema.py` (`RIDE_COLUMNS`). Keep names in sync with this table and `ride_event_schema.py`.

| Column | Meaning |
|---|---|
| `trip_id` | Stable id (source id or synthetic) |
| `city` | e.g. bengaluru, ncr |
| `mode` | ride_hail, metro, bus, aviation. Resampler emit uses all four for Bengaluru (ADR-0010) |
| `vehicle_type` | auto, cab, … Hive partition key |
| `timestamp` | Event time (pickup / event), ISO-8601 in JSON |
| `pickup_zone` | Zone / stop / grid id |
| `drop_zone` | Zone / stop / grid id |
| `status` | completed, cancelled, … |
| `fare` | Numeric; null if n/a |
| `cancel_reason` | driver, customer, …; null if not cancelled |
| `source` | Dataset tag (kaggle, namma, synthetic, …) |

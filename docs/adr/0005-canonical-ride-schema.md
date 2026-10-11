# ADR-0005: Canonical ride schema and Hive partitions

## Status

Accepted

## Context

Staged parquet and Hive need one ride column set and stable partition keys. Kafka does not use that column set. The Kafka envelope is [ADR-0008](0008-speed-layer-streaming-shape.md). Loaders pull the Bengaluru sources in [ADR-0011](0011-bengaluru-source-table-v2.md). Amplify writes synthetic events from the resampler in [ADR-0010](0010-bengaluru-calibrated-resampler.md).

## Decision

One canonical ride schema for staged parquet and Hive (`src/flowstate/schema.py`, `docs/agents/schema.md`):

`trip_id`, `city`, `mode`, `vehicle_type`, `timestamp` (event time), `pickup_zone`, `drop_zone`, `status`, `fare`, `cancel_reason`, `source`.

Rules:

- Extra source columns may sit beside these in staged files.
- Staged parquet and Hive use this column set. The Kafka contract is the `flowstate.*.v1` envelope of [ADR-0008](0008-speed-layer-streaming-shape.md). `ride_event_schema.py` follows that envelope when the producer is built.
- Amplify keeps hour-of-day mix and cancellation rates from the real aggregate clocks, never from the rejected booking CSVs. Multimodal emit rules: [ADR-0010](0010-bengaluru-calibrated-resampler.md).
- Hive tables use `PARTITIONED BY (city, vehicle_type)`. Those fields stay in the file schema for local parquet as well.
- `ride_event_schema.py` is the JSON schema for the Kafka envelope. It is not a second copy of this column set.

## Consequences

- Staged parquet and Hive share this column set. Kafka readers share the ADR-0008 envelope. A change to either contract updates every reader of that contract.
- Warehouse DDL and load scripts follow the same partition keys.
- Per-dataset veracity notes stay outside the canonical columns.

## Sources

Column dictionary: `docs/agents/schema.md`. Also `docs/team.md`.

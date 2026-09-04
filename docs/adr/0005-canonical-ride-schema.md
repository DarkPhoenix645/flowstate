# ADR-0005: Canonical ride schema and Hive partitions

## Status

Accepted

## Context

Loaders pull Kaggle rides, metro ridership, GTFS, aviation, and synthetic amplify output. Kafka replay and streaming jobs need one event shape. Hive needs stable partition keys for city-scale scans.

## Decision

One canonical ride schema shared by staged parquet, Kafka JSON, and streaming (`src/flowstate/schema.py`, `docs/agents/schema.md`):

`trip_id`, `city`, `mode`, `vehicle_type`, `timestamp` (event time), `pickup_zone`, `drop_zone`, `status`, `fare`, `cancel_reason`, `source`.

Rules:

- Extra source columns may sit beside these in staged files.
- Kafka events use this column set.
- Amplify keeps hour-of-day mix and cancellation rates from the seed.
- Hive tables use `PARTITIONED BY (city, vehicle_type)`. Those fields stay in the file schema for local parquet as well.
- `ride_event_schema.py` is the single JSON schema for producer and consumer.

## Consequences

- Schema drift across ingest, Kafka, and streaming is a bug. Change the contract in one place and update all consumers.
- Warehouse DDL and load scripts follow the same partition keys.
- Per-dataset veracity notes stay outside the canonical columns.

## Sources

Column dictionary: `docs/agents/schema.md`. Also `docs/team.md`.

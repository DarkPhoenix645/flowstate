# ADR-0018: Postgres for model config

## Status

Accepted. 2026-10-27. Amends [ADR-0003](0003-compose-cluster-host-uv.md). Narrows the Postgres line in [ADR-0002](0002-lambda-pipeline-kafka-spark-hdfs-hive.md).

## Context

Model and policy config has no version history. Catchment wards, metro shift shares, and the aviation coverage rule change by a file edit. [ADR-0017](0017-producer-as-a-realtime-twin-source.md) records that cost. Mobility data stays on disk and HDFS.

ADR-0002 says a new store needs an ADR. ADR-0003 lists the Compose services. Postgres is not in that list.

## Decision

Run Postgres 16 in Compose. Bind `127.0.0.1:5432`. The database holds `config_version`, `config_active`, `run_config`, `benchmark`, and `run`. It does not hold rides, the lake, or deploy secrets.

`infra/postgres/init.sql` creates the schema on a fresh volume. The process checks `schema_version` on connect. There is no migration framework.

Postgres is the only model-config source for a run. The database URL is `FLOWSTATE_CONFIG_DATABASE_URL`. `make config-seed` inserts the first version of each kind when that kind has no active row. A run reads those rows. It does not read the policy file or the `FLOWSTATE_ASSUMED_*` variables. If Postgres is unreachable, the schema version does not match, or `resample_config` has no active version, a run stops.

`GET /api/config` is the exception. When `resample_config` has no active version, that route reads `FLOWSTATE_ASSUMED_*` and then the documented default. A later `PUT /api/config` saves that resolved document, including a body that only sets `replay_seed`. `config_hash` is the hash of the active kind hashes. One active kind is enough.

The local database password is a deploy setting. It is not part of a resolved config.

## Consequences

- `make up` starts Postgres with the other Compose services. Then `make config-seed`. Field reference: [model-config.md](../model-config.md).
- CI does not boot Compose. Postgres tests are marked `integration` and skip when no database is available.
- #42 added `run_config` and `benchmark`. #86 added `run`. Mobility data stays on disk and HDFS.
- A new volume is required before `init.sql` runs again. A schema version mismatch stops the process.

## Sources

Issue #75, [ADR-0002](0002-lambda-pipeline-kafka-spark-hdfs-hive.md), [ADR-0003](0003-compose-cluster-host-uv.md), [ADR-0017](0017-producer-as-a-realtime-twin-source.md)

# ADR-0020: Job backend

## Status

Accepted. 2026-10-28.

## Context

User-facing work needs one start path. Make can still prepare a machine and run a quick check. It cannot be the way a demo starts a job. A caller must not send a shell string. #72 will add Dataproc later, on the same interface.

## Decision

A run is one unit of work. The API stores it in the `run` table before the job starts. The row holds the config snapshot, `config_hash`, and the version id of each active kind.

`JobBackend` has `submit`, `status`, `cancel`, and `log_tail`. `submit` returns an `external_id`. The default backend name is `compose`. A name that is not configured is HTTP 422 and writes no row.

`compose` calls the `runner` service over HTTP. The runner accepts only `spark_submit`, `hdfs_put`, `hdfs_mkdir`, `hdfs_rm` of `/lake/rides_amplified`, `beeline`, and `yarn_jar`. It does not accept a free command. The API process does not call `docker`.

The registry is `src/flowstate/jobs/registry.py`. This ticket registers `amplify` and `hdfs_probe`. A job that is not in the file cannot run. `amplify` rows are `1000000`, `10000000`, or `100000000`.

On startup the API reconciles every `running` row with its backend. An id the backend does not know becomes `failed`.

`grafana_url` uses the ADR-0019 uid and `FLOWSTATE_GRAFANA_URL`. It is null until that job has a dashboard.

## Consequences

- Make stays for dev work, one-time setup, and quick checks ([ADR-0004](0004-make-task-runner.md)).
- #88 adds replay on this process. #72 adds the `dataproc` backend. Neither adds a second HTTP server.
- Tests use an in-memory backend. A test marked `integration` calls the runner and skips when it is down.

## Sources

Issue #86, [ADR-0003](0003-compose-cluster-host-uv.md), [ADR-0004](0004-make-task-runner.md), [ADR-0018](0018-postgres-for-model-config.md), [ADR-0019](0019-analytics-catalog.md)

# ADR-0004: Make as the task runner

## Status

Accepted

## Context

Early planning used Just recipes. The team needs one entrypoint that works on Linux, macOS, and Windows (Git Bash or WSL) and that CI can call the same way.

## Decision

Use **Make** for dev work, one-time setup, and quick checks. User-facing runs go through the API ([ADR-0020](0020-job-backend.md)).

Make remains the task runner for documented dev workflows.

Locked targets and variables include: `setup`, `ingest`, `amplify`, `up` / `down`, `stream-produce`, `hive-ddl`, `mr-package`, `yarn-probe`, `test`, `lint`, plus `DATASET`, `ROWS`, `TOPIC`, `RATE`.

Analytics jobs and the stream nodes start through the API. Spark stays on Compose. It does not run on the host.

## Consequences

- Do not reintroduce a Justfile as the primary interface.
- Windows contributors use Git Bash or WSL for `make`.
- New common workflows get a Make target before they become tribal knowledge.

## Sources

`docs/agents/scaffold.md`, `docs/getting-started.md`

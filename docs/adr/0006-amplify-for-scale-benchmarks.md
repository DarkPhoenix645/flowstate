# ADR-0006: Amplify for scale benchmarks

## Status

Accepted

## Context

Public mobility extracts are smaller than the 1M / 10M / 100M rows needed for the Spark vs MapReduce story. Pure random synthetic data would break cancellation and hour-of-day structure that analytics care about.

## Decision

Scale the lake with `amplify.py`:

- Resample seed staged rows to `N` in `{1e6, 1e7, 1e8}`.
- Keep seed-faithful hour-of-day mix and cancellation rates (`--seed` supported).
- Treat `make ingest && make amplify` as the path that fills the lake for benchmarks.

`benchmarks/runner.py` records timings in `benchmarks/results/timings.csv` (committed) for the report.

## Consequences

- “Big data” scale in this repo means amplified seed data, not a second live scrape of 100M real trips.
- Benchmark claims must state amplify settings (rows, seed) when results matter.
- Loader work still owns veracity of the seed; amplify does not fix bad upstream data.

## Sources

`docs/agents/workstreams.md`, `docs/team.md`, `docs/agents/schema.md`

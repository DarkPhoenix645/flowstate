# ADR-0006: Amplify for scale benchmarks

## Status

Accepted

## Context

Public mobility extracts are smaller than the 1M / 10M / 100M rows needed for the Spark vs MapReduce story. Pure random synthetic data would break cancellation and hour-of-day structure that analytics care about.

## Decision

Scale the lake with `make amplify`. For **Bengaluru events**, `amplify.py` calls the multimodal RNN resampler in ADR-0010. It writes `N` rows in `{1e6, 1e7, 1e8}` to `data/staged/rides_amplified/` with `--seed`. `N` is the count of multimodal events (all `mode` values), not taxi-only. It does not invent a second sampler.

Keep seed-faithful hour-of-day mix and cancellation rates (enforced by ADR-0010). Never overwrite the ride-hail seed.

`benchmarks/runner.py` records timings in `benchmarks/results/timings.csv` (committed) for the report.

## Consequences

- “Big data” scale in this repo means resampler output at `N`, not a second live scrape of 100M real trips.
- Benchmark claims must state amplify settings (rows, seed) when results matter.
- Loader work still owns veracity of the seed. The resampler does not fix bad upstream data.
- Kafka emit of the same process is ADR-0010. Do not bootstrap a taxi-only distribution for the speed layer.

## Sources

`docs/agents/workstreams.md`, `docs/team.md`, `docs/agents/schema.md`, [ADR-0010](0010-bengaluru-rnn-stream-resampler.md)

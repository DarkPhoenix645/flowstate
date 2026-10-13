# Docs

Read these in order. ~20 minutes.

| # | Doc | Why |
|---|---|---|
| 1 | [getting-started.md](getting-started.md) | Tooling, `make` targets, setup |
| 2 | [architecture.md](architecture.md) | Pipeline, CityPulse mapping |
| 3 | [team.md](team.md) | Workstreams, milestones, spikes |
| 4 | [dev-modes.md](dev-modes.md) | Host Python vs Compose; Kafka ports |
| 5 | [data-sources.md](data-sources.md) | Bengaluru sources, grains, windows, and what each file is for |
| 6 | [rejected-sources.md](rejected-sources.md) | Datasets checked and excluded, with the evidence |
| 7 | [resampling-plan.md](resampling-plan.md) | v1 resampler and streaming specification |

Root [README.md](../README.md) is the one-pager; this folder is the rest.

## Decisions and agent specs

| Path | Role |
|---|---|
| [adr/](adr/) | Accepted architecture choices (ADRs) |
| [agents/](agents/) | Schema contract, spikes, CityPulse reading list, workstream file lists. Not required for human onboarding. Index: [agents/README.md](agents/README.md) |

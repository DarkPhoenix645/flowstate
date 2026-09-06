# FlowState

Batch + real-time analytics on Indian urban mobility (ride-hailing, metro, bus GTFS, aviation).

Lambda-style pipeline: **ingest → HDFS lake → Spark/MapReduce batch → Hive → dashboard**, plus **Kafka replay → Structured Streaming** for live metrics. Architecture mirrors the EU FP7 CityPulse reference (not their Java/AMQP code). See [docs/architecture.md](docs/architecture.md).

## Quickstart (Windows / macOS / Linux)

HDFS, Kafka, Spark, Hive, and Superset run in **Docker Compose**. Host needs Docker, Git, uv, and Make — no host JDK.

1. Install **Docker Desktop** (Windows: WSL2 backend), **Git**, **uv**. GNU Make ships on macOS/Linux; Windows: **Git Bash** or WSL.
2. Clone, then:

```bash
cp .env.example .env   # fill KAGGLE_API_TOKEN when ingest is implemented
make setup
make test
make up
```

Every command is a **Make target** — [docs/getting-started.md](docs/getting-started.md). Runtime (host Python vs Compose Spark): [docs/dev-modes.md](docs/dev-modes.md).

## Repo map

| Path | What |
|---|---|
| `src/flowstate/ingest/` | Dataset loaders + amplify |
| `src/flowstate/batch/mapreduce/` | Hadoop MR baseline + Maven |
| `src/flowstate/benchmarks/` | Spark vs MR timings CSV |
| `src/flowstate/batch/spark_jobs/` | Descriptive + diagnostic Spark jobs |
| `src/flowstate/streaming/` | Kafka producer + rolling metrics |
| `src/flowstate/warehouse/` | Hive DDL + load |
| `dashboards/` | Superset export + Plotly/Streamlit |
| `infra/` | Hadoop / Hive / Kafka compose extras |
| `docs/` | Human onboarding |
| `docs/agents/` | Spec dump for agents; skip unless you need a contract |

Workstreams: [docs/team.md](docs/team.md).

## Docs

Start at **[docs/README.md](docs/README.md)**. Agent/spec corpus is `docs/agents/` — not required to get up to speed.

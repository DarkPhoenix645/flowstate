# Getting started

Identical on Windows (Git Bash or WSL), macOS, Linux. All heavy infra is Docker. Python on the host is `uv` + 3.11 (ingest, producer, tests). Spark, Hadoop, Hive, Kafka, Grafana never run on the host.

## Install once

| Tool                                  | Notes                                                                                       |
| ------------------------------------- | ------------------------------------------------------------------------------------------- |
| Docker Desktop                        | Windows: WSL2 backend. macOS: Apple Silicon OK. Linux: Docker Engine + Compose plugin.      |
| Git                                   | Windows: Git for Windows (includes Git Bash — use that for `make`).                         |
| [uv](https://docs.astral.sh/uv/)      | Pins 3.11 via `.python-version`.                                                            |
| GNU Make                              | macOS/Linux default. Windows: Git Bash `make` or WSL. We do **not** use `just`.             |
| [GitHub CLI](https://cli.github.com/) | `gh auth login` before `make ingest`. Ridehail, metro, DGCA, and BMTC reads use that token. |

No host JDK. Spark/Hadoop/Hive images already include Java. MapReduce jars are built with the `maven:3.9-eclipse-temurin-8` image (`make mr-package`).

```bash
git clone <repo> flowstate && cd flowstate
cp .env.example .env
make setup          # uv lock + sync --all-groups + config --check
make test
make up             # mints Kafka CLUSTER_ID once, then starts the stack
make down           # stop; keep volumes + CLUSTER_ID
make down-v         # DESTRUCTIVE: compose down -v + remint CLUSTER_ID
```

## Make targets

| Target                    | What                                                                                                                                                                   | State   |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `make setup`              | `uv lock`, `uv sync --all-groups`, create `data/*` dirs, print config                                                                                                  | WORKING |
| `make ingest`             | `DATASET=all` (or `ridehail` / `metro` / `bus` / `aviation` / `events` / `manifest` / `validate`)                                                                      | WORKING |
| `make lake`               | Put staged ride-hail parquet on HDFS. Host files stay. Path: `hdfs://namenode:9000/lake/namma_yatri/`.                                                                 | WORKING |
| `make amplify`            | `ROWS=1000000 SEED=42`. Host corpus, HDFS `/lake/rides_amplified/`, then Hive `rollup_conservation`.                                                                   | WORKING |
| `make evaluate`           | Fit and record the frozen hold-out evaluation (commits under `src/flowstate/benchmarks/results/`). How to read it: [resampler-evaluation.md](resampler-evaluation.md). | WORKING |
| `make up`                 | Mint `infra/kafka/cluster.env` if needed, then start Compose                                                                                                           | WORKING |
| `make down`               | Stop stack; **keeps** volumes and CLUSTER_ID                                                                                                                           | WORKING |
| `make down-v`             | Stop stack, **delete volumes** (`docker compose down -v`), remint CLUSTER_ID                                                                                           | WORKING |
| `make hotspots`           | Spark demand job **inside** the `spark` service                                                                                                                        | TODO    |
| `make spark-hdfs`         | Spark list + write probe against `hdfs://namenode:9000` (replication 1). Recreate Spark after a compose conf change.                                                   | WORKING |
| `make kafka-topics`       | Create the seven `flowstate.*.v1` topics with one partition each. Run it after `make up`. The producer exits on any other partition count.                           | WORKING |
| `make stream-produce`     | `RATE=500 SEED=42 PRODUCE_ROWS=10000`, or `REQUEST=request.json` (host → Kafka `127.0.0.1:9094`). All modes go out in one `event_time` order. [ADR-0017](adr/0017-producer-as-a-realtime-twin-source.md). | WORKING |
| `make stream-consume`     | rolling metrics **inside** Compose Spark                                                                                                                               | TODO    |
| `make hive-ddl`           | Create partitioned `rides`, put the city-day clock from the lake, then `MSCK REPAIR`.                                                                                 | WORKING |
| `make mr-package`         | Maven package in Docker (no host Java)                                                                                                                                 | WORKING |
| `make yarn-probe`         | Hadoop examples `pi` on YARN inside `resourcemanager`. Final state SUCCEEDED. Not DemandCount (#24).                                                                   | WORKING |
| `make test` / `make lint` | pytest, ruff                                                                                                                                                           | WORKING |

Examples:

```bash
gh auth login
make ingest DATASET=ridehail
make amplify ROWS=10000
make kafka-topics
make stream-produce RATE=100 PRODUCE_ROWS=10000
```

`make ingest DATASET=ridehail` reads commit lists and snapshot files from `DigitalIndiaArchiver/NammaYatriStats` with the `gh` token. Requests are one at a time. GitHub's primary limit is 5000 requests per hour; the stitch waits when remaining drops below 100, and on HTTP 403 or 429 it sleeps until `Retry-After` or the reset. City is 720 commits and ward is 459, so a clean run stays inside that budget. A failed snapshot exits before the CSV is written, and `DATASET=all` does not continue.

## Python extras

- default + `dev` + `viz` groups: `make setup` already uses `--all-groups`.
- `kafka-python-ng` is the Kafka client (pure Python; no librdkafka). Do not add `confluent-kafka` for mixed OS.
- Host Python is 3.11. Spark jobs use the Compose `apache/spark` image, not host `spark-submit`.

## CI

`.github/workflows/ci.yml` runs `uv sync`, ruff, pytest on Ubuntu, macOS, and Windows. PRs that only work on one OS should fail here, not at the demo.

## Windows Make

Recipes are POSIX (`uv`, `docker compose`). Run them from **Git Bash** or **WSL**, not `cmd.exe`. Docker Desktop must be running for `make up` and Spark/MR targets.

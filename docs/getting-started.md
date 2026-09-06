# Getting started

Identical on Windows (Git Bash or WSL), macOS, Linux. All heavy infra is Docker. Python on the host is `uv` + 3.11 (ingest, producer, tests). Spark, Hadoop, Hive, Kafka, Superset never run on the host.

## Install once

| Tool                             | Notes                                                                                  |
| -------------------------------- | -------------------------------------------------------------------------------------- |
| Docker Desktop                   | Windows: WSL2 backend. macOS: Apple Silicon OK. Linux: Docker Engine + Compose plugin. |
| Git                              | Windows: Git for Windows (includes Git Bash — use that for `make`).                    |
| [uv](https://docs.astral.sh/uv/) | Pins 3.11 via `.python-version`.                                                       |
| GNU Make                         | macOS/Linux default. Windows: Git Bash `make` or WSL. We do **not** use `just`.        |

No host JDK. Spark/Hadoop/Hive images already include Java. MapReduce jars are built with the `maven:3.9-eclipse-temurin-8` image (`make mr-package`).

```bash
git clone <repo> flowstate && cd flowstate
cp .env.example .env
make setup          # uv lock + sync --all-groups + config --check
make test
make up             # mints Kafka CLUSTER_ID once, then starts the stack
```

Kaggle credentials (`KAGGLE_API_TOKEN` from account API settings) are only needed once ingest downloaders exist.

```bash
make down           # stop; keep volumes + CLUSTER_ID
make down-v         # DESTRUCTIVE: compose down -v + remint CLUSTER_ID
```

## Make targets

| Target                    | What                                                                         | State   |
| ------------------------- | ---------------------------------------------------------------------------- | ------- |
| `make setup`              | `uv lock`, `uv sync --all-groups`, create `data/*` dirs, print config        | WORKING |
| `make ingest`             | `DATASET=all` (or `kaggle` / `bmrc` / `bmtc` / `mmrda` / `dgca`)             | TODO    |
| `make amplify`            | `ROWS=1000000` (also 10M / 100M later)                                       | TODO    |
| `make up`                 | Mint `infra/kafka/cluster.env` if needed, then start Compose                 | WORKING |
| `make down`               | Stop stack; **keeps** volumes and CLUSTER_ID                                 | WORKING |
| `make down-v`             | Stop stack, **delete volumes** (`docker compose down -v`), remint CLUSTER_ID | WORKING |
| `make hotspots`           | Spark demand job **inside** the `spark` service                              | TODO    |
| `make stream-produce`     | `TOPIC=rides.raw RATE=500` (host → Kafka `:9094`)                            | TODO    |
| `make stream-consume`     | rolling metrics **inside** Compose Spark                                     | TODO    |
| `make hive-ddl`           | beeline against `rides.sql` (needs stack up + Hive↔HDFS spike done)          | TODO    |
| `make mr-package`         | Maven package in Docker (no host Java)                                       | TODO    |
| `make test` / `make lint` | pytest, ruff                                                                 | WORKING |

Examples:

```bash
make ingest DATASET=kaggle
make amplify ROWS=10000
make stream-produce TOPIC=rides.raw RATE=100
```

## Python extras

- default + `dev` + `viz` groups: `make setup` already uses `--all-groups`.
- `kafka-python-ng` is the Kafka client (pure Python; no librdkafka). Do not add `confluent-kafka` for mixed OS.
- Host Python is 3.11. Spark jobs use the Compose `apache/spark` image, not host `spark-submit`.

## CI

`.github/workflows/ci.yml` runs `uv sync`, ruff, pytest on Ubuntu, macOS, and Windows. PRs that only work on one OS should fail here, not at the demo.

## Windows Make

Recipes are POSIX (`uv`, `docker compose`). Run them from **Git Bash** or **WSL**, not `cmd.exe`. Docker Desktop must be running for `make up` and Spark/MR targets.

## Key Points

- KRaft broker — no ZooKeeper.
- Host producers use EXTERNAL://localhost:9094 (see docker-compose.yml).
- In-cluster Spark use PLAINTEXT://kafka:9092.

## CLUSTER_ID

`make up` runs `uv run python infra/kafka/ensure_cluster_id.py`, which:

1. Creates `infra/kafka/cluster.id` once (same format as `kafka-storage.sh random-uuid`).
2. Writes `infra/kafka/cluster.env` for Compose interpolation.
3. Reuses the same id on later ups.

Both files are gitignored. They must stay stable while the `kafka_data` volume exists.

| Command       | Effect                                           |
| ------------- | ------------------------------------------------ |
| `make down`   | Stop containers; keep volumes + CLUSTER_ID       |
| `make down-v` | `docker compose down -v`, then remint CLUSTER_ID |

Raw `docker compose up` without the env file fails on purpose; use `make up`.

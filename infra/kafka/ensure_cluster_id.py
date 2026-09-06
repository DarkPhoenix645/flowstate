"""Mint a stable Kafka KRaft CLUSTER_ID for Compose interpolation.

Writes:
  infra/kafka/cluster.id   — raw id (reused across ups)
  infra/kafka/cluster.env  — CLUSTER_ID=... for `docker compose --env-file`

Same id format as `kafka-storage.sh random-uuid` (url-safe base64 UUID).
Do not delete cluster.id while the kafka_data volume still exists unless you
also wipe volumes (`make down-v`).
"""

from __future__ import annotations

import argparse
import base64
import uuid
from pathlib import Path

_DIR = Path(__file__).resolve().parent
_ID_FILE = _DIR / "cluster.id"
_ENV_FILE = _DIR / "cluster.env"


def _random_cluster_id() -> str:
    return base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("ascii").rstrip(
        "="
    )


def ensure_cluster_id(*, force: bool = False) -> str:
    if (
        force
        or not _ID_FILE.is_file()
        or not _ID_FILE.read_text(encoding="utf-8").strip()
    ):
        cluster_id = _random_cluster_id()
        _ID_FILE.write_text(cluster_id + "\n", encoding="utf-8")
        print(f"Wrote new Kafka CLUSTER_ID → {_ID_FILE}")
    else:
        cluster_id = _ID_FILE.read_text(encoding="utf-8").strip()

    _ENV_FILE.write_text(f"CLUSTER_ID={cluster_id}\n", encoding="utf-8")
    return cluster_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Mint a new id (only after make down-v / wiped kafka_data)",
    )
    args = parser.parse_args()
    ensure_cluster_id(force=args.force)


if __name__ == "__main__":
    main()

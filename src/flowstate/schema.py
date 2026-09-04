"""Canonical ride-event schema shared by ingest, producer, and streaming."""

from __future__ import annotations

from typing import Any

# Staged parquet + Kafka JSON payload. Extra source columns fold into extras.
RIDE_COLUMNS: list[str] = [
    "trip_id",
    "city",
    "mode",
    "vehicle_type",
    "timestamp",
    "pickup_zone",
    "drop_zone",
    "status",
    "fare",
    "cancel_reason",
    "source",
]


def empty_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {c: None for c in RIDE_COLUMNS}
    row.update(overrides)
    return row

"""Canonical ride columns for staged parquet and Hive.

Kafka does not use this column set. The envelope is ADR-0008, and it
lands in ``ride_event_schema.py`` when the producer is built.
"""

from __future__ import annotations

from typing import Any

# kind is "string" or "number".
# timestamp is ISO-8601 text. It is not a Spark timestamp.
RIDE_FIELDS: tuple[tuple[str, str], ...] = (
    ("trip_id", "string"),
    ("city", "string"),
    ("mode", "string"),
    ("vehicle_type", "string"),
    ("timestamp", "string"),
    ("pickup_zone", "string"),
    ("drop_zone", "string"),
    ("status", "string"),
    ("fare", "number"),
    ("cancel_reason", "string"),
    ("source", "string"),
)

RIDE_COLUMNS: list[str] = [name for name, _ in RIDE_FIELDS]

MODES: tuple[str, ...] = ("ride_hail", "metro", "bus", "aviation")
STATUSES: tuple[str, ...] = ("completed", "cancelled")

# Assigned when the source row has no vehicle class.
# GTFS route_type 3 is not copied. An empty bus token becomes bus.
ASSIGNED_VEHICLE_TYPE: dict[str, str] = {
    "ride_hail": "auto",
    "metro": "train",
    "bus": "bus",
    "aviation": "airplane",
}

# Hive partition values this contract names.
# Archive spellings Auto, Cab, and Bus fold onto auto, cab, and bus.
# Any other non-empty source token is pass-through.
DOCUMENTED_VEHICLE_TYPES: tuple[str, ...] = (
    "auto",
    "cab",
    "train",
    "bus",
    "airplane",
)

# T3 id carried by pickup_zone and drop_zone. Not extra columns.
ZONE_ID_BY_MODE: dict[str, str] = {
    "ride_hail": "ward_id",
    "metro": "station_id",
    "bus": "stop_id",
    "aviation": "airport",
}

_CANONICAL_VEHICLE_TYPE = {
    name.casefold(): name for name in DOCUMENTED_VEHICLE_TYPES
}


def empty_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {c: None for c in RIDE_COLUMNS}
    row.update(overrides)
    return row


def resolve_vehicle_type(
    mode: str, source_vehicle_type: str | None = None
) -> str:
    """Return the vehicle_type for one ride row.

    Documented values are auto, cab, train, bus, and airplane.
    Auto, Cab, and Bus fold to auto, cab, and bus.
    Any other non-empty source token is pass-through (copied).
    An empty token uses the assigned value: auto, train, bus, or airplane.
    """
    token = (source_vehicle_type or "").strip()
    if token:
        folded = _CANONICAL_VEHICLE_TYPE.get(token.casefold())
        if folded is not None:
            return folded
        return token
    try:
        return ASSIGNED_VEHICLE_TYPE[mode]
    except KeyError as exc:
        raise ValueError(
            f"{mode} has no assigned vehicle_type; "
            "pass the source token through"
        ) from exc


def ride_json_schema() -> dict[str, Any]:
    """JSON Schema for one ride row. Properties match RIDE_COLUMNS."""
    properties: dict[str, Any] = {}
    for name, kind in RIDE_FIELDS:
        if kind == "string":
            spec: dict[str, Any] = {"type": ["string", "null"]}
            if name == "timestamp":
                spec["format"] = "date-time"
        elif kind == "number":
            spec = {"type": ["number", "null"]}
        else:
            raise ValueError(f"unknown field kind {kind}")
        properties[name] = spec
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Canonical ride row",
        "type": "object",
        "properties": properties,
        "required": list(RIDE_COLUMNS),
        "additionalProperties": True,
    }


def ride_spark_schema() -> Any:
    """Spark StructType whose fields match RIDE_COLUMNS."""
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    spark_type = {"string": StringType(), "number": DoubleType()}
    return StructType(
        [
            StructField(name, spark_type[kind], nullable=True)
            for name, kind in RIDE_FIELDS
        ]
    )

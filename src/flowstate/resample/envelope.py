"""Corpus rows to the ADR-0008 Kafka envelope.

The lake parquet and the Kafka topics are different contracts. This
module maps one corpus row onto ``RideEvent``. It does not generate
events. ``city_id`` defaults to ``blr``, the ADR-0008 contract value,
the same literal the staged constructor writes. It is a contract
value, not a model input.

Bus events carry ``parameters_ref`` ``bus_prior`` (ADR-0016) and
``schema_version`` 1.1. Every other mode stays on 1.0 until it has a
parameter-table key. Synthetic fare stays an ordinary field.
"""

from __future__ import annotations

from typing import Any

from flowstate.resample.bundle import FARE_BASIS
from flowstate.resample.pins import PinBook
from flowstate.stage.bus import FEED_DATE
from flowstate.streaming.ride_event_schema import (
    BUS_PRIOR_PARAMETERS,
    envelope_json,
)
from flowstate.topic_names import AVIATION_TOPICS, TOPIC_BY_MODE


def topic_for(row: dict[str, Any]) -> str | None:
    if row["mode"] == "aviation":
        return AVIATION_TOPICS.get(row.get("direction"))
    return TOPIC_BY_MODE[row["mode"]]


def to_envelope(
    row: dict[str, Any],
    *,
    city_id: str = "blr",
    emitted_at: str | None = None,
    pins: PinBook | None = None,
) -> dict[str, Any]:
    """One corpus row as a validated ADR-0008 event envelope.

    ``pins`` copies the geography point onto ``location_ref``. A miss
    stays null. Ride-hail also copies the drop ward. That drop is an
    independent draw, not an observed origin-destination.
    """
    mode = row["mode"]
    parameters_ref = BUS_PRIOR_PARAMETERS if mode == "bus" else None
    location: dict[str, Any] = {"at": _place_for(row, pins)}
    if mode == "ride_hail":
        location["drop"] = _drop_place(row, pins)
    event: dict[str, Any] = {
        "schema_version": "1.1" if parameters_ref else "1.0",
        "event_id": row["trip_id"],
        "city_id": city_id,
        "mode": mode,
        "event_type": row["event_type"],
        "event_time": row["timestamp"],
        "emitted_at": emitted_at,
        "count": row["count"],
        "location_ref": location,
        "scenario_id": row["scenario_id"],
        "parameters_ref": parameters_ref,
        "provenance": {
            "status": row["provenance_status"],
            "source_ids": [
                part for part in str(row["source_ids"]).split(",") if part
            ],
            "model_bundle_version": row["model_bundle_version"],
            "profile_version": row.get("profile_version"),
            "seed_id": row["seed_id"],
            "evidence_grade": row["evidence_grade"],
            "outside_training_window": row["outside_training_window"]
            == "true",
        },
    }
    if mode == "metro":
        event["entries_count"] = row["entries_count"]
        event["exits_count"] = row["exits_count"]
        event["measure"] = row.get("measure")
        event["interval_start"] = f"{int(row['hour']):02d}:00"
        event["interval_end"] = f"{int(row['hour']) + 1:02d}:00"
    elif mode == "bus":
        event["trip_id"] = row["trip_id"]
        event["stop_sequence"] = row.get("stop_sequence")
        event["feed_version"] = FEED_DATE
        event["ridership"] = {
            "boardings": row["boardings"],
            "alightings": row["alightings"],
            "onboard_leaving": row["onboard_leaving"],
            "provenance": {
                "status": row["ridership_status"],
                "evidence_grade": row["ridership_grade"],
            },
        }
    elif mode == "aviation":
        event["direction"] = row["direction"]
        event["icao24"] = row["icao24"]
        event["callsign"] = row["callsign"]
    elif mode == "ride_hail" and row["event_type"] == "completed_ride":
        event["synthetic_fare_inr"] = row[FARE_BASIS]
    return envelope_json(event)


def _place_for(row: dict[str, Any], pins: PinBook | None) -> dict[str, Any]:
    mode = row["mode"]
    key = str(row["trip_id"])
    if mode == "ride_hail":
        return _ward_place(row.get("pickup_zone"), key, pins)
    if mode == "metro":
        station = row.get("pickup_zone")
        return _fixed_place(
            "station_id",
            station,
            None if pins is None else pins.station(station),
            "station_pin",
            pins,
        )
    if mode == "bus":
        stop = row.get("pickup_zone")
        return _fixed_place(
            "stop_id",
            stop,
            None if pins is None else pins.stop(stop),
            "stop_gtfs",
            pins,
        )
    airport = (
        row["drop_zone"]
        if row.get("direction") == "arrival"
        else row.get("pickup_zone")
    )
    return _fixed_place(
        "airport",
        airport,
        None if pins is None else pins.airport(airport),
        "airport_pin",
        pins,
    )


def _drop_place(row: dict[str, Any], pins: PinBook | None) -> dict[str, Any]:
    return _ward_place(
        row.get("drop_zone"), f"{row['trip_id']}|drop", pins
    )


def _ward_place(
    ward_id: str | None, key: str, pins: PinBook | None
) -> dict[str, Any]:
    place: dict[str, Any] = {}
    if ward_id is not None:
        place["ward_id"] = ward_id
    if pins is None:
        return place
    found = pins.ward_point(ward_id, key)
    if found is None:
        place.update(latitude=None, longitude=None, placement=None)
    else:
        place.update(
            latitude=found[0], longitude=found[1], placement=found[2]
        )
    return place


def _fixed_place(
    field: str,
    ident: str | None,
    point: tuple[float, float] | None,
    placement: str,
    pins: PinBook | None,
) -> dict[str, Any]:
    place: dict[str, Any] = {}
    if ident is not None:
        place[field] = ident
    if pins is None:
        return place
    if point is None:
        place.update(latitude=None, longitude=None, placement=None)
    else:
        place.update(
            latitude=point[0], longitude=point[1], placement=placement
        )
    return place

"""Corpus rows to the ADR-0008 Kafka envelope.

The lake parquet and the Kafka topics are different contracts: the
parquet is the canonical ride schema plus extras
(``docs/agents/schema.md``), the envelope is ADR-0008
(``flowstate.streaming.ride_event_schema.py``, filled by #39). This
module maps one onto the other so the producer (#39) never grows a
second generator.

``city_id`` defaults to ``blr``, the ADR-0008 contract value, the
same literal ``flowstate.stage.events`` writes. It is a contract
value, not a model input.
"""

from __future__ import annotations

from typing import Any

from flowstate.resample.bundle import FARE_BASIS

TOPIC_BY_MODE = {
    "ride_hail": "flowstate.ridehail.activity.v1",
    "metro": "flowstate.metro.station_hour.v1",
    "bus": "flowstate.bus.scheduled_stop.v1",
    "aviation": None,  # arrival and departure are separate topics
}

AVIATION_TOPICS = {
    "arrival": "flowstate.aviation.arrival.v1",
    "departure": "flowstate.aviation.departure.v1",
}


def topic_for(row: dict[str, Any]) -> str | None:
    if row["mode"] == "aviation":
        return AVIATION_TOPICS.get(row.get("direction"))
    return TOPIC_BY_MODE[row["mode"]]


def to_envelope(
    row: dict[str, Any], *, city_id: str = "blr"
) -> dict[str, Any]:
    """One corpus row as an ADR-0008 event envelope."""
    mode = row["mode"]
    location = {
        "ward_id": None,
        "station_id": None,
        "stop_id": None,
        "airport": None,
    }
    if mode == "ride_hail":
        location["ward_id"] = row["pickup_zone"]
    elif mode == "metro":
        location["station_id"] = row["pickup_zone"]
    elif mode == "bus":
        location["stop_id"] = row["pickup_zone"]
    elif mode == "aviation":
        location["airport"] = (
            row["drop_zone"]
            if row.get("direction") == "arrival"
            else row["pickup_zone"]
        )
    event: dict[str, Any] = {
        "schema_version": "1.0",
        "event_id": row["trip_id"],
        "city_id": city_id,
        "mode": mode,
        "event_type": row["event_type"],
        "event_time": row["timestamp"],
        "emitted_at": None,
        "count": row["count"],
        "location_ref": location,
        "scenario_id": row["scenario_id"],
        "provenance": {
            "status": row["provenance_status"],
            "source_ids": [
                part for part in str(row["source_ids"]).split(",") if part
            ],
            "model_bundle_version": row["model_bundle_version"],
            "profile_version": None,
            "seed_id": row["seed_id"],
            "evidence_grade": row["evidence_grade"],
            "outside_training_window": row["outside_training_window"]
            == "true",
        },
    }
    if mode == "metro":
        event["entries_count"] = row["entries_count"]
        event["exits_count"] = row["exits_count"]
        event["measure"] = None
        event["interval_start"] = f"{int(row['hour']):02d}:00"
        event["interval_end"] = f"{int(row['hour']) + 1:02d}:00"
    elif mode == "bus":
        event["trip_id"] = row["trip_id"]
        event["stop_sequence"] = None
        event["feed_version"] = None
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
    return event

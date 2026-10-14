"""Envelope payloads for the staged metro and bus rows.

ADR-0008. These functions do not publish. There is no DGCA topic and
no enrichment topic. Bus boardings are not filled here.
"""

from __future__ import annotations

from typing import Any

METRO_TOPIC = "flowstate.metro.station_hour.v1"
BUS_TOPIC = "flowstate.bus.scheduled_stop.v1"

# Provenance status values from ADR-0008. observed_aggregate is not one of them.
PROVENANCE_STATUS = (
    "observed",
    "observed_proxy",
    "derived",
    "synthetic",
)


def metro_station_hour_event(row: dict[str, Any]) -> dict[str, Any]:
    """One RTI station-hour row as an envelope event.

    The envelope status is ``observed``. The ticket word
    ``observed_aggregate`` is not an ADR-0008 status.
    """
    measure = row["measure"]
    count = int(row["count"])
    entries = count if measure == "entry" else None
    exits = count if measure == "exit" else None
    station = str(row["station"])
    event_time = f"{row['service_date']}T{row['interval_start']}:00+05:30"
    event_id = (
        f"bmrcl-{row['service_date']}-{station}-{measure}-"
        f"{row['interval_start']}"
    )
    return _envelope(
        event_id=event_id,
        mode="metro",
        event_type="station_hour",
        event_time=event_time,
        count=count,
        location_ref=_location(station_id=station),
        status="observed",
        source_ids=["bmrcl_rti"],
        evidence_grade="aggregate_calibrated",
        extra={
            "entries_count": entries,
            "exits_count": exits,
            "measure": measure,
            "interval_start": row["interval_start"],
            "interval_end": row["interval_end"],
            "ends_at_last_train": bool(row["ends_at_last_train"]),
            "source_sheet": row["source_sheet"],
            "source_row": int(row["source_row"]),
        },
    )


def bus_scheduled_stop_event(row: dict[str, Any]) -> dict[str, Any]:
    """One GTFS stop call. Ridership counts stay null until #40."""
    stop_id = str(row["stop_id"])
    event_time = _gtfs_event_time(row["service_date"], row["arrival_time"])
    event_id = f"bmtc-{row['trip_id']}-{row['stop_sequence']}"
    event = _envelope(
        event_id=event_id,
        mode="bus",
        event_type="scheduled_stop",
        event_time=event_time,
        count=1,
        location_ref=_location(stop_id=stop_id),
        status="derived",
        source_ids=["bmtc_gtfs"],
        evidence_grade="shape_prior",
        extra={
            "trip_id": str(row["trip_id"]),
            "stop_sequence": int(row["stop_sequence"]),
            "feed_version": row.get("feed_version"),
        },
    )
    event["ridership"] = {
        "boardings": None,
        "alightings": None,
        "onboard_leaving": None,
        "provenance": {
            "status": "synthetic",
            "evidence_grade": "assumption_driven",
        },
    }
    return event


def _envelope(
    *,
    event_id: str,
    mode: str,
    event_type: str,
    event_time: str,
    count: int,
    location_ref: dict[str, Any],
    status: str,
    source_ids: list[str],
    evidence_grade: str,
    extra: dict[str, Any],
) -> dict[str, Any]:
    if status not in PROVENANCE_STATUS:
        raise ValueError(f"bad provenance status {status}")
    event = {
        "schema_version": "1.0",
        "event_id": event_id,
        "city_id": "blr",
        "mode": mode,
        "event_type": event_type,
        "event_time": event_time,
        "emitted_at": None,
        "count": count,
        "location_ref": location_ref,
        "scenario_id": None,
        "provenance": {
            "status": status,
            "source_ids": source_ids,
            "model_bundle_version": None,
            "profile_version": None,
            "seed_id": None,
            "evidence_grade": evidence_grade,
            "outside_training_window": False,
        },
    }
    event.update(extra)
    return event


def _location(
    *,
    ward_id: str | None = None,
    station_id: str | None = None,
    stop_id: str | None = None,
    airport: str | None = None,
) -> dict[str, str | None]:
    return {
        "ward_id": ward_id,
        "station_id": station_id,
        "stop_id": stop_id,
        "airport": airport,
    }


def _gtfs_event_time(service_date: str, clock: str) -> str:
    hour, minute, second = clock.split(":")
    return (
        f"{service_date}T{int(hour):02d}:{int(minute):02d}:"
        f"{int(second):02d}+05:30"
    )

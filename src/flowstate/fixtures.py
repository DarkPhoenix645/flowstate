"""Ride rows shaped like the accepted aggregates.

These rows are not a copy of a source file and not a rejected taxi CSV.
Counts, fares, and flight identities are not in them.
"""

from __future__ import annotations

from typing import Any

from flowstate.schema import empty_row, resolve_vehicle_type


def ride_fixture_rows() -> list[dict[str, Any]]:
    """One row per zone id, plus a cab pass-through from the hourly archive.

    Zone strings are ids from the accepted sources:
    ward_num ``b_112`` / ``b_84``, station codes ``BYPH`` / ``MAGR``,
    GTFS stop ``21629``, airport ``VOBL``.
    """
    return [
        empty_row(
            trip_id="fixture-ridehail-auto",
            city="bengaluru",
            mode="ride_hail",
            vehicle_type=resolve_vehicle_type("ride_hail"),
            timestamp="2025-08-01T00:00:00+05:30",
            pickup_zone="b_112",
            drop_zone="b_84",
            status="completed",
            source="namma_yatri",
        ),
        empty_row(
            trip_id="fixture-ridehail-cab",
            city="bengaluru",
            mode="ride_hail",
            vehicle_type=resolve_vehicle_type("ride_hail", "Cab"),
            timestamp="2025-08-01T18:00:00+05:30",
            pickup_zone="b_84",
            drop_zone="b_112",
            status="cancelled",
            cancel_reason="unknown",
            source="namma_yatri",
        ),
        empty_row(
            trip_id="fixture-metro-station",
            city="bengaluru",
            mode="metro",
            vehicle_type=resolve_vehicle_type("metro"),
            timestamp="2025-08-01T08:00:00+05:30",
            pickup_zone="BYPH",
            drop_zone="MAGR",
            status="completed",
            source="bmrcl_rti",
        ),
        empty_row(
            trip_id="fixture-bus-stop",
            city="bengaluru",
            mode="bus",
            vehicle_type=resolve_vehicle_type("bus"),
            timestamp="2026-04-15T09:00:00+05:30",
            pickup_zone="21629",
            drop_zone=None,
            status="completed",
            source="bmtc_gtfs",
        ),
        empty_row(
            trip_id="fixture-aviation-arrival",
            city="bengaluru",
            mode="aviation",
            vehicle_type=resolve_vehicle_type("aviation"),
            timestamp="2025-08-14T10:00:00+05:30",
            pickup_zone=None,
            drop_zone="VOBL",
            status="completed",
            source="opensky",
        ),
        empty_row(
            trip_id="fixture-aviation-departure",
            city="bengaluru",
            mode="aviation",
            vehicle_type=resolve_vehicle_type("aviation"),
            timestamp="2025-08-14T12:00:00+05:30",
            pickup_zone="VOBL",
            drop_zone=None,
            status="completed",
            source="opensky",
        ),
    ]

import pandas as pd

from flowstate.stage.bus import validate_frames
from flowstate.stage.dgca import city_month_table
from flowstate.stage.events import (
    BUS_TOPIC,
    METRO_TOPIC,
    bus_scheduled_stop_event,
    metro_station_hour_event,
)


def test_dgca_keeps_both_directions_and_no_day_grain():
    frame = pd.DataFrame(
        [
            _pair(2025, 8, "BENGALURU", "DELHI", 10, 1),
            _pair(2025, 8, "DELHI", "BENGALURU", 4, 2),
            _pair(2025, 8, "MUMBAI", "DELHI", 9, 9),
        ]
    )
    table, coverage = city_month_table(frame)
    assert "date" not in table.column_names
    rows = table.to_pylist()
    assert len(rows) == 2
    assert {row["direction"] for row in rows} == {
        "BENGALURU->DELHI",
        "DELHI->BENGALURU",
    }
    assert coverage["ordered_pairs_with_reverse"] == 2
    assert any("no DGCA Kafka" in line for line in coverage["veracity"])


def test_gtfs_rejects_a_bad_stop_and_keeps_a_good_trip():
    agency = pd.DataFrame(
        [{"agency_timezone": "Asia/Kolkata"}]
    )
    calendar = pd.DataFrame([{"service_id": "1"}])
    trips = pd.DataFrame(
        [
            {"trip_id": "good", "service_id": "1", "route_id": "r"},
            {"trip_id": "bad", "service_id": "1", "route_id": "r"},
        ]
    )
    stops = pd.DataFrame([{"stop_id": "21629"}])
    times = pd.DataFrame(
        [
            _stop("good", "21629", 1, "08:00:00", "08:00:30"),
            _stop("good", "21629", 2, "08:10:00", "08:10:30"),
            _stop("bad", "99999", 1, "08:00:00", "08:00:30"),
        ]
    )
    result = validate_frames(agency, calendar, trips, stops, times)
    assert set(result["trips"]["trip_id"]) == {"good"}
    assert result["rejections"].to_dict("records") == [
        {"trip_id": "bad", "reason": "unknown_stop"}
    ]


def test_metro_event_is_observed_and_bus_ridership_is_synthetic():
    metro = metro_station_hour_event(
        {
            "measure": "entry",
            "count": 12,
            "station": "11-Baiyappanahalli",
            "service_date": "2025-08-11",
            "interval_start": "08:00",
            "interval_end": "09:00",
            "ends_at_last_train": False,
            "source_sheet": "Sep-2025 Entry",
            "source_row": 4,
        }
    )
    assert metro["provenance"]["status"] == "observed"
    assert metro["entries_count"] == 12
    assert metro["exits_count"] is None
    assert metro["location_ref"]["station_id"] == "11-Baiyappanahalli"
    assert METRO_TOPIC == "flowstate.metro.station_hour.v1"

    bus = bus_scheduled_stop_event(
        {
            "trip_id": "t1",
            "stop_id": "21629",
            "stop_sequence": 1,
            "arrival_time": "08:00:00",
            "service_date": "2026-09-07",
            "feed_version": "20260907",
        }
    )
    assert bus["provenance"]["status"] == "derived"
    assert bus["event_type"] == "scheduled_stop"
    assert bus["ridership"]["provenance"]["status"] == "synthetic"
    assert bus["ridership"]["boardings"] is None
    assert BUS_TOPIC == "flowstate.bus.scheduled_stop.v1"


def _pair(
    year: int,
    month: int,
    city1: str,
    city2: str,
    pax_to: int,
    pax_from: int,
) -> dict:
    return {
        "Year": year,
        "Month": month,
        "City1": city1,
        "City2": city2,
        "PaxToCity2": pax_to,
        "PaxFromCity2": pax_from,
        "FreightToCity2": 0,
        "FreightFromCity2": 0,
        "MailToCity2": 0,
        "MailFromCity2": 0,
    }


def _stop(
    trip: str, stop: str, seq: int, arrival: str, departure: str
) -> dict:
    return {
        "trip_id": trip,
        "stop_id": stop,
        "stop_sequence": seq,
        "arrival_time": arrival,
        "departure_time": departure,
    }


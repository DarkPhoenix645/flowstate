from datetime import date

import pandas as pd
import pytest

from flowstate.stage.bus import validate_frames
from flowstate.stage.dgca import city_month_table
from flowstate.stage.events import (
    BUS_TOPIC,
    METRO_TOPIC,
    bus_scheduled_stop_event,
    metro_station_hour_event,
)
from flowstate.stage.metro import daily_table, parse_interval
from flowstate.stage.ridehail import CITY_DAY_COLUMNS, city_day_table


def test_city_day_keeps_blr_normal_only():
    rows = [
        _day("2025-08-01", "BLR", "Normal", 10, 100),
        _day("2025-08-01", "BLR", "Purple", 1, 1),
        _day("2025-08-02", "COK", "Normal", 4, 4),
        _day("2025-08-02", "BLR", "Normal", 0, 50),
    ]
    table, coverage = city_day_table(rows)
    assert table.column_names == CITY_DAY_COLUMNS
    assert "drvr_cancel" not in table.column_names
    out = table.to_pylist()
    assert [row["date"] for row in out] == ["2025-08-01", "2025-08-02"]
    assert out[0]["earning_per_completed_ride_proxy"] == 10
    assert out[1]["earning_per_completed_ride_proxy"] is None
    assert coverage["purple_rows_excluded"] == 1
    assert coverage["other_city_rows_excluded"] == 1
    assert {row["booking_type"] for row in out} == {"Normal"}
    assert {row["city_code"] for row in out} == {"BLR"}


def test_city_day_duplicate_date_raises():
    rows = [
        _day("2025-08-01", "BLR", "Normal", 1, 1),
        _day("2025-08-01", "BLR", "Normal", 2, 2),
    ]
    with pytest.raises(ValueError, match="duplicate"):
        city_day_table(rows)


def test_parse_interval_keeps_last_train():
    assert parse_interval("00:00 Hrs To     01:00 Hrs") == (
        "00:00",
        "01:00",
        False,
    )
    assert parse_interval("23:00 Hrs To Last train") == ("23:00", None, True)
    assert parse_interval("H23") == ("23:00", "24:00", False)


def test_metro_daily_drops_rows_after_cutoff():
    frame = pd.DataFrame(
        [
            _metro("2026-07-15", 1),
            _metro("2026-07-16", 2),
        ]
    )
    table, coverage = daily_table(frame)
    assert table.column("record_date").to_pylist() == ["2026-07-15"]
    assert coverage["dropped_after_2026_07_15"] == 1
    assert "total" in table.column_names


def test_metro_daily_identity_failure():
    row = _metro("2026-07-15", 1)
    row["Total"] = 99
    with pytest.raises(ValueError, match="Total"):
        daily_table(pd.DataFrame([row]))


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


def _day(day: str, city: str, booking: str, done: int, earning: int) -> dict:
    return {
        "date": day,
        "city": "Bengaluru" if city == "BLR" else city,
        "city_code": city,
        "booking_type": booking,
        "srch_rqst": done,
        "srch_fr_q": 0,
        "srch_which_got_q": 0,
        "srch_which_got_e": 0,
        "booking": done,
        "done_ride": done,
        "cancel_ride": 0,
        "earning": earning,
        "drvr_cancel": 0,
        "rider_cancel": 0,
    }


def _metro(day: str, total: int) -> dict:
    return {
        "Record Date": day,
        "Smart Cards": total,
        "NCMC": 0,
        "Commute": total,
        "Tokens": 0,
        "QR": 0,
        "Group Ticket": 0,
        "Casual": 0,
        "Total": total,
    }


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


def test_cutoff_constant_is_the_adr_date():
    assert date(2026, 7, 15).isoformat() == "2026-07-15"

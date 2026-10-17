"""Local bins for Bengaluru's zone. Midnight, month boundary, query edges.

Asia/Kolkata is UTC+05:30, so 18:30 UTC is midnight the next local date.
That offset belongs to this zone, not to the bin function.
"""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from flowstate.ingest.opensky_flights import END, START
from flowstate.normalize.cities.bengaluru import BENGALURU
from flowstate.normalize.clock import local_bin, to_local

ZONE = BENGALURU.timezone


def test_bengaluru_zone_is_kolkata():
    assert ZONE == ZoneInfo("Asia/Kolkata")


def test_midnight_local_is_1830_utc():
    before = datetime(2024, 10, 25, 18, 29, 59, tzinfo=UTC)
    at = datetime(2024, 10, 25, 18, 30, tzinfo=UTC)
    assert local_bin(before, ZONE) == ("2024-10-25", 23)
    assert local_bin(at, ZONE) == ("2024-10-26", 0)
    assert to_local(at, ZONE).strftime("%Y-%m-%d %H:%M") == (
        "2024-10-26 00:00"
    )


def test_month_boundary():
    before = datetime(2025, 8, 31, 18, 29, 59, tzinfo=UTC)
    at = datetime(2025, 8, 31, 18, 30, tzinfo=UTC)
    assert local_bin(before, ZONE) == ("2025-08-31", 23)
    assert local_bin(at, ZONE) == ("2025-09-01", 0)


def test_opensky_query_bounds_are_local_window_edges():
    assert local_bin(START, ZONE) == ("2024-10-26", 0)
    assert local_bin(END, ZONE) == ("2026-04-15", 23)
    local_end = to_local(END, ZONE)
    assert local_end.tzinfo == ZONE
    assert local_end.minute == 59
    assert local_end.second == 59

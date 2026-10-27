"""Demand grain and the weekday-adjusted association."""

from datetime import date
from pathlib import Path

from flowstate.batch.association import weekday_adjusted_slope
from flowstate.batch.demand import day_type_name, is_demand_row


def test_day_type_is_weekday_through_friday():
    assert day_type_name(0) == "weekday"
    assert day_type_name(4) == "weekday"
    assert day_type_name(5) == "weekend"
    assert day_type_name(date(2024, 10, 26).weekday()) == "weekend"


def test_another_city_is_skipped():
    assert is_demand_row("ride_hail", "bengaluru") is True
    assert is_demand_row("ride_hail", "pune") is False
    assert is_demand_row("metro", "bengaluru") is False


def test_slope_is_an_association_with_an_interval():
    rows = [
        (float(i + flag), float(i), flag)
        for i in range(6)
        for flag in (0, 1)
    ]
    fit = weekday_adjusted_slope(rows)
    assert fit["n"] == 12
    assert fit["estimate"] == 1
    assert fit["interval_low"] <= fit["estimate"] <= fit["interval_high"]


def test_make_hotspots_is_gone():
    text = Path("Makefile").read_text()
    assert "hotspots:" not in text
    assert "stream-produce:" in text

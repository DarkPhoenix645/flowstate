"""Demand grain. Spark (#18) and MapReduce DemandCount (#24) share it.

A ride-hail row is one booking. ``demand_count`` is the number of
those rows. ``hour_of_day`` is 0–23 in Asia/Kolkata. ``day_type`` is
``weekday`` for Monday–Friday and ``weekend`` otherwise.
``datetime.weekday()`` is 0 on Monday.
"""

from __future__ import annotations


def day_type_name(weekday: int) -> str:
    """Monday is 0, Sunday is 6."""
    if weekday < 0 or weekday > 6:
        raise ValueError("weekday must be 0 through 6")
    return "weekday" if weekday <= 4 else "weekend"


def is_demand_row(mode: str, city: str | None) -> bool:
    """Bengaluru ride-hail counts. Another city is skipped, not a crash."""
    if mode != "ride_hail":
        return False
    return city in (None, "", "bengaluru")

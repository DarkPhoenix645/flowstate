"""Hour z-score against the #18 ward × hour × day-type baseline."""

from __future__ import annotations

import math
from datetime import date, timedelta


def day_type_days(start: date, end: date, day_type: str) -> int:
    """How many dates in the span have this day type. At least one."""
    if end < start:
        raise ValueError("window end is before start")
    if day_type not in ("weekday", "weekend"):
        raise ValueError("day_type must be weekday or weekend")
    count = 0
    cursor = start
    while cursor <= end:
        weekday = cursor.weekday() <= 4
        if day_type == "weekday" and weekday:
            count += 1
        if day_type == "weekend" and not weekday:
            count += 1
        cursor += timedelta(days=1)
    return max(count, 1)


def expected_hour(demand_count: float, days: int) -> float:
    """Corpus total for that grain, spread over the days it occurred."""
    if days < 1:
        raise ValueError("days must be positive")
    return float(demand_count) / float(days)


def poisson_z(observed: float, expected: float) -> float | None:
    """(observed - expected) / sqrt(expected). No score when expected is 0."""
    if expected <= 0:
        return None
    return (observed - expected) / math.sqrt(expected)

"""UTC to a city's local zone, and calendar spans.

The zone and the window come from the city profile. 18:30 UTC is midnight
the next day in Asia/Kolkata, which is Bengaluru's zone. It is not a
property of every city.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo


def to_local(value: datetime, zone: ZoneInfo) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamp needs a timezone")
    return value.astimezone(zone)


def local_bin(value: datetime, zone: ZoneInfo) -> tuple[str, int]:
    """Local calendar date and hour for one UTC timestamp."""
    local = to_local(value, zone)
    return local.date().isoformat(), local.hour


def unix_to_utc(seconds: int) -> datetime:
    return datetime.fromtimestamp(int(seconds), UTC)


def iso_utc(seconds: int) -> str:
    return unix_to_utc(seconds).isoformat()


def in_window(day: str, start: date, end: date) -> bool:
    return start.isoformat() <= day <= end.isoformat()


def span_dates(start: date, end: date) -> list[str]:
    days: list[str] = []
    cursor = start
    while cursor <= end:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def dates_covering(spans: tuple[tuple[date, date], ...]) -> set[str]:
    covered: set[str] = set()
    for start, end in spans:
        covered.update(span_dates(start, end))
    return covered


def missing_dates(dates: list[str]) -> list[str]:
    """Dates absent between the earliest and latest. Does not fill them."""
    ordered = sorted(set(dates))
    if not ordered:
        return []
    start = date.fromisoformat(ordered[0])
    end = date.fromisoformat(ordered[-1])
    have = set(ordered)
    missing: list[str] = []
    cursor = start
    while cursor <= end:
        text = cursor.isoformat()
        if text not in have:
            missing.append(text)
        cursor += timedelta(days=1)
    return missing


def future_dates(
    dates: list[str], as_of: date | None = None
) -> list[str]:
    cutoff = as_of or date.today()
    return sorted(
        day
        for day in set(dates)
        if date.fromisoformat(day) > cutoff
    )

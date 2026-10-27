"""One coverage rule for OpenSky day-directions (ADR-0017).

OpenSky airport windows can fail or come back thin. Thin windows are
common at the old end of the alignment window. A thin day is a gap in
the fetch. It is not a low-traffic day (``docs/data-sources.md``, plan
§14). The generator and the daily fit both use this rule, so a gap is
neither emitted as ``observed_proxy`` nor learned as a low day.

A day-direction is covered when all of these hold:

* no row came from a failed or quota window, and
* its event count is at least ``SPARSE_FRACTION`` of the direction's
  reference level. The reference is the ``REFERENCE_QUANTILE`` of the
  direction's daily counts that came from healthy windows.

The 75th percentile is the reference, not the median. A patchy fetch
drags the median down and lets thin days through. On the checked data
the reference is about 350 events a day, which matches the documented
330 to 375.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa

SPARSE_FRACTION = 0.5
REFERENCE_QUANTILE = 75
FAILED_QUERY_STATUS = frozenset({"failed", "quota"})
DIRECTIONS = ("arrival", "departure")


def observed_counts(
    flight_events: pa.Table,
) -> tuple[dict[str, dict[str, int]], dict[str, set[str]]]:
    """Event count per day, and the days that touch a failed window."""
    columns = ["direction", "local_date"]
    has_status = "query_status" in flight_events.column_names
    if has_status:
        columns.append("query_status")
    frame = flight_events.select(columns).to_pydict()
    statuses = (
        frame["query_status"]
        if has_status
        else [None] * len(frame["direction"])
    )
    counts: dict[str, dict[str, int]] = {name: {} for name in DIRECTIONS}
    failed: dict[str, set[str]] = {name: set() for name in DIRECTIONS}
    for direction, day, status in zip(
        frame["direction"], frame["local_date"], statuses, strict=True
    ):
        if not day or direction not in counts:
            continue
        key = str(day)[:10]
        counts[direction][key] = counts[direction].get(key, 0) + 1
        if status in FAILED_QUERY_STATUS:
            failed[direction].add(key)
    return counts, failed


def reference_level(
    counts: dict[str, int], failed: set[str]
) -> float:
    healthy = [value for day, value in counts.items() if day not in failed]
    if not healthy:
        return 0.0
    return float(np.percentile(np.asarray(healthy), REFERENCE_QUANTILE))


def covered_counts(flight_events: pa.Table) -> dict[str, dict[str, int]]:
    """Counts for the covered day-directions only."""
    counts, failed = observed_counts(flight_events)
    kept: dict[str, dict[str, int]] = {}
    for direction in DIRECTIONS:
        level = reference_level(counts[direction], failed[direction])
        floor = SPARSE_FRACTION * level
        kept[direction] = {
            day: value
            for day, value in counts[direction].items()
            if day not in failed[direction] and value >= floor
        }
    return kept


def gated_keys(flight_events: pa.Table) -> set[tuple[str, str]]:
    """(day, direction) pairs that have rows and fail the rule."""
    counts, _ = observed_counts(flight_events)
    kept = covered_counts(flight_events)
    gated: set[tuple[str, str]] = set()
    for direction in DIRECTIONS:
        for day in counts[direction]:
            if day not in kept[direction]:
                gated.add((day, direction))
    return gated


def coverage_summary(flight_events: pa.Table) -> dict[str, Any]:
    counts, failed = observed_counts(flight_events)
    kept = covered_counts(flight_events)
    return {
        direction: {
            "days_with_rows": len(counts[direction]),
            "covered_days": len(kept[direction]),
            "reference_level": reference_level(
                counts[direction], failed[direction]
            ),
        }
        for direction in DIRECTIONS
    }

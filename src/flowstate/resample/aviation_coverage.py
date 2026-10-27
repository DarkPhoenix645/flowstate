"""One coverage rule for OpenSky day-directions (ADR-0017).

OpenSky airport windows can fail or come back thin. Thin windows are
common at the old end of the alignment window. A thin day is a gap in
the fetch. It is not a low-traffic day (``docs/data-sources.md``, plan
§14). The generator and the daily fit both use this rule, so a gap is
neither emitted as ``observed_proxy`` nor learned as a low day.

A day-direction is covered when all of these hold:

* no row came from a failed status on the coverage rule, and
* its event count is at least the rule's sparse fraction of the
  direction's reference level. The reference is the rule's quantile
  of the direction's daily counts that came from healthy windows.

The rule is the one on the bundle. These functions do not read a
module constant, so a later edit cannot move a bundle that is
already built.

The 75th percentile is the reference, not the median. A patchy fetch
drags the median down and lets thin days through. On the checked data
the reference is about 350 events a day, which matches the documented
330 to 375.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pyarrow as pa

from flowstate.config.model import CoverageRule

DIRECTIONS = ("arrival", "departure")


def _rule(rule: CoverageRule | None) -> CoverageRule:
    return rule if rule is not None else CoverageRule()


def observed_counts(
    flight_events: pa.Table,
    rule: CoverageRule | None = None,
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
    failed_status = set(_rule(rule).failed_statuses)
    counts: dict[str, dict[str, int]] = {name: {} for name in DIRECTIONS}
    failed: dict[str, set[str]] = {name: set() for name in DIRECTIONS}
    for direction, day, status in zip(
        frame["direction"], frame["local_date"], statuses, strict=True
    ):
        if not day or direction not in counts:
            continue
        key = str(day)[:10]
        counts[direction][key] = counts[direction].get(key, 0) + 1
        if status in failed_status:
            failed[direction].add(key)
    return counts, failed


def reference_level(
    counts: dict[str, int],
    failed: set[str],
    rule: CoverageRule | None = None,
) -> float:
    healthy = [value for day, value in counts.items() if day not in failed]
    if not healthy:
        return 0.0
    quantile = _rule(rule).reference_quantile
    return float(np.percentile(np.asarray(healthy), quantile))


def covered_counts(
    flight_events: pa.Table,
    rule: CoverageRule | None = None,
) -> dict[str, dict[str, int]]:
    """Counts for the covered day-directions only."""
    rule = _rule(rule)
    counts, failed = observed_counts(flight_events, rule)
    kept: dict[str, dict[str, int]] = {}
    for direction in DIRECTIONS:
        level = reference_level(counts[direction], failed[direction], rule)
        floor = rule.sparse_fraction * level
        kept[direction] = {
            day: value
            for day, value in counts[direction].items()
            if day not in failed[direction] and value >= floor
        }
    return kept


def gated_keys(
    flight_events: pa.Table,
    rule: CoverageRule | None = None,
) -> set[tuple[str, str]]:
    """(day, direction) pairs that have rows and fail the rule."""
    rule = _rule(rule)
    counts, _ = observed_counts(flight_events, rule)
    kept = covered_counts(flight_events, rule)
    gated: set[tuple[str, str]] = set()
    for direction in DIRECTIONS:
        for day in counts[direction]:
            if day not in kept[direction]:
                gated.add((day, direction))
    return gated


def coverage_summary(
    flight_events: pa.Table,
    rule: CoverageRule | None = None,
) -> dict[str, Any]:
    rule = _rule(rule)
    counts, failed = observed_counts(flight_events, rule)
    kept = covered_counts(flight_events, rule)
    return {
        direction: {
            "days_with_rows": len(counts[direction]),
            "covered_days": len(kept[direction]),
            "reference_level": reference_level(
                counts[direction], failed[direction], rule
            ),
        }
        for direction in DIRECTIONS
    }

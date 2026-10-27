"""The shared OpenSky coverage rule and the daily fit that uses it."""

from __future__ import annotations

from datetime import date, timedelta

import pyarrow as pa
import pytest

from flowstate.resample.aviation_coverage import (
    covered_counts,
    gated_keys,
)
from flowstate.resample.clocks import _fit_aviation_daily, aviation_mean_for

START = date(2025, 1, 1)


def _table(
    healthy_days: int,
    thin_days: int,
    *,
    failed_days: int = 0,
    level: int = 100,
) -> pa.Table:
    rows = []
    day = START
    for index in range(healthy_days):
        for _ in range(level + (index % 5) - 2):
            rows.append(_row("arrival", day, "fetched"))
            rows.append(_row("departure", day, "fetched"))
        day += timedelta(days=1)
    for _ in range(thin_days):
        for _ in range(3):
            rows.append(_row("arrival", day, "fetched"))
        for _ in range(level):
            rows.append(_row("departure", day, "fetched"))
        day += timedelta(days=1)
    for _ in range(failed_days):
        for _ in range(level):
            rows.append(_row("arrival", day, "failed"))
            rows.append(_row("departure", day, "fetched"))
        day += timedelta(days=1)
    return pa.Table.from_pylist(rows)


def _row(direction: str, day: date, status: str) -> dict:
    return {
        "direction": direction,
        "local_date": day.isoformat(),
        "query_status": status,
    }


def test_thin_and_failed_days_are_gated_not_counted():
    table = _table(90, 20, failed_days=5)
    kept = covered_counts(table)
    assert len(kept["arrival"]) == 90
    assert len(kept["departure"]) == 115
    gated = gated_keys(table)
    assert {direction for _, direction in gated} == {"arrival"}
    assert len(gated) == 25


def test_the_fit_does_not_learn_a_gap_as_a_low_day():
    table = _table(120, 40)
    start = START
    end = START + timedelta(days=170)
    fits = _fit_aviation_daily(table, start, end)
    span = (end - start).days + 1
    probe = (START + timedelta(days=130)).isoformat()
    mean = aviation_mean_for(fits["arrival"], probe, span, start)
    # The healthy level is about 100 events. Learning the thin days
    # would pull this far below.
    assert mean == pytest.approx(100, rel=0.15)


def test_too_few_covered_days_is_an_error():
    table = _table(20, 40)
    with pytest.raises(ValueError, match="covered days"):
        _fit_aviation_daily(table, START, START + timedelta(days=70))


def test_a_gap_direction_takes_its_share_from_the_model():
    import numpy as np

    import resample_fixtures as fx
    from flowstate.resample import build_bundle
    from flowstate.resample.unfold import ParentTotals, aviation_events

    bundle = build_bundle(fx.fixture_inputs(), fx.fixture_spec(), seed=42)
    table = fx.fixture_inputs().flight_events
    rows = table.to_pylist()
    day = "2025-06-10"
    # Thin out the arrivals on that day only: a gap in the fetch.
    kept = []
    seen = 0
    for row in rows:
        if row["local_date"] == day and row["direction"] == "arrival":
            seen += 1
            if seen > 3:
                continue
        kept.append(row)
    thin = pa.Table.from_pylist(kept)
    stream, _ = aviation_events(
        bundle,
        np.random.default_rng(3),
        thin,
        day_pool=[day],
        budget_rows=40,
        parents=ParentTotals(),
    )
    emitted = [row for batch in stream for row in batch]
    arrivals = [r for r in emitted if r["direction"] == "arrival"]
    departures = [r for r in emitted if r["direction"] == "departure"]
    # Thin counts would give the arrivals 3 of 40. The model gives the
    # two directions similar weight on the fixture city.
    assert len(arrivals) >= 10
    assert len(departures) >= 10
    assert {r["provenance_status"] for r in arrivals} == {"synthetic"}

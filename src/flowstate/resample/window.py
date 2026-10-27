"""One plan for a window of days. Amplify and the producer share it.

ADR-0017. The mode split is the measured natural share of the window
(``natural_shares``), applied to the requested number of events. A day
without a source row does not vanish. Its totals freeze at the latest
source day with the same weekday, and the plan says so. Bus stop calls
and synthetic aviation are stamped on the requested day.

The per-day unfoldings are the ones in ``unfold``. This module only
splits the budget and picks each day's totals.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from flowstate.resample.bundle import ModelBundle
from flowstate.resample.clocks import aviation_mean_for
from flowstate.resample.profiles import to_date
from flowstate.resample.unfold import (
    ParentTotals,
    _aviation_synthetic_rows,
    allocate_days,
    aviation_events,
    bus_events,
    largest_remainder,
    metro_day_events,
    ridehail_day_events,
    rng_for,
)

MODES = ("ride_hail", "metro", "bus", "aviation")
MIN_ROWS = 1000
OUTSIDE_WIDEN = 0.5


def metro_days(inputs: Any, start: date, end: date) -> list[str]:
    days = inputs.metro_daily.column("record_date").to_pylist()
    return [
        str(day)[:10]
        for day in days
        if start <= to_date(str(day)[:10]) <= end
    ]


def metro_total(inputs: Any, day: str) -> int:
    frame = inputs.metro_daily.select(["record_date", "total"]).to_pydict()
    for record_date, total in zip(
        frame["record_date"], frame["total"], strict=True
    ):
        if str(record_date)[:10] == day:
            return int(total or 0)
    return 0


def natural_shares(inputs: Any, bundle: ModelBundle) -> dict[str, float]:
    """Measured natural volumes in the window. Budget priors only."""
    start, end = bundle.spec.window_start, bundle.spec.window_end
    ride_total = 0
    frame = inputs.city_day.select(
        ["date", "booking", "done_ride", "cancel_ride"]
    ).to_pydict()
    for day, booking, done, cancel in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        strict=True,
    ):
        if not start <= to_date(str(day)[:10]) <= end:
            continue
        total = sum(int(value or 0) for value in (booking, done, cancel))
        if total <= 0:
            continue
        ride_total += total
    metro_records = 0
    for day in metro_days(inputs, start, end):
        metro_records += 24 * len(bundle.metro.active(to_date(day)))
    flights = inputs.flight_events.column("local_date").to_pylist()
    aviation_rows = sum(
        1
        for day in flights
        if day and start <= to_date(str(day)[:10]) <= end
    )
    return {
        "ride_hail": float(ride_total),
        "metro": float(metro_records),
        "bus": float(inputs.stop_times.num_rows),
        "aviation": float(aviation_rows),
    }


def mode_budgets(
    inputs: Any, bundle: ModelBundle, rows: int
) -> dict[str, int]:
    """Events per mode: ``rows`` split by the measured natural shares."""
    shares = natural_shares(inputs, bundle)
    return dict(
        zip(
            MODES,
            largest_remainder(rows, [shares[mode] for mode in MODES]),
            strict=True,
        )
    )


def window_days(start: str, end: str) -> list[str]:
    cursor, last = to_date(start), to_date(end)
    if last < cursor:
        raise ValueError("window_end is before window_start")
    days: list[str] = []
    while cursor <= last:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return days


def frozen_source(known: list[str], day: str) -> str | None:
    """The source day a missing day freezes at.

    The latest known day on or before ``day`` with the same weekday.
    A day before every known day takes the earliest same-weekday day.
    """
    weekday = to_date(day).weekday()
    same = [item for item in known if to_date(item).weekday() == weekday]
    before = [item for item in same if item <= day]
    if before:
        return max(before)
    return min(same) if same else None


def freeze_to_window(bundle: ModelBundle, day: str) -> str:
    """The same weekday inside the window, at the terminal level.

    A day after the window takes the latest same-weekday day on or
    before ``window_end``. A day before it takes the earliest one on or
    after ``window_start``. The trend term stops at the window edge.
    """
    moment = to_date(day)
    if moment > bundle.spec.window_end:
        weeks = -(-(moment - bundle.spec.window_end).days // 7)
        return (moment - timedelta(days=7 * weeks)).isoformat()
    if moment < bundle.spec.window_start:
        weeks = -(-(bundle.spec.window_start - moment).days // 7)
        return (moment + timedelta(days=7 * weeks)).isoformat()
    return day


def outside_window(bundle: ModelBundle, day: str) -> bool:
    moment = to_date(day)
    return moment < bundle.spec.window_start or moment > bundle.spec.window_end


def ride_lookup(
    inputs: Any, bundle: ModelBundle
) -> dict[str, tuple[dict[str, int], float | None]]:
    """Observed city-day totals inside the alignment window."""
    frame = inputs.city_day.select(
        [
            "date",
            "booking",
            "done_ride",
            "cancel_ride",
            "earning_per_completed_ride_proxy",
        ]
    ).to_pydict()
    found: dict[str, tuple[dict[str, int], float | None]] = {}
    for day, booking, done, cancel, proxy in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        frame["earning_per_completed_ride_proxy"],
        strict=True,
    ):
        key = str(day)[:10]
        if outside_window(bundle, key):
            continue
        metrics = {
            "booking": int(booking or 0),
            "done_ride": int(done or 0),
            "cancel_ride": int(cancel or 0),
        }
        if sum(metrics.values()) <= 0:
            continue
        found[key] = (metrics, None if proxy is None else float(proxy))
    return found


def metro_lookup(inputs: Any, bundle: ModelBundle) -> dict[str, int]:
    found: dict[str, int] = {}
    for day in metro_days(
        inputs, bundle.spec.window_start, bundle.spec.window_end
    ):
        total = metro_total(inputs, day)
        if total > 0:
            found[day] = total
    return found


def plan_window(
    bundle: ModelBundle,
    inputs: Any,
    *,
    window_start: str,
    window_end: str,
    rows: int,
    seed: int,
    parents: ParentTotals,
    log: Any = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Corpus rows for each day of the window, ``rows`` in total."""
    if rows < MIN_ROWS:
        raise ValueError(
            f"rows={rows} is too small for a multimodal plan "
            f"(minimum {MIN_ROWS})"
        )
    days = window_days(window_start, window_end)
    budgets = mode_budgets(inputs, bundle, rows)
    collected: list[dict[str, Any]] = []
    summary: dict[str, Any] = {
        "budgets": budgets,
        "days": days,
        "frozen": {"ride_hail": {}, "metro": {}},
        "ride_scale": {},
        "metro_scale": {},
    }

    # ---- bus: whole trips, stamped on each replay day ------------
    bus_rows = 0
    if budgets["bus"] > 0:
        shares = largest_remainder(budgets["bus"], [1] * len(days))
        for day, share in zip(days, shares, strict=True):
            if share <= 0:
                continue
            stream, _ = bus_events(
                bundle,
                rng_for(seed, "bus", day),
                inputs.stop_times,
                feed_date=day,
                budget_rows=share,
                parents=parents,
                id_prefix=f"{day}-",
            )
            for batch in stream:
                collected.extend(batch)
                bus_rows += len(batch)

    # ---- aviation: observed where the gate passes, else synthetic -
    aviation_rows = _aviation(
        bundle, inputs, days, budgets["aviation"], seed, parents, collected
    )

    # ---- metro: tracker total, or frozen --------------------------
    metro_rows = _metro(
        bundle, inputs, days, budgets["metro"], seed, parents, collected,
        summary,
    )

    # ---- ride-hail: the exact remainder ---------------------------
    ride_budget = rows - bus_rows - aviation_rows - metro_rows
    if ride_budget <= 0:
        raise ValueError(
            f"the other modes used {rows - ride_budget} of {rows} rows"
        )
    ride_rows = _ride(
        bundle, inputs, days, ride_budget, seed, parents, collected, summary,
        log,
    )
    for row in collected:
        # The date decides the flag. A bus stop call on an in-window
        # day is still a 2026 timetable. The event carries feed_version.
        row["outside_training_window"] = (
            "true" if outside_window(bundle, row["timestamp"][:10]) else "false"
        )
    summary["rows"] = {
        "ride_hail": ride_rows,
        "metro": metro_rows,
        "bus": bus_rows,
        "aviation": aviation_rows,
        "total": ride_rows + metro_rows + bus_rows + aviation_rows,
    }
    return collected, summary


def _aviation(
    bundle: ModelBundle,
    inputs: Any,
    days: list[str],
    budget: int,
    seed: int,
    parents: ParentTotals,
    collected: list[dict[str, Any]],
) -> int:
    if budget <= 0:
        return 0
    flights = {
        str(day)[:10]
        for day in inputs.flight_events.column("local_date").to_pylist()
        if day
    }
    span = bundle.span_days()
    used = 0
    shares = largest_remainder(budget, [1] * len(days))
    for day, share in zip(days, shares, strict=True):
        if share <= 0:
            continue
        if day in flights:
            stream, _ = aviation_events(
                bundle,
                rng_for(seed, "aviation", day),
                inputs.flight_events,
                day_pool=[day],
                budget_rows=share,
                parents=parents,
            )
            for batch in stream:
                collected.extend(batch)
                used += len(batch)
            continue
        means = [
            max(
                aviation_mean_for(
                    bundle.clocks.aviation[direction],
                    freeze_to_window(bundle, day),
                    span,
                    bundle.spec.window_start,
                ),
                1e-6,
            )
            for direction in ("arrival", "departure")
        ]
        counts = largest_remainder(share, means)
        for direction, count in zip(
            ("arrival", "departure"), counts, strict=True
        ):
            if count <= 0:
                continue
            made = _aviation_synthetic_rows(
                bundle,
                rng_for(seed, "aviation", day, direction),
                day=day,
                direction=direction,
                parents=parents,
                span=span,
                count=count,
            )
            collected.extend(made)
            used += len(made)
    return used


def _metro(
    bundle: ModelBundle,
    inputs: Any,
    days: list[str],
    budget: int,
    seed: int,
    parents: ParentTotals,
    collected: list[dict[str, Any]],
    summary: dict[str, Any],
) -> int:
    if budget <= 0:
        return 0
    known = metro_lookup(inputs, bundle)
    records = [
        24 * len(bundle.metro.active(to_date(day))) for day in days
    ]
    if sum(records) <= 0:
        return 0
    shares = largest_remainder(budget, [float(r) for r in records])
    used = 0
    for day, share, record_count in zip(days, shares, records, strict=True):
        if share <= 0 or record_count <= 0:
            continue
        total = known.get(day)
        if total is None:
            source = frozen_source(list(known), day)
            if source is None:
                continue
            total = known[source]
            summary["frozen"]["metro"][day] = source
        people = max(int(round(total * share / record_count)), share)
        batches = list(
            metro_day_events(
                bundle,
                rng_for(seed, "metro", day),
                day,
                people,
                parents,
                partial_records=share,
            )
        )
        emitted = sum(len(batch) for batch in batches)
        for batch in batches:
            collected.extend(batch)
        used += emitted
        summary["metro_scale"][day] = {"real_total": total, "people": people}
    return used


def _ride(
    bundle: ModelBundle,
    inputs: Any,
    days: list[str],
    budget: int,
    seed: int,
    parents: ParentTotals,
    collected: list[dict[str, Any]],
    summary: dict[str, Any],
    log: Any,
) -> int:
    known = ride_lookup(inputs, bundle)
    if not known:
        raise ValueError("no city-day clock inside the alignment window")
    plan: list[tuple[str, dict[str, int], float | None, bool]] = []
    for day in days:
        if day in known:
            metrics, proxy = known[day]
            plan.append((day, metrics, proxy, False))
            continue
        source = frozen_source(list(known), day)
        if source is None:
            continue
        metrics, proxy = known[source]
        summary["frozen"]["ride_hail"][day] = source
        if log is not None:
            log.info(
                "ride-hail %s has no city-day row; totals frozen at %s",
                day,
                source,
            )
        plan.append((day, metrics, proxy, True))
    weights = [float(sum(item[1].values())) for item in plan]
    assigned = allocate_days(budget, [item[0] for item in plan], weights)
    leftover = budget - sum(k for _, k in assigned)
    used = 0
    by_day = {item[0]: item for item in plan}
    for index, (day, share) in enumerate(assigned):
        if leftover and index == len(assigned) - 1:
            share += leftover
            leftover = 0
        _, metrics, proxy, frozen = by_day[day]
        day_total = sum(metrics.values())
        emitted = 0
        for batch in ridehail_day_events(
            bundle,
            rng_for(seed, "ride_hail", day),
            day,
            metrics,
            proxy,
            parents,
            parent_source="sampled" if frozen else "observed",
            partial_rows=share,
            widen=OUTSIDE_WIDEN if outside_window(bundle, day) else 0.0,
        ):
            collected.extend(batch)
            emitted += len(batch)
        used += emitted
        summary["ride_scale"][day] = {
            "real_activity": day_total,
            "rows": emitted,
        }
    return used

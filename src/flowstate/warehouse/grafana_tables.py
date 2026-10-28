"""Tables Grafana may read. The panel names a rollup, not the corpus."""

from __future__ import annotations

import re
from collections import defaultdict

# Hive rollups and one analytics result table per question.
WAREHOUSE = frozenset(
    {
        "rollup_ridehail_day",
        "rollup_ridehail_hour",
        "rollup_metro_station_hour",
        "rollup_metro_day",
        "rollup_bus_hour",
        "rollup_aviation_day",
        "rollup_conservation",
        "analytics_demand_hotspots",
        "analytics_cancellation_analysis",
        "analytics_ridership_trends",
        "analytics_ridehail_metro",
        "analytics_flights_ridehail",
        "analytics_scenario_effect",
    }
)
# Stream-sink external tables. Not Hive rides.
STREAM = frozenset(
    {
        "sink_active_rides",
        "sink_cancel_rate",
        "sink_surge",
    }
)
# ADR-0016 KIA catchment. One series, not one series per ward.
KIA_WARDS = ("b_6", "b_1", "b_2", "b_7", "b_8", "b_5", "b_3")
_RUN = re.compile(r"[0-9a-fA-F-]{36}")
_WARD = re.compile(r"b_[0-9]+")


# `note` contains commas. Beeline CSV does not quote them, so a
# SELECT * shifts every column after it.
_COLUMNS = {
    "analytics_ridehail_metro": (
        "period_start, grain, n, estimate, interval_low, interval_high, "
        "corpus_rows, seed, bundle_hash, window_start, window_end"
    ),
    "analytics_flights_ridehail": (
        "period_start, n, estimate, interval_low, interval_high, "
        "arrival_covered_days, departure_covered_days, "
        "required_covered_days, corpus_rows, seed, bundle_hash, "
        "window_start, window_end"
    ),
}


def warehouse_sql(table: str, run_id: str | None) -> str:
    """One SELECT. The table name is an allowlisted identifier."""
    if table not in WAREHOUSE:
        raise ValueError("table is not a warehouse rollup")
    columns = _COLUMNS.get(table, "*")
    if table.startswith("analytics_"):
        if run_id is None or _RUN.fullmatch(run_id) is None:
            raise ValueError("run_id is not a run")
        return (
            f"SELECT {columns} FROM {table} WHERE run_id = '{run_id}'"
        )
    if run_id:
        raise ValueError("a rollup has no run_id")
    return f"SELECT {columns} FROM {table}"


def ward_list(ward: str) -> tuple[str, ...]:
    """`kia` is the catchment sum. One ward id is that ward only."""
    if ward == "kia":
        return KIA_WARDS
    if _WARD.fullmatch(ward) is None:
        raise ValueError("ward is not a ward id")
    return (ward,)


def _in(wards: tuple[str, ...]) -> str:
    return ", ".join(f"'{item}'" for item in wards)


def _num(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def _hour(value: object) -> str:
    return f"{int(_num(value)):02d}"


def chart_rows(
    table: str, view: str, rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Wide rows for a chart. Provenance values stay in separate series."""
    if view == "identity":
        return _identity(rows)
    if table == "rollup_ridehail_hour" and view == "hour":
        return _labeled(rows, "hour", ("bookings",))
    if table == "rollup_bus_hour" and view == "hour":
        return _labeled(rows, "hour", ("boardings", "alightings"))
    if table == "rollup_bus_hour" and view == "onboard":
        return _labeled(rows, "hour", ("mean_onboard_leaving",))
    if table == "rollup_metro_day" and view == "series":
        return _pivot_time(rows, "service_date", "provenance_status", "volume")
    if table == "rollup_aviation_day" and view == "series":
        return _pivot_time(
            rows,
            "service_date",
            ("direction", "provenance_status"),
            "events",
        )
    if table == "rollup_metro_station_hour" and view == "station":
        return _top_stations(rows)
    if table == "analytics_demand_hotspots" and view == "hour":
        return _demand_hour(rows)
    if table == "analytics_demand_hotspots" and view == "ward":
        return _top_wards(rows)
    if table == "analytics_cancellation_analysis" and view == "hour":
        return _cancel_hour(rows)
    if table == "analytics_ridership_trends" and view == "series":
        return _ridership(rows)
    association = ("analytics_ridehail_metro", "analytics_flights_ridehail")
    if table in association and view == "series":
        return _association(rows)
    if table == "analytics_scenario_effect" and view == "mode":
        return _scenario_mode(rows)
    raise ValueError("view is not a chart")


def _identity(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    if not rows:
        return []
    first = rows[0]
    grades = {
        str(row.get("evidence_grade"))
        for row in rows
        if row.get("evidence_grade")
    }
    counts = [
        _num(row.get("n"))
        for row in rows
        if row.get("n") not in (None, "")
    ]
    return [
        {
            "corpus_rows": _num(first.get("corpus_rows")),
            "seed": _num(first.get("seed")),
            "bundle_hash": first.get("bundle_hash") or "",
            "window_start": first.get("window_start") or "",
            "window_end": first.get("window_end") or "",
            "evidence_grade": ", ".join(sorted(grades)),
            "n": max(counts) if counts else None,
        }
    ]


def _labeled(
    rows: list[dict[str, object]], key: str, measures: tuple[str, ...]
) -> list[dict[str, object]]:
    out = []
    for row in rows:
        item: dict[str, object] = {"hour": _hour(row.get(key))}
        for name in measures:
            item[name] = _num(row.get(name))
        out.append(item)
    return sorted(out, key=lambda item: str(item["hour"]))


def _pivot_time(
    rows: list[dict[str, object]],
    time_key: str,
    label_keys: str | tuple[str, ...],
    value_key: str,
) -> list[dict[str, object]]:
    keys = (label_keys,) if isinstance(label_keys, str) else label_keys
    bucket: dict[str, dict[str, object]] = {}
    for row in rows:
        stamp = str(row.get(time_key) or "")
        label = " ".join(str(row.get(key) or "") for key in keys).strip()
        slot = bucket.setdefault(stamp, {"time": stamp})
        slot[label] = _num(slot.get(label)) + _num(row.get(value_key))
    return [bucket[stamp] for stamp in sorted(bucket)]


def _top_stations(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    totals: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"entries_count": 0.0, "exits_count": 0.0}
    )
    for row in rows:
        station = str(row.get("station_id") or "")
        status = str(row.get("provenance_status") or "")
        key = (station, status)
        totals[key]["entries_count"] += _num(row.get("entries_count"))
        totals[key]["exits_count"] += _num(row.get("exits_count"))
    ranked = sorted(
        totals,
        key=lambda item: (
            totals[item]["entries_count"] + totals[item]["exits_count"]
        ),
        reverse=True,
    )[:12]
    provenances = {key[1] for key in ranked}
    out = []
    for key in ranked:
        label = key[0] if len(provenances) == 1 else f"{key[0]} ({key[1]})"
        out.append(
            {
                "station": label,
                "entries_count": totals[key]["entries_count"],
                "exits_count": totals[key]["exits_count"],
            }
        )
    return out


def _demand_hour(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    bucket: dict[str, dict[str, float]] = defaultdict(
        lambda: {"weekday": 0.0, "weekend": 0.0}
    )
    for row in rows:
        kind = str(row.get("day_type") or "")
        if kind not in ("weekday", "weekend"):
            continue
        hour = _hour(row.get("hour_of_day"))
        bucket[hour][kind] += _num(row.get("demand_count"))
    return [
        {"hour": hour, "weekday": slot["weekday"], "weekend": slot["weekend"]}
        for hour, slot in sorted(bucket.items())
    ]


def _top_wards(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        totals[str(row.get("ward_id") or "")] += _num(row.get("demand_count"))
    ranked = sorted(totals, key=lambda key: totals[key], reverse=True)[:12]
    return [{"ward_id": ward, "demand": totals[ward]} for ward in ranked]


def _cancel_hour(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    bucket: dict[str, dict[str, dict[str, float]]] = defaultdict(
        lambda: {
            "weekday": {"cancels": 0.0, "bookings": 0.0},
            "weekend": {"cancels": 0.0, "bookings": 0.0},
        }
    )
    for row in rows:
        kind = str(row.get("day_type") or "")
        if kind not in ("weekday", "weekend"):
            continue
        slot = bucket[_hour(row.get("hour_of_day"))][kind]
        slot["cancels"] += _num(row.get("cancels"))
        slot["bookings"] += _num(row.get("bookings"))
    out = []
    for hour, kinds in sorted(bucket.items()):
        item: dict[str, object] = {"hour": hour}
        for kind, slot in kinds.items():
            bookings = slot["bookings"]
            item[kind] = None if bookings == 0 else slot["cancels"] / bookings
        out.append(item)
    return out


def _ridership(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    daily = [row for row in rows if row.get("grain") == "day"]
    pivoted = _pivot_time(daily, "period_start", "mode", "volume")
    for row in pivoted:
        row.setdefault("metro", None)
        row.setdefault("aviation", None)
    return pivoted


def _association(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    chosen = [row for row in rows if row.get("grain") in (None, "", "day")]
    out = []
    for row in chosen:
        out.append(
            {
                "time": row.get("period_start") or "",
                "estimate": _num(row.get("estimate")),
                "interval_low": _num(row.get("interval_low")),
                "interval_high": _num(row.get("interval_high")),
                "n": _num(row.get("n")),
            }
        )
    return sorted(out, key=lambda item: str(item["time"]))


def _scenario_mode(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    bucket: dict[str, dict[str, float]] = defaultdict(
        lambda: {"baseline": 0.0, "fork": 0.0}
    )
    for row in rows:
        slot = bucket[str(row.get("mode") or "")]
        slot["baseline"] += _num(row.get("baseline_value"))
        slot["fork"] += _num(row.get("fork_value"))
    return [
        {
            "mode": mode,
            "baseline": slot["baseline"],
            "fork": slot["fork"],
            "difference": slot["fork"] - slot["baseline"],
        }
        for mode, slot in sorted(bucket.items())
    ]


def stream_sql(table: str, ward: str) -> str:
    """City total plus one ward series. Hour buckets are Asia/Kolkata."""
    if table not in STREAM:
        raise ValueError("table is not a stream sink")
    chosen = _in(ward_list(ward))
    hour = (
        "date_format(from_utc_timestamp({col}, 'Asia/Kolkata'), "
        "'yyyy-MM-dd HH:00:00')"
    )
    if table == "sink_active_rides":
        stamp = hour.format(col="window_start")
        return (
            f"SELECT {stamp} AS hour_start, "
            "sum(activity_count) AS city, "
            f"sum(CASE WHEN ward_id IN ({chosen}) "
            "THEN activity_count ELSE 0 END) AS ward "
            f"FROM {table} GROUP BY {stamp} ORDER BY hour_start"
        )
    if table == "sink_cancel_rate":
        stamp = hour.format(col="window_start")
        return (
            "SELECT hour_start, "
            "city_n AS city_events, ward_n AS ward_events, "
            "CASE WHEN city_n >= 5 THEN city_cancels / city_n END AS city, "
            "CASE WHEN ward_n >= 5 THEN ward_cancels / ward_n END AS ward "
            "FROM ("
            f"SELECT {stamp} AS hour_start, "
            "sum(cancels) AS city_cancels, "
            "sum(activity_count) AS city_n, "
            f"sum(CASE WHEN ward_id IN ({chosen}) "
            "THEN cancels ELSE 0 END) AS ward_cancels, "
            f"sum(CASE WHEN ward_id IN ({chosen}) "
            "THEN activity_count ELSE 0 END) AS ward_n "
            f"FROM {table} GROUP BY {stamp}"
            ") t ORDER BY hour_start"
        )
    stamp = hour.format(col="hour_start")
    return (
        f"SELECT {stamp} AS hour_start, "
        "avg(z_score) AS city, "
        f"avg(CASE WHEN ward_id IN ({chosen}) THEN z_score END) AS ward "
        f"FROM {table} GROUP BY {stamp} ORDER BY hour_start"
    )

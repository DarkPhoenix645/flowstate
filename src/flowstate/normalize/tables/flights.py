"""OpenSky arrival and departure events already on disk.

UTC fields stay. Local date and hour use the city timezone. An empty
window file is 404-empty, which is how the fetcher stores HTTP 404. No
window is fetched here. A day with no event is not written as zero flights.
"""

from __future__ import annotations

import bisect
import csv
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

from flowstate.ingest.opensky_flights import windows
from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile
from flowstate.normalize.clock import (
    future_dates,
    iso_utc,
    local_bin,
    unix_to_utc,
)
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.sources import ensure_sources

COLUMNS = [
    "direction",
    "icao24",
    "callsign",
    "first_seen_unix",
    "first_seen_utc",
    "last_seen_unix",
    "last_seen_utc",
    "event_time_unix",
    "event_time_utc",
    "est_departure_airport",
    "est_arrival_airport",
    "local_date",
    "local_hour",
    "query_window_start_utc",
    "query_window_end_utc",
    "query_status",
    "provenance_status",
]


def build_flight_events(
    arrivals: list[dict[str, str]],
    departures: list[dict[str, str]],
    arrival_windows: list[dict[str, Any]],
    departure_windows: list[dict[str, Any]],
    *,
    zone: ZoneInfo,
) -> TableBuild:
    rows = _events("arrival", arrivals, arrival_windows, zone)
    rows.extend(_events("departure", departures, departure_windows, zone))
    rows.sort(
        key=lambda item: (
            item["direction"],
            item["event_time_unix"]
            if item["event_time_unix"] is not None
            else -1,
            item["icao24"] or "",
        )
    )
    key_counts: dict[tuple[str, str, int | None], int] = {}
    for row in rows:
        key = (row["direction"], row["icao24"], row["first_seen_unix"])
        key_counts[key] = key_counts.get(key, 0) + 1
    duplicates = [
        {"direction": key[0], "icao24": key[1], "first_seen_unix": key[2]}
        for key, count in key_counts.items()
        if count > 1
    ]
    arrival_dates = {
        row["local_date"]
        for row in rows
        if row["direction"] == "arrival" and row["local_date"]
    }
    departure_dates = {
        row["local_date"]
        for row in rows
        if row["direction"] == "departure" and row["local_date"]
    }
    dates = sorted(arrival_dates | departure_dates)
    status_counts = _status_counts(arrival_windows + departure_windows)
    unassigned = sum(1 for row in rows if row["query_status"] == "unassigned")
    null_airports = sum(
        1
        for row in rows
        if row["est_departure_airport"] is None
        or row["est_arrival_airport"] is None
    )
    table = pa.Table.from_pylist(rows, schema=_schema())
    coverage = {
        "min": dates[0] if dates else None,
        "max": dates[-1] if dates else None,
        "counts": {
            "rows": len(rows),
            "arrivals": sum(1 for row in rows if row["direction"] == "arrival"),
            "departures": sum(
                1 for row in rows if row["direction"] == "departure"
            ),
            "arrival_local_days": len(arrival_dates),
            "departure_local_days": len(departure_dates),
            "timezone": str(zone),
            "null_airport_rows": null_airports,
            "unassigned_events": unassigned,
            "query_status": status_counts,
        },
        "missing": [],
        "missing_cells": 0,
        "duplicate_count": len(duplicates),
        "duplicates": duplicates[:20],
        "future_dates": future_dates(
            [row["local_date"] for row in rows if row["local_date"]]
        ),
        "windows": [
            {
                "direction": item["direction"],
                "window_start_utc": item["window_start_utc"],
                "window_end_utc": item["window_end_utc"],
                "query_status": item["query_status"],
            }
            for item in arrival_windows + departure_windows
        ],
        "veracity": [
            "event_time is lastSeen on arrival and firstSeen on departure.",
            "Both are estimated. Neither is a verified touchdown.",
            "provenance_status is observed_proxy.",
            "An empty window file is 404-empty.",
            "The fetcher does not store the HTTP code separately.",
            "A 200 with an empty list would look the same.",
            "No zero-flight day is written.",
            "This command does not fetch.",
        ],
    }
    return TableBuild(
        table=table,
        coverage=coverage,
        dates=set(dates),
        extra={
            "arrival_dates": arrival_dates,
            "departure_dates": departure_dates,
        },
    )


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    ensure_sources(
        [
            profile.opensky_dataset_arrivals,
            profile.opensky_dataset_departures,
        ],
        datasets,
    )
    root = dataset_path(datasets, profile.opensky_dir)
    extracts = [item for item in profile.airports if item.opensky_extract]
    if not extracts:
        raise ValueError(f"{profile.id} has no OpenSky extract airport")
    arrivals: list[dict[str, str]] = []
    departures: list[dict[str, str]] = []
    arrival_windows: list[dict[str, Any]] = []
    departure_windows: list[dict[str, Any]] = []
    for airport in extracts:
        arrivals.extend(
            _read_csv(root / _extract_name(profile, airport.code, "arrival"))
        )
        departures.extend(
            _read_csv(
                root / _extract_name(profile, airport.code, "departure")
            )
        )
        arrival_windows.extend(
            window_records(root, "arrival", airport.code)
        )
        departure_windows.extend(
            window_records(root, "departure", airport.code)
        )
    return build_flight_events(
        arrivals,
        departures,
        arrival_windows,
        departure_windows,
        zone=profile.timezone,
    )


def window_records(
    root: Path, direction: str, airport: str
) -> list[dict[str, Any]]:
    status_path = root / f"status_{direction}.json"
    exhausted = False
    if status_path.is_file():
        exhausted = bool(
            json.loads(status_path.read_text()).get("credits_exhausted")
        )
    records = []
    for start, end in windows():
        name = f"{airport}_{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}Z.json"
        path = root / "windows" / direction / name
        records.append(
            {
                "direction": direction,
                "window_start_utc": start.isoformat(),
                "window_end_utc": end.isoformat(),
                "start_unix": int(start.timestamp()),
                "end_unix": int(end.timestamp()),
                "query_status": _window_status(path, exhausted),
            }
        )
    return records


def _window_status(path: Path, exhausted: bool) -> str:
    if not path.is_file() or path.stat().st_size == 0:
        return "quota" if exhausted else "failed"
    if path.stat().st_size <= 2:
        if path.read_text().strip() == "[]":
            return "404-empty"
        return "failed"
    with path.open() as handle:
        start = handle.read(1)
    if start == "[":
        return "fetched"
    return "failed"


def _extract_name(
    profile: CityProfile, code: str, direction: str
) -> str:
    label = "arrivals" if direction == "arrival" else "departures"
    return (
        f"{code}_{label}_"
        f"{profile.window_start.isoformat()}_"
        f"{profile.window_end.isoformat()}.csv"
    )


def _events(
    direction: str,
    raw_rows: list[dict[str, str]],
    spans: list[dict[str, Any]],
    zone: ZoneInfo,
) -> list[dict[str, Any]]:
    starts = [item["start_unix"] for item in spans]
    built = []
    for raw in raw_rows:
        if direction == "arrival":
            first_seen = _unix(raw.get("first_seen_utc"))
            last_seen = _unix(raw.get("arrival_time_proxy_utc"))
            event_time = last_seen
        else:
            first_seen = _unix(raw.get("departure_time_proxy_utc"))
            last_seen = _unix(raw.get("last_seen_utc"))
            event_time = first_seen
        local_date = None
        local_hour = None
        if event_time is not None:
            local_date, local_hour = local_bin(unix_to_utc(event_time), zone)
        window = (
            _find_window(event_time, spans, starts)
            if event_time is not None
            else None
        )
        built.append(
            {
                "direction": direction,
                "icao24": raw.get("icao24") or None,
                "callsign": _text(raw.get("callsign")),
                "first_seen_unix": first_seen,
                "first_seen_utc": _iso(first_seen),
                "last_seen_unix": last_seen,
                "last_seen_utc": _iso(last_seen),
                "event_time_unix": event_time,
                "event_time_utc": _iso(event_time),
                "est_departure_airport": _text(
                    raw.get("est_departure_airport")
                ),
                "est_arrival_airport": _text(raw.get("est_arrival_airport")),
                "local_date": local_date,
                "local_hour": local_hour,
                "query_window_start_utc": (
                    window["window_start_utc"] if window else None
                ),
                "query_window_end_utc": (
                    window["window_end_utc"] if window else None
                ),
                "query_status": (
                    window["query_status"] if window else "unassigned"
                ),
                "provenance_status": "observed_proxy",
            }
        )
    return built


def _find_window(
    ts: int, spans: list[dict[str, Any]], starts: list[int]
) -> dict[str, Any] | None:
    index = bisect.bisect_right(starts, ts) - 1
    if index < 0:
        return None
    window = spans[index]
    if window["start_unix"] <= ts <= window["end_unix"]:
        return window
    return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _iso(seconds: int | None) -> str | None:
    if seconds is None:
        return None
    return iso_utc(seconds)


def _unix(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _status_counts(spans: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"fetched": 0, "404-empty": 0, "failed": 0, "quota": 0}
    for item in spans:
        status = item["query_status"]
        counts[status] = counts.get(status, 0) + 1
    return counts


def _schema() -> pa.Schema:
    ints = {
        "first_seen_unix",
        "last_seen_unix",
        "event_time_unix",
        "local_hour",
    }
    return pa.schema(
        [
            pa.field(name, pa.int64() if name in ints else pa.string())
            for name in COLUMNS
        ]
    )

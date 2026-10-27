"""BMTC GTFS validation and staged schedule tables.

Boardings are not invented. That prior is #40.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from flowstate.config import Settings
from flowstate.provenance import require_valid
from flowstate.staged import staged_parquet_path, write_parquet

MAX_TRIP_SECONDS = 24 * 3600
FEED_DATE = "2026-09-07"


def gtfs_seconds(value: object) -> int | None:
    if not isinstance(value, str) or value.count(":") != 2:
        return None
    hour, minute, second = (part.strip() for part in value.split(":"))
    if not (hour.isdigit() and minute.isdigit() and second.isdigit()):
        return None
    return int(hour) * 3600 + int(minute) * 60 + int(second)


def validate_frames(
    agency: pd.DataFrame,
    calendar: pd.DataFrame,
    trips: pd.DataFrame,
    stops: pd.DataFrame,
    stop_times: pd.DataFrame,
) -> dict[str, Any]:
    """Reject trips that fail the schedule checks. One reason per trip."""
    timezone = str(agency["agency_timezone"].iloc[0])
    services = set(calendar["service_id"].astype(str))
    stop_ids = set(stops["stop_id"].astype(str))
    trips = trips.copy()
    trips["trip_id"] = trips["trip_id"].astype(str)
    trips["service_id"] = trips["service_id"].astype(str)

    times = stop_times.copy()
    times["trip_id"] = times["trip_id"].astype(str)
    times["stop_id"] = times["stop_id"].astype(str)
    times["stop_sequence"] = times["stop_sequence"].astype(int)
    times["arrival_s"] = times["arrival_time"].map(gtfs_seconds)
    times["departure_s"] = times["departure_time"].map(gtfs_seconds)
    times = times.sort_values(["trip_id", "stop_sequence"])
    grouped = times.groupby("trip_id", sort=False)
    times["prev_seq"] = grouped["stop_sequence"].shift()
    times["prev_dep"] = grouped["departure_s"].shift()

    reasons: dict[str, str] = {}
    if timezone != "Asia/Kolkata":
        for trip_id in trips["trip_id"]:
            reasons[trip_id] = "bad_timezone"

    known_trips = set(trips["trip_id"])
    with_times = set(times["trip_id"])
    for trip_id in known_trips - with_times:
        reasons.setdefault(trip_id, "no_stop_times")
    for trip_id in trips.loc[
        ~trips["service_id"].isin(services), "trip_id"
    ]:
        reasons.setdefault(trip_id, "unknown_service")

    bad_stop = times.loc[~times["stop_id"].isin(stop_ids), "trip_id"]
    bad_clock = times.loc[
        times["arrival_s"].isna() | times["departure_s"].isna(), "trip_id"
    ]
    bad_dwell = times.loc[
        times["departure_s"] < times["arrival_s"], "trip_id"
    ]
    bad_seq = times.loc[
        times["prev_seq"].notna()
        & (times["stop_sequence"] <= times["prev_seq"]),
        "trip_id",
    ]
    bad_back = times.loc[
        times["prev_dep"].notna() & (times["arrival_s"] < times["prev_dep"]),
        "trip_id",
    ]
    for trip_id in bad_stop:
        reasons.setdefault(trip_id, "unknown_stop")
    for trip_id in bad_clock:
        reasons.setdefault(trip_id, "bad_time")
    for trip_id in bad_seq:
        reasons.setdefault(trip_id, "sequence")
    for trip_id in pd.concat([bad_dwell, bad_back]).dropna():
        reasons.setdefault(str(trip_id), "time_goes_backwards")

    span = times.groupby("trip_id").agg(
        start=("arrival_s", "min"), end=("departure_s", "max")
    )
    too_long = span[(span["end"] - span["start"]) > MAX_TRIP_SECONDS]
    for trip_id in too_long.index:
        reasons.setdefault(str(trip_id), "duration")

    rejected_ids = set(reasons)
    valid_trips = trips[~trips["trip_id"].isin(rejected_ids)].copy()
    valid_ids = set(valid_trips["trip_id"])
    valid_times = times[times["trip_id"].isin(valid_ids)].copy()
    rejections = pd.DataFrame(
        [
            {"trip_id": trip_id, "reason": reason}
            for trip_id, reason in sorted(reasons.items())
        ]
    )
    if rejections.empty:
        rejections = pd.DataFrame(columns=["trip_id", "reason"])
    counts: dict[str, int] = {}
    for reason in rejections["reason"]:
        counts[reason] = counts.get(reason, 0) + 1
    return {
        "timezone": timezone,
        "trips": valid_trips,
        "stop_times": valid_times.drop(
            columns=["arrival_s", "departure_s", "prev_seq", "prev_dep"]
        ),
        "rejections": rejections,
        "summary": {
            "feed_timezone": timezone,
            "trips": int(len(trips)),
            "valid_trips": int(len(valid_trips)),
            "rejected_trips": int(len(rejections)),
            "reasons": counts,
            "veracity": [
                "The stop call is derived from the GTFS feed.",
                "Boardings are not in this table. #40 fills them.",
                "A trip longer than 24 hours is rejected.",
                "The upstream feed says some timings and routes may be wrong.",
            ],
        },
    }


def stage_bus(settings: Settings) -> dict[str, Any]:
    require_valid(["bmtc_gtfs"], settings.datasets_dir)
    feed = settings.datasets_dir / "bmtc"
    agency = _csv(feed / "agency.txt")
    calendar = _csv(feed / "calendar.txt")
    routes = _csv(feed / "routes.txt")
    trips = _csv(feed / "trips.txt")
    stops = _csv(feed / "stops.txt")
    stop_times = _csv(
        feed / "stop_times.txt",
        usecols=[
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
        ],
    )
    result = validate_frames(agency, calendar, trips, stops, stop_times)
    _write(
        settings,
        "stops",
        stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]],
    )
    _write(settings, "routes", routes)
    _write(settings, "trips", result["trips"])
    _write(settings, "stop_times", result["stop_times"])
    _write(settings, "rejections", result["rejections"])
    summary = result["summary"]
    path = staged_parquet_path(settings, "bmtc", "trips")
    path.with_suffix(".coverage.json").write_text(
        json.dumps(summary, indent=2) + "\n"
    )
    _write_sample_event(settings, result, summary)
    summary["rows"] = summary["valid_trips"]
    return summary


def _write_sample_event(
    settings: Settings,
    result: dict[str, Any],
    summary: dict[str, Any],
) -> None:
    from flowstate.stage.events import bus_scheduled_stop_event

    times = result["stop_times"]
    if times.empty:
        return
    row = times.iloc[0]
    event = bus_scheduled_stop_event(
        {
            "trip_id": row["trip_id"],
            "stop_id": row["stop_id"],
            "stop_sequence": int(row["stop_sequence"]),
            "arrival_time": row["arrival_time"],
            "service_date": FEED_DATE,
            "feed_version": "20260907",
        }
    )
    dest = staged_parquet_path(settings, "bmtc", "trips")
    dest.with_name("scheduled_stop_event.json").write_text(
        json.dumps(event, indent=2) + "\n"
    )
    summary["sample_event"] = event["event_id"]


def _csv(path: Path, usecols: list[str] | None = None) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, usecols=usecols, keep_default_na=False)


def _write(settings: Settings, name: str, frame: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(frame, preserve_index=False)
    write_parquet(staged_parquet_path(settings, "bmtc", name), table)

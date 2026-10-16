"""G1 metro identity, G2 archive arithmetic, G3 shared-time coverage.

Overlaps are the intersection of dates that are actually present.
A nominal window length is not a coverage claim.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityHourCheck, CityProfile
from flowstate.normalize.clock import dates_covering, in_window
from flowstate.normalize.coverage import TableBuild
from flowstate.normalize.tables.metro_daily import unresolved_future


def evaluate(
    builds: dict[str, TableBuild],
    datasets: Path,
    profile: CityProfile,
) -> dict[str, Any]:
    overlaps = _overlaps(builds, profile)
    for name, built in builds.items():
        built.coverage["pairwise_overlaps"] = overlaps["by_table"][name]
    return {
        "G1": _g1(builds["metro_daily"], overlaps, profile),
        "G2": _g2(datasets, profile),
        "G3": _g3(builds, overlaps, profile),
    }


def _g1(
    metro: TableBuild, overlaps: dict[str, Any], profile: CityProfile
) -> dict[str, Any]:
    unresolved = unresolved_future(metro.dates, profile.metro_last_valid)
    identity = metro.coverage["identity"]["holds"]
    duplicates = metro.coverage["duplicate_count"]
    passed = identity and not unresolved and duplicates == 0
    shared = overlaps["city_day_and_metro_in_alignment"]
    return {
        "pass": passed,
        "identity_holds": identity,
        "unresolved_future_dates": unresolved,
        "duplicate_count": duplicates,
        "excluded_after_cutoff": metro.coverage["excluded_after_cutoff"],
        "missing_dates_in_span": metro.coverage["counts"][
            "missing_dates_in_span"
        ],
        "alignment_rows": metro.coverage["counts"]["alignment_rows"],
        "shared_dates_in_alignment": shared,
        "note": (
            "The shared-date count is the intersection of kept dates "
            "inside the alignment window."
        ),
    }


def _g2(datasets: Path, profile: CityProfile) -> dict[str, Any]:
    root = dataset_path(datasets, profile.stitched_parts)
    check = profile.city_hour
    city = _archive(root, check.stem)
    ward = _archive(root, profile.ward_hour_stem)
    city_ok = _city_hour_ok(city, check)
    ward_span_ok = (
        ward["distinct_commit_dates"] + ward["missing_commit_dates"]
        == ward["calendar_days_in_commit_span"]
    )
    ward_rows_ok = ward["csv_rows"] == ward["rows_after_dedupe"]
    return {
        "pass": bool(city_ok and ward_span_ok and ward_rows_ok),
        "city": city,
        "ward": ward,
        "accounted": (
            f"{check.n_commits}/{check.distinct_commit_dates}/"
            f"{check.calendar_days_in_commit_span}/"
            f"{check.missing_commit_dates}/"
            f"{check.duplicate_commit_dates}"
        ),
        "note": (
            "A missing commit date is a missing snapshot, not zero ridership."
        ),
    }


def _g3(
    builds: dict[str, TableBuild],
    overlaps: dict[str, Any],
    profile: CityProfile,
) -> dict[str, Any]:
    expected = dates_covering(profile.service_spans)
    actual = builds["metro_station_hour_long"].dates
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    return {
        "pass": actual == expected,
        "rti_days": len(actual),
        "rti_missing": missing,
        "rti_extra": extra,
        "overlaps": {
            key: overlaps[key]
            for key in overlaps
            if key != "by_table"
        },
        "note": (
            "Intersections use dates present in the tables. "
            "RTI coverage is the city profile's service spans, "
            "not a longer window. "
            "A day with no flight event is not a zero-flight day."
        ),
    }


def _city_hour_ok(city: dict[str, Any], check: CityHourCheck) -> bool:
    return (
        city["n_commits"] == check.n_commits
        and city["distinct_commit_dates"] == check.distinct_commit_dates
        and city["calendar_days_in_commit_span"]
        == check.calendar_days_in_commit_span
        and city["missing_commit_dates"] == check.missing_commit_dates
        and city["duplicate_commit_dates"]
        == check.duplicate_commit_dates
        and city["empty_snapshots"] == check.empty_snapshots
        and city["rows_after_dedupe"] == check.rows_after_dedupe
        and city["csv_rows"] == city["rows_after_dedupe"]
        and city["repeated_keys"] == check.repeated_keys
        and city["changed_keys"] == check.changed_keys
    )


def _overlaps(
    builds: dict[str, TableBuild], profile: CityProfile
) -> dict[str, Any]:
    city = builds["namma_yatri_city_day"].dates
    metro = builds["metro_daily"].dates
    rti = builds["metro_station_hour_long"].dates
    flights = builds["flight_events"]
    arrival = set(flights.extra["arrival_dates"])
    departure = set(flights.extra["departure_dates"])
    dates = set(builds["significant_dates_classified"].dates)
    alignment = dates_covering(
        ((profile.window_start, profile.window_end),)
    )
    city_in = {
        day
        for day in city
        if in_window(day, profile.window_start, profile.window_end)
    }
    metro_in = {
        day
        for day in metro
        if in_window(day, profile.window_start, profile.window_end)
    }
    shared = city_in & metro_in
    payload = {
        "alignment_calendar_days": len(alignment),
        "city_day_in_alignment": len(city_in),
        "metro_in_alignment": len(metro_in),
        "city_day_and_metro_in_alignment": len(shared),
        "rti_days": len(rti),
        "rti_and_city_day": len(rti & city),
        "rti_and_metro": len(rti & metro),
        "arrival_local_days": len(arrival),
        "departure_local_days": len(departure),
        "arrival_in_alignment": len(arrival & alignment),
        "departure_in_alignment": len(departure & alignment),
        "arrival_and_rti": len(arrival & rti),
        "departure_and_rti": len(departure & rti),
        "alignment_days_without_arrival_event": len(alignment - arrival),
        "alignment_days_without_departure_event": len(
            alignment - departure
        ),
        "significant_dates_in_alignment": len(dates & alignment),
    }
    by_table = {
        "namma_yatri_city_day": _slice(
            payload,
            "city_day_in_alignment",
            "city_day_and_metro_in_alignment",
            "rti_and_city_day",
        ),
        "metro_daily": _slice(
            payload,
            "metro_in_alignment",
            "city_day_and_metro_in_alignment",
            "rti_and_metro",
        ),
        "metro_station_hour_long": _slice(
            payload,
            "rti_days",
            "rti_and_city_day",
            "rti_and_metro",
            "arrival_and_rti",
            "departure_and_rti",
        ),
        "station_geography": {
            "grain": "station, not day",
            "line_open": profile.line_open.isoformat(),
            "line_name": profile.line_name,
        },
        "airport_geography": {
            "grain": "airport, not day",
            "airports": builds["airport_geography"].coverage["rows"],
        },
        "ward_geography": {
            "grain": "ward, not day",
            "matched": builds["ward_geography"].coverage["matched"],
            "out_of_scope": builds["ward_geography"].coverage[
                "out_of_scope"
            ],
        },
        "od_profile": {
            "days": len(builds["od_profile"].dates),
            "days_and_rti": len(builds["od_profile"].dates & rti),
        },
        "flight_events": _slice(
            payload,
            "arrival_local_days",
            "departure_local_days",
            "arrival_in_alignment",
            "departure_in_alignment",
            "arrival_and_rti",
            "departure_and_rti",
            "alignment_days_without_arrival_event",
            "alignment_days_without_departure_event",
        ),
        "significant_dates_classified": _slice(
            payload, "significant_dates_in_alignment"
        ),
    }
    payload["by_table"] = by_table
    return payload


def _slice(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
    return {key: payload[key] for key in keys}


def _archive(root: Path, stem: str) -> dict[str, Any]:
    coverage = json.loads((root / f"{stem}.coverage.json").read_text())
    overwrites = json.loads((root / f"{stem}.overwrites.json").read_text())
    csv_rows = _csv_rows(root / f"{stem}.csv")
    commits = int(coverage["n_commits"])
    distinct = int(coverage["distinct_commit_dates"])
    return {
        "n_commits": commits,
        "distinct_commit_dates": distinct,
        "calendar_days_in_commit_span": int(
            coverage["calendar_days_in_commit_span"]
        ),
        "missing_commit_dates": len(coverage["commit_dates_missing"]),
        "duplicate_commit_dates": commits - distinct,
        "empty_snapshots": int(coverage["empty_snapshots"]),
        "rows_after_dedupe": int(coverage["rows_after_dedupe"]),
        "csv_rows": csv_rows,
        "repeated_keys": int(overwrites["repeated_keys"]),
        "changed_keys": int(overwrites["changed_keys"]),
    }


def _csv_rows(path: Path) -> int:
    with path.open("rb") as handle:
        return max(sum(1 for _ in handle) - 1, 0)

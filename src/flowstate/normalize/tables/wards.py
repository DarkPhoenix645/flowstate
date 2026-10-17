"""Ward pins for the map.

Ring axis order comes from the city profile. Bengaluru's file stores
[latitude, longitude]. A centroid that treats that pair as longitude-first
is not in the city.

Only ids with the profile's in-scope prefix join the polygon number field.
Other prefixes are out of scope and get no pin. The resampler copies
`latitude` and `longitude` onto each ride-hail event. It does not geocode
the ward id.
"""

from __future__ import annotations

import csv
import json
import logging
import time
import urllib.request
from pathlib import Path
from typing import Any

import pyarrow as pa

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import CityProfile, WardJoin
from flowstate.normalize.coverage import TableBuild, sha256_file
from flowstate.normalize.sources import ensure_sources

log = logging.getLogger("normalize.wards")

COLUMNS = [
    "ward_num",
    "city",
    "official_ward_no",
    "ward_name",
    "latitude",
    "longitude",
    "pin_method",
    "source_url",
    "source_date",
    "match_status",
]

def ward_ids(csv_path: Path) -> list[str]:
    found: set[str] = set()
    with csv_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            found.add(row["ward_num"])
    return sorted(found, key=_sort_key)


def build_rows(
    ward_nums: list[str],
    collection: dict[str, Any],
    join: WardJoin,
) -> list[dict[str, Any]]:
    by_no = _index(collection, join)
    rows = []
    for ward_num in ward_nums:
        rows.append(_row(ward_num, by_no, join))
    return rows


def contains(latitude: float, longitude: float, rings: list[list]) -> bool:
    """True when the point is in the exterior and in no hole."""
    if not _in_ring(latitude, longitude, rings[0]):
        return False
    return all(not _in_ring(latitude, longitude, hole) for hole in rings[1:])


def from_disk(datasets: Path, profile: CityProfile) -> TableBuild:
    join = profile.ward
    ensure_sources([join.dataset_id])
    path = _ensure_geojson(datasets, join)
    digest = sha256_file(path)
    collection = json.loads(path.read_text())
    csv_path = dataset_path(datasets, join.feed_parts)
    rows = build_rows(ward_ids(csv_path), collection, join)
    matched = [row for row in rows if row["match_status"] == "matched"]
    table = pa.Table.from_pylist(rows, schema=_schema())
    coverage = {
        "rows": table.num_rows,
        "matched": len(matched),
        "unmatched": sum(
            row["match_status"] == "unmatched" for row in rows
        ),
        "out_of_scope": sum(
            row["match_status"] == "out_of_scope" for row in rows
        ),
        "interior_pins": sum(
            row["pin_method"] == "interior" for row in rows
        ),
        "coordinate_order": join.axis,
        "scheme_year": join.scheme_year,
        "source_url": join.source_url,
        "catalogue_url": join.catalogue_url,
        "licence": join.licence,
        "geojson_sha256": digest,
        "holes": sum(
            1
            for feature in collection["features"]
            if len(feature["geometry"]["coordinates"]) > 1
        ),
        "veracity": (
            f"{join.in_scope_prefix}_N joins {join.number_field} "
            f"on the {join.scheme_year} polygon file. "
            f"Axis order is {join.axis}. "
            "The pin is the centroid when that point is inside the "
            "polygon, otherwise a point on the longest interior chord. "
            "Other prefixes have no pin. Do not geocode a ward id."
        ),
    }
    return TableBuild(table, coverage)


def _ensure_geojson(datasets: Path, join: WardJoin) -> Path:
    """Return the polygon file under the datasets dir.

    Download it when the file is missing. A hash mismatch is an error.
    """
    path = dataset_path(datasets, join.geojson_parts)
    if path.is_file() and sha256_file(path) == join.geojson_sha256:
        log.info("cache hit %s", path)
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    # Cloudflare rejects the urllib default User-Agent with 403.
    request = urllib.request.Request(
        join.source_url,
        headers={"User-Agent": "flowstate-ingest"},
    )
    log.info("GET %s", join.source_url)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read()
    except Exception as exc:
        raise RuntimeError(
            f"ward polygon download failed for {path.name}"
        ) from exc
    path.write_bytes(payload)
    log.info(
        "TIMING series=ward_polygon wall_s=%.3f bytes=%s file=%s",
        time.perf_counter() - t0,
        path.stat().st_size,
        path,
    )
    digest = sha256_file(path)
    if digest != join.geojson_sha256:
        path.unlink()
        raise ValueError(f"ward geojson hash is {digest}")
    return path


def _row(
    ward_num: str, by_no: dict[int, dict[str, Any]], join: WardJoin
) -> dict[str, Any]:
    prefix, number = _split(ward_num)
    in_scope = prefix == join.in_scope_prefix
    if not in_scope or number is None or number not in by_no:
        status = "unmatched" if in_scope else "out_of_scope"
        return {
            "ward_num": ward_num,
            "city": None,
            "official_ward_no": number if in_scope else None,
            "ward_name": None,
            "latitude": None,
            "longitude": None,
            "pin_method": None,
            "source_url": None,
            "source_date": None,
            "match_status": status,
        }
    feature = by_no[number]
    rings = [
        [_lat_lon(point, join.axis) for point in ring]
        for ring in feature["geometry"]["coordinates"]
    ]
    latitude, longitude = _centroid(rings)
    method = "centroid"
    if not contains(latitude, longitude, rings):
        latitude, longitude = _interior(rings)
        method = "interior"
    if not contains(latitude, longitude, rings):
        raise ValueError(f"ward pin outside polygon: {ward_num}")
    return {
        "ward_num": ward_num,
        "city": join.city_id,
        "official_ward_no": number,
        "ward_name": feature["properties"][join.name_field],
        "latitude": latitude,
        "longitude": longitude,
        "pin_method": method,
        "source_url": join.source_url,
        "source_date": join.scheme_year,
        "match_status": "matched",
    }


def _index(
    collection: dict[str, Any], join: WardJoin
) -> dict[int, dict[str, Any]]:
    indexed = {}
    for feature in collection["features"]:
        number = int(feature["properties"][join.number_field])
        if number in indexed:
            raise ValueError(f"duplicate {join.number_field} {number}")
        indexed[number] = feature
    return indexed


def _lat_lon(point: list[float], axis: str) -> tuple[float, float]:
    if axis == "latitude_longitude":
        return point[0], point[1]
    if axis == "longitude_latitude":
        return point[1], point[0]
    raise ValueError(f"unknown axis {axis}")


def _centroid(rings: list[list[tuple[float, float]]]) -> tuple[float, float]:
    cross_sum = 0.0
    latitude_sum = 0.0
    longitude_sum = 0.0
    for ring in rings:
        for start, end in zip(ring, ring[1:]):
            cross = start[0] * end[1] - end[0] * start[1]
            cross_sum += cross
            latitude_sum += (start[0] + end[0]) * cross
            longitude_sum += (start[1] + end[1]) * cross
    if cross_sum == 0:
        return rings[0][0]
    return latitude_sum / (3 * cross_sum), longitude_sum / (3 * cross_sum)


def _interior(rings: list[list[tuple[float, float]]]) -> tuple[float, float]:
    latitudes = [point[0] for point in rings[0]]
    low, high = min(latitudes), max(latitudes)
    best: tuple[float, float, float] | None = None
    for fraction in (0.5, 0.35, 0.65, 0.2, 0.8, 0.45, 0.55):
        latitude = low + (high - low) * fraction
        hits = _crossings(rings, latitude)
        if len(hits) % 2:
            continue
        for index in range(0, len(hits), 2):
            left, right = hits[index], hits[index + 1]
            span = right - left
            if span <= 0:
                continue
            longitude = (left + right) / 2
            if not contains(latitude, longitude, rings):
                continue
            if best is None or span > best[0]:
                best = (span, latitude, longitude)
    if best is None:
        raise ValueError("ward polygon has no interior chord")
    return best[1], best[2]


def _crossings(
    rings: list[list[tuple[float, float]]], latitude: float
) -> list[float]:
    hits = []
    for ring in rings:
        for start, end in zip(ring, ring[1:]):
            if (start[0] > latitude) == (end[0] > latitude):
                continue
            if end[0] == start[0]:
                continue
            share = (latitude - start[0]) / (end[0] - start[0])
            hits.append(start[1] + (end[1] - start[1]) * share)
    hits.sort()
    return hits


def _in_ring(
    latitude: float,
    longitude: float,
    ring: list[tuple[float, float]],
) -> bool:
    inside = False
    for start, end in zip(ring, ring[1:]):
        if (start[1] > longitude) == (end[1] > longitude):
            continue
        if end[1] == start[1]:
            continue
        crossed = (end[0] - start[0]) * (longitude - start[1]) / (
            end[1] - start[1]
        ) + start[0]
        if latitude < crossed:
            inside = not inside
    return inside


def _split(ward_num: str) -> tuple[str, int | None]:
    prefix, _, number = ward_num.partition("_")
    if not number.isdigit():
        return prefix, None
    return prefix, int(number)


def _sort_key(ward_num: str) -> tuple[str, int, str]:
    prefix, number = _split(ward_num)
    return (prefix, number if number is not None else -1, ward_num)


def _schema() -> pa.Schema:
    return pa.schema(
        [
            ("ward_num", pa.string()),
            ("city", pa.string()),
            ("official_ward_no", pa.int64()),
            ("ward_name", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("pin_method", pa.string()),
            ("source_url", pa.string()),
            ("source_date", pa.string()),
            ("match_status", pa.string()),
        ]
    )

"""Ward polygons, in-ward points, and the station-to-ward crosswalk.

ADR-0017. The 2022 BBMP polygons (``ward_geography`` source) give each
emitted ward a shape. This module does three jobs and nothing else:

* ``point`` picks a deterministic point inside a ward for one event.
  It is a synthetic placement, never an observed trip location.
* ``station_ward`` maps a station pin to the ward that contains it.
  A pin outside every polygon takes the nearest ward centroid.
* ``stops_near`` lists GTFS stops within a radius of a station pin.

The ring axis order follows the city profile (``WardJoin.axis``). The
Bengaluru file stores [latitude, longitude].
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from flowstate.normalize.cities import dataset_path
from flowstate.normalize.cities.profile import WardJoin
from flowstate.normalize.tables.wards import _index, _lat_lon

SAMPLES_PER_WARD = 512
EARTH_KM = 6371.0088


def haversine_km(
    a: tuple[float, float], b: tuple[float, float]
) -> float:
    phi1, phi2 = math.radians(a[0]), math.radians(b[0])
    dphi = math.radians(b[0] - a[0])
    dlam = math.radians(b[1] - a[1])
    h = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    )
    return 2 * EARTH_KM * math.asin(math.sqrt(h))


def inside_rings(
    latitudes: np.ndarray, longitudes: np.ndarray, rings: list[list[Any]]
) -> np.ndarray:
    """Even-odd test over every ring, so a hole is outside."""
    if len(latitudes) > 256:
        # Chunk the points so the points-by-edges matrix stays small.
        parts = [
            inside_rings(latitudes[i : i + 256], longitudes[i : i + 256], rings)
            for i in range(0, len(latitudes), 256)
        ]
        return np.concatenate(parts)
    inside = np.zeros(latitudes.shape, dtype=bool)
    for ring in rings:
        start = np.asarray(ring[:-1], dtype=float)
        end = np.asarray(ring[1:], dtype=float)
        # Edge j spans longitude s1..e1. A point crosses it when its
        # longitude sits between them and its latitude is south of
        # the edge at that longitude.
        s_lat, s_lon = start[:, 0], start[:, 1]
        e_lat, e_lon = end[:, 0], end[:, 1]
        span = e_lon - s_lon
        valid = span != 0
        lon = longitudes[:, None]
        straddle = (s_lon[None, :] > lon) != (e_lon[None, :] > lon)
        safe_span = np.where(valid, span, 1.0)[None, :]
        cross = (
            (e_lat - s_lat)[None, :] * (lon - s_lon[None, :]) / safe_span
            + s_lat[None, :]
        )
        hit = straddle & valid[None, :] & (latitudes[:, None] < cross)
        inside ^= (hit.sum(axis=1) % 2).astype(bool)
    return inside


@dataclass
class WardGeometry:
    """Polygons and deterministic in-ward points for matched wards."""

    rings: dict[str, list[list[tuple[float, float]]]]
    centroids: dict[str, tuple[float, float]]
    samples: dict[str, np.ndarray] = field(default_factory=dict)

    def has(self, ward_id: str) -> bool:
        return ward_id in self.rings

    def point(self, ward_id: str, key: str) -> tuple[float, float] | None:
        """One synthetic in-ward point. Same ward and key, same point."""
        pool = self.samples.get(ward_id)
        if pool is None or len(pool) == 0:
            return self.centroids.get(ward_id)
        digest = hashlib.sha256(f"{ward_id}|{key}".encode()).digest()
        index = int.from_bytes(digest[:8], "big") % len(pool)
        return float(pool[index][0]), float(pool[index][1])

    def containing(self, latitude: float, longitude: float) -> str | None:
        probe_lat = np.asarray([latitude])
        probe_lon = np.asarray([longitude])
        for ward_id, rings in self.rings.items():
            lats = [p[0] for p in rings[0]]
            lons = [p[1] for p in rings[0]]
            if not (
                min(lats) <= latitude <= max(lats)
                and min(lons) <= longitude <= max(lons)
            ):
                continue
            if bool(inside_rings(probe_lat, probe_lon, rings)[0]):
                return ward_id
        return None

    def nearest(self, latitude: float, longitude: float) -> str | None:
        best: tuple[float, str] | None = None
        for ward_id, centre in self.centroids.items():
            km = haversine_km((latitude, longitude), centre)
            if best is None or km < best[0]:
                best = (km, ward_id)
        return None if best is None else best[1]


def load_geometry(
    datasets_dir: Path,
    join: WardJoin,
    centroids: dict[str, tuple[float, float]],
) -> WardGeometry:
    """Read the polygon file. A matched ward without a polygon is an error."""
    path = dataset_path(datasets_dir, join.geojson_parts)
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing. Run make normalize.")
    collection = json.loads(path.read_text())
    by_number = _index(collection, join)
    rings: dict[str, list[list[tuple[float, float]]]] = {}
    for ward_id in centroids:
        prefix, _, number = ward_id.partition("_")
        if prefix != join.in_scope_prefix or not number.isdigit():
            continue
        feature = by_number.get(int(number))
        if feature is None:
            raise ValueError(f"no polygon for ward {ward_id}")
        rings[ward_id] = [
            [_lat_lon(point, join.axis) for point in ring]
            for ring in feature["geometry"]["coordinates"]
        ]
    geometry = WardGeometry(rings=rings, centroids=dict(centroids))
    for ward_id, shape in rings.items():
        geometry.samples[ward_id] = _sample(ward_id, shape)
    return geometry


def _sample(
    ward_id: str, rings: list[list[tuple[float, float]]]
) -> np.ndarray:
    lats = [p[0] for p in rings[0]]
    lons = [p[1] for p in rings[0]]
    low_lat, high_lat = min(lats), max(lats)
    low_lon, high_lon = min(lons), max(lons)
    seed = int.from_bytes(
        hashlib.sha256(f"ward-sample|{ward_id}".encode()).digest()[:8], "big"
    )
    rng = np.random.default_rng(seed)
    kept = np.empty((0, 2), dtype=float)
    for _ in range(6):
        draw_lat = rng.uniform(low_lat, high_lat, SAMPLES_PER_WARD * 4)
        draw_lon = rng.uniform(low_lon, high_lon, SAMPLES_PER_WARD * 4)
        mask = inside_rings(draw_lat, draw_lon, rings)
        found = np.stack([draw_lat[mask], draw_lon[mask]], axis=1)
        kept = np.concatenate([kept, found])
        if len(kept) >= SAMPLES_PER_WARD:
            break
    return kept[:SAMPLES_PER_WARD]


def stops_within(
    centre: tuple[float, float],
    stops: dict[str, tuple[float, float]],
    radius_km: float,
) -> list[str]:
    """Stop ids within ``radius_km`` of ``centre``, nearest first."""
    if not stops:
        return []
    ids = list(stops)
    points = np.asarray([stops[stop_id] for stop_id in ids], dtype=float)
    lat1 = math.radians(centre[0])
    lat2 = np.radians(points[:, 0])
    dphi = lat2 - lat1
    dlam = np.radians(points[:, 1] - centre[1])
    h = (
        np.sin(dphi / 2) ** 2
        + math.cos(lat1) * np.cos(lat2) * np.sin(dlam / 2) ** 2
    )
    distance = 2 * EARTH_KM * np.arcsin(np.sqrt(h))
    order = np.argsort(distance)
    return [ids[i] for i in order if distance[i] <= radius_km]

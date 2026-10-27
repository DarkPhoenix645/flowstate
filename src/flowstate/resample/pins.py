"""Geography pins copied onto the Kafka envelope.

The ward centroid is the polygon centroid (or an interior chord) from
``ward_geography``. It is not a Places geocode of the ward id. A
ride-hail event is placed at a deterministic synthetic point inside its
ward polygon (ADR-0017). Out-of-scope ward ids are not emitted, so every
ride-hail event has a place. Station and airport pins come from the
Places cache. Bus pins are the GTFS stop coordinates.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

from flowstate.config import Settings
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.resample.geo import WardGeometry, load_geometry, stops_within

BUS_RADIUS_KM = 1.0


@dataclass
class PinBook:
    wards: dict[str, tuple[float, float]]
    stations: dict[str, tuple[float, float]]
    stops: dict[str, tuple[float, float]]
    airports: dict[str, tuple[float, float]]
    geometry: WardGeometry | None = None
    _station_ward: dict[str, str | None] = field(default_factory=dict)
    _station_stops: dict[str, list[str]] = field(default_factory=dict)

    def ward(self, ward_id: str | None) -> tuple[float, float] | None:
        if ward_id is None:
            return None
        return self.wards.get(str(ward_id))

    def ward_point(
        self, ward_id: str | None, key: str
    ) -> tuple[float, float, str] | None:
        """A point for one event and how it was placed.

        ``ward_synthetic_point`` is a deterministic point inside the
        polygon. ``ward_centroid`` is the fallback when no polygon is
        loaded.
        """
        if ward_id is None:
            return None
        if self.geometry is not None and self.geometry.has(str(ward_id)):
            point = self.geometry.point(str(ward_id), key)
            if point is not None:
                return point[0], point[1], "ward_synthetic_point"
        centre = self.wards.get(str(ward_id))
        if centre is None:
            return None
        return centre[0], centre[1], "ward_centroid"

    def station(self, station_id: str | None) -> tuple[float, float] | None:
        if station_id is None:
            return None
        return self.stations.get(str(station_id))

    def stop(self, stop_id: str | None) -> tuple[float, float] | None:
        if stop_id is None:
            return None
        return self.stops.get(str(stop_id))

    def airport(self, code: str | None) -> tuple[float, float] | None:
        if code is None:
            return None
        return self.airports.get(str(code))

    def station_ward(self, station_id: str) -> str | None:
        """The ward that contains the station pin, else the nearest."""
        key = str(station_id)
        if key in self._station_ward:
            return self._station_ward[key]
        pin = self.stations.get(key)
        ward: str | None = None
        if pin is not None and self.geometry is not None:
            ward = self.geometry.containing(pin[0], pin[1])
            if ward is None:
                ward = self.geometry.nearest(pin[0], pin[1])
        self._station_ward[key] = ward
        return ward

    def stops_near_station(
        self, station_id: str, radius_km: float = BUS_RADIUS_KM
    ) -> list[str]:
        key = f"{station_id}|{radius_km}"
        if key not in self._station_stops:
            pin = self.stations.get(str(station_id))
            self._station_stops[key] = (
                [] if pin is None else stops_within(pin, self.stops, radius_km)
            )
        return self._station_stops[key]


def load_pin_book(settings: Settings | Path) -> PinBook:
    """Read the pin tables and the ward polygons. A missing file is an error."""
    data_dir = settings.data_dir if isinstance(settings, Settings) else settings
    datasets = (
        settings.datasets_dir
        if isinstance(settings, Settings)
        else data_dir / "raw"
    )
    normalized = data_dir / "normalized"
    wards = _points(
        normalized / "ward_geography.parquet",
        "ward_num",
        "latitude",
        "longitude",
    )
    profile = load(DEFAULT_CITY_ID)
    geometry = load_geometry(datasets, profile.ward, wards)
    return PinBook(
        wards=wards,
        stations=_station_points(normalized / "station_geography.parquet"),
        stops=_points(
            data_dir / "staged" / "bmtc" / "stops.parquet",
            "stop_id",
            "stop_lat",
            "stop_lon",
        ),
        airports=_points(
            normalized / "airport_geography.parquet",
            "airport_code",
            "latitude",
            "longitude",
        ),
        geometry=geometry,
    )


def _points(
    path: Path, key: str, lat: str, lon: str
) -> dict[str, tuple[float, float]]:
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing. Run make normalize.")
    frame = pq.read_table(path, columns=[key, lat, lon]).to_pydict()
    pins: dict[str, tuple[float, float]] = {}
    for ident, latitude, longitude in zip(
        frame[key], frame[lat], frame[lon], strict=True
    ):
        point = _pair(latitude, longitude)
        if ident is None or point is None:
            continue
        pins[str(ident)] = point
    return pins


def _station_points(path: Path) -> dict[str, tuple[float, float]]:
    """Index a station pin by code, RTI label, and name."""
    if not path.is_file():
        raise FileNotFoundError(f"{path} is missing. Run make normalize.")
    table = pq.read_table(path)
    pins: dict[str, tuple[float, float]] = {}
    for row in table.to_pylist():
        point = _pair(row.get("latitude"), row.get("longitude"))
        if point is None:
            continue
        for key in ("station_code", "rti_label", "name"):
            value = row.get(key)
            if value:
                pins[str(value)] = point
    return pins


def _pair(latitude: object, longitude: object) -> tuple[float, float] | None:
    if latitude is None or longitude is None:
        return None
    if latitude == "" or longitude == "":
        return None
    return float(latitude), float(longitude)

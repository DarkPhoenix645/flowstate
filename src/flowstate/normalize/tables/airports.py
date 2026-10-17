"""Airport pins from the city profile.

One row per airport. A city can list more than one.
The pin comes from Places. A miss stays null.
"""

from __future__ import annotations

import pyarrow as pa

from flowstate.normalize.cities.profile import Airport, CityProfile
from flowstate.normalize.coverage import TableBuild

COLUMNS = [
    "airport_code",
    "query",
    "latitude",
    "longitude",
    "opensky_extract",
]


def build_airport_geography(
    airports: tuple[Airport, ...],
    pins: dict[str, tuple[float, float] | None],
) -> TableBuild:
    rows = []
    for airport in airports:
        point = pins.get(airport.code)
        latitude = longitude = None
        if point is not None:
            latitude, longitude = point
        rows.append(
            {
                "airport_code": airport.code,
                "query": airport.query,
                "latitude": latitude,
                "longitude": longitude,
                "opensky_extract": airport.opensky_extract,
            }
        )
    table = pa.Table.from_pylist(rows, schema=_schema())
    coverage = {
        "rows": table.num_rows,
        "pins": sum(row["latitude"] is not None for row in rows),
        "extracts": sum(airport.opensky_extract for airport in airports),
        "veracity": (
            "One row per airport on the city profile. "
            "The pin comes from Places. A miss stays null."
        ),
    }
    return TableBuild(table, coverage)


def from_profile(
    profile: CityProfile,
    pins: dict[str, tuple[float, float] | None],
) -> TableBuild:
    return build_airport_geography(profile.airports, pins)


def _schema() -> pa.Schema:
    return pa.schema(
        [
            ("airport_code", pa.string()),
            ("query", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("opensky_extract", pa.bool_()),
        ]
    )

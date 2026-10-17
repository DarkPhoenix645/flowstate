"""Registered cities. v1 has Bengaluru."""

from __future__ import annotations

from pathlib import Path

from flowstate.normalize.cities.bengaluru import BENGALURU
from flowstate.normalize.cities.profile import CityProfile

DEFAULT_CITY_ID = "bengaluru"

CITIES: dict[str, CityProfile] = {
    BENGALURU.id: BENGALURU,
}


def load(city_id: str) -> CityProfile:
    try:
        return CITIES[city_id]
    except KeyError:
        known = ", ".join(sorted(CITIES))
        raise ValueError(
            f"unknown city {city_id!r}; known: {known}"
        ) from None


def dataset_path(root: Path, parts: tuple[str, ...]) -> Path:
    path = root
    for part in parts:
        path = path / part
    return path

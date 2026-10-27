"""Places station lookup. Live cases skip when the key is absent."""

from __future__ import annotations

import json
import math

import pytest

from flowstate.config import Settings
from flowstate.ingest.amplify import main as amplify_main
from flowstate.normalize.cities.bengaluru import BENGALURU
from flowstate.normalize.places.geocode import (
    lookup_airports,
    lookup_stations,
    places_cache_path,
)
from flowstate.normalize.places.review import record_failure, review_path
from flowstate.provenance import ProvenanceError
from flowstate.streaming.producer import main as produce_main

BOUNDS = BENGALURU.geocode_bounds
ALIASES = dict(BENGALURU.station_aliases)
CANONICAL = {
    "Mantri Square Sampige Road",
    "Nallurahalli",
    "Infosys Foundation Konappana Agrahara",
    "Electronic City",
    "Krantivira Sangolli Rayanna Railway Station",
}

MANUAL = {
    "Mantri Square Sampige Road": (
        12.990649716687187,
        77.57085854536761,
    ),
    "Nallurahalli": (12.976767006881, 77.724855959513),
    "Infosys Foundation Konappana Agrahara": (
        12.846594703281646,
        77.67128647892753,
    ),
    "Electronic City": (12.856454732620097, 77.66376907329013),
    "Krantivira Sangolli Rayanna Railway Station": (
        12.975814,
        77.565708,
    ),
}


def _haversine_m(
    left: tuple[float, float], right: tuple[float, float]
) -> float:
    lat1, lon1 = map(math.radians, left)
    lat2, lon2 = map(math.radians, right)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    half = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 6371000 * 2 * math.asin(math.sqrt(half))


def test_unknown_name_is_not_sent(tmp_path):
    calls: list[str] = []

    def fetch(query, _key, _bounds):
        calls.append(query)
        return []

    pins, stats = lookup_stations(
        ["Not A Station"],
        aliases=ALIASES,
        canonical=CANONICAL,
        query_template=BENGALURU.station_geocode,
        path=places_cache_path(tmp_path, "bengaluru"),
        review=review_path(tmp_path, "bengaluru"),
        bounds=BOUNDS,
        api_key="test-key",
        fetch=True,
        fetch_places=fetch,
    )
    assert calls == []
    assert pins["Not A Station"] is None
    assert stats["needs_review"] == ["Not A Station"]
    store = json.loads(review_path(tmp_path, "bengaluru").read_text())
    assert store["Not A Station"]["status"] == "needs_review"
    assert store["Not A Station"]["reason"] == "unknown_name"


def test_miss_is_cached_and_not_sent_again(tmp_path):
    calls: list[str] = []

    def fetch(query, _key, _bounds):
        calls.append(query)
        return [
            {
                "id": "bus-1",
                "displayName": {"text": "Bus stop"},
                "location": {"latitude": 12.0, "longitude": 77.0},
                "types": ["bus_station", "transit_station"],
            }
        ]

    kwargs = dict(
        aliases=ALIASES,
        canonical=CANONICAL,
        query_template=BENGALURU.station_geocode,
        path=places_cache_path(tmp_path, "bengaluru"),
        review=review_path(tmp_path, "bengaluru"),
        bounds=BOUNDS,
        api_key="test-key",
        fetch=True,
        fetch_places=fetch,
    )
    names = ["Nallurahalli", "Nallur Halli", "Nallurhalli"]
    first, _stats = lookup_stations(names, **kwargs)
    second, stats = lookup_stations(names, **kwargs)
    assert calls == ["Nallurahalli metro station"]
    assert first["Nallur Halli"] is None
    assert second["Nallurhalli"] is None
    assert stats["cached"] == 0
    assert stats["missed"] == 1
    cache = json.loads(kwargs["path"].read_text())
    assert cache["Nallurahalli"]["status"] == "miss"
    assert cache["Nallurahalli"]["provider"] == "places"
    store = json.loads(kwargs["review"].read_text())
    assert store["Nallurahalli"]["status"] == "needs_review"
    assert store["Nallurahalli"]["reason"] == "no_subway_station"
    assert store["Nallurahalli"]["candidates"][0]["types"] == [
        "bus_station",
        "transit_station",
    ]


def test_open_review_stops_amplify_and_produce(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWSTATE_DATA_DIR", str(tmp_path))
    record_failure(
        review_path(tmp_path, "bengaluru"),
        "Nallurahalli",
        query="Nallurahalli metro station",
        reason="zero_candidates",
        candidates=[],
    )
    with pytest.raises(SystemExit, match="Nallurahalli"):
        amplify_main([])
    with pytest.raises(SystemExit, match="Nallurahalli"):
        produce_main([])


def test_finished_review_lets_amplify_run(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOWSTATE_DATA_DIR", str(tmp_path))
    path = review_path(tmp_path, "bengaluru")
    record_failure(
        path,
        "Nallurahalli",
        query="Nallurahalli metro station",
        reason="zero_candidates",
        candidates=[
            {
                "id": "place-1",
                "displayName": "Nallur Halli",
                "latitude": 1.0,
                "longitude": 2.0,
                "types": ["subway_station"],
            }
        ],
    )
    store = json.loads(path.read_text())
    store["Nallurahalli"]["review"] = {
        "place_id": "place-1",
        "latitude": None,
        "longitude": None,
        "note": "Checked the Whitefield station.",
        "reviewed_at": "2026-10-16T00:00:00Z",
    }
    path.write_text(json.dumps(store, indent=2) + "\n")
    # Review gate passed. DATA_DIR is tmp_path, so the manifest is
    # absent and amplify stops before it reads the repo extracts.
    with pytest.raises(ProvenanceError, match="manifest.json"):
        amplify_main([])


def test_airport_miss_is_cached_and_not_sent_again(tmp_path):
    calls: list[str] = []

    def fetch(query, _key, _bounds):
        calls.append(query)
        return [
            {
                "id": "bus-1",
                "displayName": {"text": "Bus stop"},
                "location": {"latitude": 13.2, "longitude": 77.7},
                "types": ["bus_station"],
            }
        ]

    kwargs = dict(
        path=places_cache_path(tmp_path, "bengaluru"),
        review=review_path(tmp_path, "bengaluru"),
        bounds=BOUNDS,
        api_key="test-key",
        fetch=True,
        fetch_places=fetch,
    )
    query = BENGALURU.airports[0].query
    first, _stats = lookup_airports([("VOBL", query)], **kwargs)
    second, stats = lookup_airports([("VOBL", query)], **kwargs)
    assert calls == [query]
    assert first["VOBL"] is None
    assert second["VOBL"] is None
    assert stats["cached"] == 0
    assert stats["missed"] == 1
    assert stats["needs_review"] == ["VOBL"]
    cache = json.loads(kwargs["path"].read_text())
    assert cache["VOBL"]["status"] == "miss"
    assert cache["VOBL"]["included_type"] == "airport"
    store = json.loads(kwargs["review"].read_text())
    assert store["VOBL"]["reason"] == "no_airport"
    assert store["VOBL"]["query"] == query


def test_places_pins_match_the_manual_points(tmp_path):
    key = Settings().google_maps_api_key.strip()
    if not key:
        pytest.skip("FLOWSTATE_GOOGLE_MAPS_API_KEY is not set")
    names = [
        "Mantri Square Sampige Road",
        "Nallurahalli",
        "Nallur Halli",
        "Nallurhalli",
        "Infosys Foundation Konappana Agrahara",
        "Electronic City",
        "Krantivira Sangolli Rayanna Railway Station",
        "KSR Railway Station",
        "Bangalore City Station",
    ]
    pins, stats = lookup_stations(
        names,
        aliases=ALIASES,
        canonical=CANONICAL,
        query_template=BENGALURU.station_geocode,
        path=places_cache_path(tmp_path, "bengaluru"),
        review=review_path(tmp_path, "bengaluru"),
        bounds=BOUNDS,
        api_key=key,
        fetch=True,
    )
    assert stats["fetched"] == 5
    assert stats["needs_review"] == []
    assert pins["Nallur Halli"] == pins["Nallurahalli"]
    assert pins["Nallurhalli"] == pins["Nallurahalli"]
    assert pins["KSR Railway Station"] == pins[
        "Krantivira Sangolli Rayanna Railway Station"
    ]
    assert pins["Bangalore City Station"] == pins[
        "Krantivira Sangolli Rayanna Railway Station"
    ]
    distances = {}
    for name, manual in MANUAL.items():
        point = pins[name]
        assert point is not None
        distances[name] = _haversine_m(point, manual)
        assert distances[name] < 80
    rounded = {key: round(value, 1) for key, value in distances.items()}
    print("places distances_m", rounded)

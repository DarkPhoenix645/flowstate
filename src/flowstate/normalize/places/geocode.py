"""Station and airport pins from Places.

Both use ``data/geocode/<city>.places.json``.
A stored query is not sent again.
A miss is cached as a miss and listed for human review.
Pytest calls this with fetch=False, so it does not use the network.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any, NoReturn

from flowstate.normalize.cities.profile import GeoBounds
from flowstate.normalize.places.review import record_failure, reviewed_point

log = logging.getLogger("normalize.places")

PlacesFetch = Callable[[str, str, GeoBounds], list[dict[str, Any]]]


class PlacesDisabled(RuntimeError):
    """Places API (New) is not enabled. Do not fall back."""


def places_cache_path(data_dir: Path, city_id: str) -> Path:
    """Places cache for stations and airports."""
    return data_dir / "geocode" / f"{city_id}.places.json"


def canonical_names(
    rows: list[dict[str, Any]], aliases: dict[str, str]
) -> set[str]:
    """Official names from station codes, plus alias targets.

    An alias key is not itself canonical, even when the code file
    uses that spelling.
    """
    alias_keys = set(aliases)
    names = {
        str(row["name"])
        for row in rows
        if row.get("station_code") and str(row["name"]) not in alias_keys
    }
    names.update(aliases.values())
    return names


def lookup_stations(
    names: list[str],
    *,
    aliases: dict[str, str],
    canonical: set[str],
    query_template: str,
    path: Path,
    review: Path,
    bounds: GeoBounds,
    api_key: str,
    fetch: bool,
    fetch_places: PlacesFetch | None = None,
) -> tuple[dict[str, tuple[float, float] | None], dict[str, Any]]:
    """One Places request per canonical station.

    A name that is not canonical and not an alias is not queried.
    A miss is cached as a miss. Status ``OK`` is only a subway pin.
    """
    allowed = set(canonical) | set(aliases.values())
    groups: dict[str, list[str]] = {}
    unknown: list[str] = []
    for name in dict.fromkeys(names):
        if name in aliases:
            groups.setdefault(aliases[name], []).append(name)
            continue
        if name in allowed:
            groups.setdefault(name, []).append(name)
            continue
        unknown.append(name)
    cache = _read(path)
    pins: dict[str, tuple[float, float] | None] = {}
    stats: dict[str, Any] = {
        "cached": 0,
        "fetched": 0,
        "missed": 0,
        "needs_review": [],
    }
    caller = fetch_places or _default_fetch("subway_station")
    for name in unknown:
        log.info("places miss %s reason=unknown_name", name)
        record_failure(
            review,
            name,
            query=None,
            reason="unknown_name",
            candidates=[],
        )
        pins[name] = None
        stats["missed"] += 1
        stats["needs_review"].append(name)
    for canon, inputs in groups.items():
        point, origin = _named_point(
            canon,
            query=query_template.format(name=canon),
            included_type="subway_station",
            cache=cache,
            path=path,
            review=review,
            bounds=bounds,
            api_key=api_key,
            fetch=fetch,
            caller=caller,
            stats=stats,
        )
        if origin == "cached":
            stats["cached"] += 1
        elif origin == "fetched":
            stats["fetched"] += 1
        elif origin == "miss":
            stats["missed"] += 1
        for raw in inputs:
            pins[raw] = point
    if stats["fetched"]:
        _write(path, cache)
    return pins, stats


def lookup_airports(
    airports: list[tuple[str, str]],
    *,
    path: Path,
    review: Path,
    bounds: GeoBounds,
    api_key: str,
    fetch: bool,
    fetch_places: PlacesFetch | None = None,
) -> tuple[dict[str, tuple[float, float] | None], dict[str, Any]]:
    """One Places request per airport code.

    ``includedType`` is ``airport``. A miss is cached and reviewed.
    """
    cache = _read(path)
    pins: dict[str, tuple[float, float] | None] = {}
    stats: dict[str, Any] = {
        "cached": 0,
        "fetched": 0,
        "missed": 0,
        "needs_review": [],
    }
    caller = fetch_places or _default_fetch("airport")
    for code, query in airports:
        point, origin = _named_point(
            code,
            query=query,
            included_type="airport",
            cache=cache,
            path=path,
            review=review,
            bounds=bounds,
            api_key=api_key,
            fetch=fetch,
            caller=caller,
            stats=stats,
        )
        if origin == "cached":
            stats["cached"] += 1
        elif origin == "fetched":
            stats["fetched"] += 1
        elif origin == "miss":
            stats["missed"] += 1
        pins[code] = point
    if stats["fetched"]:
        _write(path, cache)
    return pins, stats


def _named_point(
    canon: str,
    *,
    query: str,
    included_type: str,
    cache: dict[str, Any],
    path: Path,
    review: Path,
    bounds: GeoBounds,
    api_key: str,
    fetch: bool,
    caller: PlacesFetch,
    stats: dict[str, Any],
) -> tuple[tuple[float, float] | None, str]:
    reviewed = reviewed_point(review, canon)
    if reviewed is not None:
        return reviewed, "cached"
    entry = cache.get(canon)
    if isinstance(entry, dict) and entry.get("provider") == "places":
        if entry.get("status") == "OK":
            return _point(entry), "cached"
        if entry.get("status") == "miss":
            stats["needs_review"].append(canon)
            return None, "miss"
    if not fetch or not api_key:
        return None, "skip"
    places = caller(query, api_key, bounds)
    candidates = [_candidate(place) for place in places]
    chosen = _choose_type(candidates, included_type)
    if chosen is None:
        reason = (
            "zero_candidates"
            if not candidates
            else f"no_{included_type}"
        )
        log.info("places miss %s reason=%s", canon, reason)
        cache[canon] = {
            "provider": "places",
            "status": "miss",
            "query": query,
            "included_type": included_type,
            "reason": reason,
        }
        record_failure(
            review,
            canon,
            query=query,
            reason=reason,
            candidates=candidates,
        )
        stats["needs_review"].append(canon)
        _write(path, cache)
        return None, "miss"
    cache[canon] = {
        "provider": "places",
        "status": "OK",
        "query": query,
        "included_type": included_type,
        "place_id": chosen["id"],
        "display_name": chosen["displayName"],
        "latitude": chosen["latitude"],
        "longitude": chosen["longitude"],
        "types": chosen["types"],
    }
    return (
        (float(chosen["latitude"]), float(chosen["longitude"])),
        "fetched",
    )


def _candidate(place: dict[str, Any]) -> dict[str, Any]:
    location = place.get("location") or {}
    display = place.get("displayName")
    if isinstance(display, dict):
        display = display.get("text")
    return {
        "id": place.get("id"),
        "displayName": display,
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "types": list(place.get("types") or []),
    }


def _choose_type(
    candidates: list[dict[str, Any]], included_type: str
) -> dict[str, Any] | None:
    for candidate in candidates:
        if included_type not in candidate["types"]:
            continue
        if candidate["latitude"] is None or candidate["longitude"] is None:
            continue
        return candidate
    return None


def _default_fetch(included_type: str) -> PlacesFetch:
    def fetch(
        query: str, api_key: str, bounds: GeoBounds
    ) -> list[dict[str, Any]]:
        return _fetch_places(query, api_key, bounds, included_type)

    return fetch


def _fetch_places(
    query: str,
    api_key: str,
    bounds: GeoBounds,
    included_type: str,
) -> list[dict[str, Any]]:
    body = {
        "textQuery": query,
        "includedType": included_type,
        "strictTypeFiltering": True,
        "pageSize": 5,
        "locationRestriction": {
            "rectangle": {
                "low": {
                    "latitude": bounds.min_latitude,
                    "longitude": bounds.min_longitude,
                },
                "high": {
                    "latitude": bounds.max_latitude,
                    "longitude": bounds.max_longitude,
                },
            }
        },
    }
    log.info("GET places %s type=%s", query, included_type)
    t0 = time.perf_counter()
    request = urllib.request.Request(
        "https://places.googleapis.com/v1/places:searchText",
        data=json.dumps(body).encode(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Goog-Api-Key": api_key,
            "X-Goog-FieldMask": (
                "places.id,places.displayName,"
                "places.location,places.types"
            ),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        _places_http_error(exc, api_key, query)
    except Exception as exc:
        message = str(exc).replace(api_key, "[redacted]")
        raise RuntimeError(
            f"Places API (New) request failed for {query!r}: {message}"
        ) from None
    places = payload.get("places") or []
    if not isinstance(places, list):
        places = []
    log.info(
        "saved places %s candidates=%s wall_s=%.2f",
        query,
        len(places),
        time.perf_counter() - t0,
    )
    return places


def _places_http_error(
    exc: urllib.error.HTTPError, api_key: str, query: str
) -> NoReturn:
    detail = exc.read().decode(errors="replace").replace(api_key, "[redacted]")
    blocked = exc.code == 403 or "API_KEY_SERVICE_BLOCKED" in detail
    if blocked:
        raise PlacesDisabled(
            "Places API (New) is not enabled for this key."
        ) from None
    raise RuntimeError(
        f"Places API (New) HTTP {exc.code} for {query!r}"
    ) from None


def _point(entry: dict[str, Any]) -> tuple[float, float] | None:
    if entry.get("status") != "OK":
        return None
    return float(entry["latitude"]), float(entry["longitude"])


def _read(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"geocode cache {path} is not an object")
    return payload


def _write(path: Path, cache: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, indent=2, sort_keys=True) + "\n")

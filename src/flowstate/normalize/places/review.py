"""Manual review of Places pins that were not accepted.

Stations and airports share this store. Both only have a name.

The store is ``data/geocode/<city>.review.json``. It starts empty.
A lookup failure appends one object. A human finishes it.
Downstream commands refuse to run while any entry is still open.

An open entry::

    {
      "Nallurahalli": {
        "status": "needs_review",
        "query": "Nallurahalli metro station",
        "reason": "zero_candidates",
        "candidates": []
      }
    }

A finished entry adds ``review``. Use a candidate ``id`` or an
explicit latitude and longitude. Both need a note and a timestamp::

    "review": {
      "place_id": "ChIJ...",
      "latitude": null,
      "longitude": null,
      "note": "Matched the Whitefield station on the map.",
      "reviewed_at": "2026-10-16T02:10:00+05:30"
    }
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def review_path(data_dir: Path, city_id: str) -> Path:
    return data_dir / "geocode" / f"{city_id}.review.json"


def record_failure(
    path: Path,
    name: str,
    *,
    query: str | None,
    reason: str,
    candidates: list[dict[str, Any]] | None = None,
) -> None:
    """Add or refresh an open review. Do not touch a finished one."""
    store = _read(path)
    current = store.get(name)
    if isinstance(current, dict) and _complete(current.get("review")):
        return
    store[name] = {
        "status": "needs_review",
        "query": query,
        "reason": reason,
        "candidates": candidates or [],
    }
    _write(path, store)


def reviewed_point(
    path: Path, name: str
) -> tuple[float, float] | None:
    """Coordinates from a finished review, or null."""
    entry = _read(path).get(name)
    if not isinstance(entry, dict):
        return None
    review = entry.get("review")
    if not _complete(review):
        return None
    latitude = review.get("latitude")
    longitude = review.get("longitude")
    if latitude is not None and longitude is not None:
        return float(latitude), float(longitude)
    place_id = review.get("place_id")
    for candidate in entry.get("candidates") or []:
        if candidate.get("id") != place_id:
            continue
        lat = candidate.get("latitude")
        lon = candidate.get("longitude")
        if lat is None or lon is None:
            return None
        return float(lat), float(lon)
    return None


def pending_names(path: Path) -> list[str]:
    """Canonical names whose review is not finished."""
    if not path.is_file():
        return []
    pending: list[str] = []
    for name, entry in _read(path).items():
        if not isinstance(entry, dict):
            pending.append(name)
            continue
        if entry.get("status") != "needs_review":
            continue
        if _complete(entry.get("review")) and reviewed_point(path, name):
            continue
        pending.append(name)
    return pending


def require_station_reviews(data_dir: Path, city_id: str) -> None:
    """Abort when any station or airport pin is still unreviewed."""
    pending = pending_names(review_path(data_dir, city_id))
    if not pending:
        return
    listed = ", ".join(pending)
    raise SystemExit(f"unreviewed places: {listed}")


def _complete(review: Any) -> bool:
    if not isinstance(review, dict):
        return False
    note = str(review.get("note") or "").strip()
    reviewed_at = str(review.get("reviewed_at") or "").strip()
    if not note or not reviewed_at:
        return False
    place_id = str(review.get("place_id") or "").strip()
    has_point = (
        review.get("latitude") is not None
        and review.get("longitude") is not None
    )
    return bool(place_id) or has_point


def _read(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"review store {path} is not an object")
    return payload


def _write(path: Path, store: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(store, indent=2, sort_keys=True) + "\n")

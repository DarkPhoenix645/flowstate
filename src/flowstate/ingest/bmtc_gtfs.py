"""BMTC GTFS zip loader."""

from __future__ import annotations

from flowstate.config import Settings


def run(settings: Settings) -> None:
    dest = settings.staged_dir / "bmtc_gtfs"
    dest.mkdir(parents=True, exist_ok=True)
    print(f"TODO: parse GTFS zip → {dest}")

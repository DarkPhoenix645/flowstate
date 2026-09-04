"""BMRCL HTML ridership reports → tidy parquet."""

from __future__ import annotations

from flowstate.config import Settings


def run(settings: Settings) -> None:
    dest = settings.staged_dir / "bmrc_ridership"
    dest.mkdir(parents=True, exist_ok=True)
    print(f"TODO: scrape BMRCL ridership HTML → {dest}")

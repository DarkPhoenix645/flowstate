"""BMTC GTFS. Local feed copy, checked not re-downloaded."""

from __future__ import annotations

import logging

from flowstate.config import Settings

log = logging.getLogger("ingest.bmtc")


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "bmtc" / "feed_info.txt"
    if not path.exists():
        raise SystemExit(f"ingest:bus missing {path}")
    log.info(
        "BMTC feed present at %s. Parquet staging is not implemented.",
        path.parent,
    )

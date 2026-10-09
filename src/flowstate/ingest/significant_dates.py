"""Bengaluru significant-dates file. Local copy, not a download."""

from __future__ import annotations

import logging

from flowstate.config import Settings

log = logging.getLogger("ingest.events")


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "significant-dates" / "significant_dates.csv"
    if not path.exists():
        raise SystemExit(f"ingest:events missing {path}")
    log.info("significant dates present at %s", path)

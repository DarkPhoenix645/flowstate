"""Namma Metro daily ridership. Local tracker CSV, checked not re-downloaded."""

from __future__ import annotations

import logging

from flowstate.config import Settings

log = logging.getLogger("ingest.namma")


def run(settings: Settings) -> None:
    path = (
        settings.datasets_dir
        / "namma-metro"
        / "NammaMetro_Ridership_Processed.csv"
    )
    if not path.exists():
        raise SystemExit(f"ingest:metro missing {path}")
    log.info(
        "metro daily file present at %s. Parquet staging is not implemented.",
        path,
    )

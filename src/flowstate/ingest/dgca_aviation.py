"""DGCA domestic city-pair table. Local copy, checked not re-downloaded."""

from __future__ import annotations

import logging

from flowstate.config import Settings

log = logging.getLogger("ingest.dgca")


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "aviation" / "domestic" / "city.csv"
    if not path.exists():
        raise SystemExit(f"ingest:aviation missing {path}")
    log.info(
        "DGCA city pairs present at %s. Parquet staging is not implemented.",
        path,
    )

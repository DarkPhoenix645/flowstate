"""Namma Metro daily ridership from the public tracker repository."""

from __future__ import annotations

import logging
import time

from flowstate.config import Settings
from flowstate.ingest.github import fetch_repo_file

log = logging.getLogger("ingest.namma")

REPO = "thecont1/namma-metro-ridership-tracker"
PATH = "NammaMetro_Ridership_Processed.csv"


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "namma-metro" / PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    payload = fetch_repo_file(REPO, PATH)
    path.write_bytes(payload)
    log.info(
        "TIMING series=metro_daily wall_s=%.3f bytes=%s",
        time.perf_counter() - t0,
        path.stat().st_size,
    )

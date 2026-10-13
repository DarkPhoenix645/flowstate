"""Namma Metro daily ridership from the public tracker repository."""

from __future__ import annotations

import logging
import time

import requests

from flowstate.config import Settings

log = logging.getLogger("ingest.namma")

URL = (
    "https://raw.githubusercontent.com/thecont1/"
    "namma-metro-ridership-tracker/main/"
    "NammaMetro_Ridership_Processed.csv"
)


def run(settings: Settings) -> None:
    path = (
        settings.datasets_dir
        / "namma-metro"
        / "NammaMetro_Ridership_Processed.csv"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("GET %s", URL)
    t0 = time.perf_counter()
    response = requests.get(URL, timeout=180)
    response.raise_for_status()
    path.write_bytes(response.content)
    log.info(
        "TIMING series=metro_daily wall_s=%.3f bytes=%s",
        time.perf_counter() - t0,
        path.stat().st_size,
    )

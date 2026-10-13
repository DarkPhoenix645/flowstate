"""DGCA domestic city-pair table from Vonter/india-aviation-traffic."""

from __future__ import annotations

import logging
import time

import requests

from flowstate.config import Settings

log = logging.getLogger("ingest.dgca")

URL = (
    "https://raw.githubusercontent.com/Vonter/india-aviation-traffic/"
    "main/aggregated/domestic/city.csv"
)


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "aviation" / "domestic" / "city.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    log.info("GET %s", URL)
    t0 = time.perf_counter()
    response = requests.get(URL, timeout=180)
    response.raise_for_status()
    path.write_bytes(response.content)
    log.info(
        "TIMING series=dgca_city wall_s=%.3f bytes=%s",
        time.perf_counter() - t0,
        path.stat().st_size,
    )

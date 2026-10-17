"""DGCA domestic city-pair table from Vonter/india-aviation-traffic."""

from __future__ import annotations

import logging
import time

from flowstate.config import Settings
from flowstate.ingest.github import fetch_repo_file

log = logging.getLogger("ingest.dgca")

REPO = "Vonter/india-aviation-traffic"
PATH = "aggregated/domestic/city.csv"


def run(settings: Settings) -> None:
    path = settings.datasets_dir / "aviation" / "domestic" / "city.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    payload = fetch_repo_file(REPO, PATH)
    path.write_bytes(payload)
    log.info(
        "TIMING series=dgca_city wall_s=%.3f bytes=%s",
        time.perf_counter() - t0,
        path.stat().st_size,
    )

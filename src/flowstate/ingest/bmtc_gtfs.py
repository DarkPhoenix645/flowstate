"""BMTC GTFS feed from Vonter/bmtc-gtfs (gtfs/bmtc.zip)."""

from __future__ import annotations

import io
import logging
import time
import zipfile
from pathlib import Path

import requests

from flowstate.config import Settings

log = logging.getLogger("ingest.bmtc")

URL = (
    "https://raw.githubusercontent.com/Vonter/bmtc-gtfs/main/gtfs/bmtc.zip"
)


def run(settings: Settings) -> None:
    dest = settings.datasets_dir / "bmtc"
    dest.mkdir(parents=True, exist_ok=True)
    log.info("GET %s", URL)
    t0 = time.perf_counter()
    response = requests.get(URL, timeout=300)
    response.raise_for_status()
    written = _extract_txt(dest, response.content)
    log.info(
        "TIMING series=bmtc_gtfs wall_s=%.3f zip_bytes=%s txt_files=%s",
        time.perf_counter() - t0,
        len(response.content),
        written,
    )


def _extract_txt(dest: Path, payload: bytes) -> int:
    count = 0
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for info in archive.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".txt"):
                continue
            target = dest / Path(info.filename).name
            target.write_bytes(archive.read(info))
            count += 1
    if not (dest / "feed_info.txt").exists():
        raise SystemExit("ingest:bus zip has no feed_info.txt")
    return count

"""BMTC GTFS feed from Vonter/bmtc-gtfs (gtfs/bmtc.zip)."""

from __future__ import annotations

import io
import logging
import time
import zipfile
from pathlib import Path

from flowstate.config import Settings
from flowstate.ingest.github import fetch_repo_file

log = logging.getLogger("ingest.bmtc")

REPO = "Vonter/bmtc-gtfs"
PATH = "gtfs/bmtc.zip"


def run(settings: Settings) -> None:
    dest = settings.datasets_dir / "bmtc"
    dest.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    payload = fetch_repo_file(REPO, PATH)
    written = _extract_txt(dest, payload)
    log.info(
        "TIMING series=bmtc_gtfs wall_s=%.3f zip_bytes=%s txt_files=%s",
        time.perf_counter() - t0,
        len(payload),
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

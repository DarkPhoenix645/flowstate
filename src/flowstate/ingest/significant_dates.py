"""Bengaluru significant dates from Kaggle dataset version 13."""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path

import kagglehub

from flowstate.config import Settings

log = logging.getLogger("ingest.events")

HANDLE = "maheshshantaram/significant-dates-in-bangalore-india"


def run(settings: Settings) -> None:
    dest_dir = settings.datasets_dir / "significant-dates"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "significant_dates.csv"
    log.info("GET kaggle %s", HANDLE)
    t0 = time.perf_counter()
    downloaded = Path(
        kagglehub.dataset_download(
            HANDLE,
            force_download=True,
            output_dir=str(dest_dir / "_kaggle"),
        )
    )
    source = _find_csv(downloaded)
    shutil.copyfile(source, dest)
    log.info(
        "TIMING series=significant_dates wall_s=%.3f bytes=%s file=%s",
        time.perf_counter() - t0,
        dest.stat().st_size,
        source.name,
    )


def _find_csv(root: Path) -> Path:
    if root.is_file() and root.suffix == ".csv":
        return root
    csvs = sorted(root.rglob("*.csv"))
    if not csvs:
        raise SystemExit(f"ingest:events no csv under {root}")
    named = [p for p in csvs if p.name == "significant_dates.csv"]
    return named[0] if named else csvs[0]

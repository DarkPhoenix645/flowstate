"""Timed Spark vs MapReduce rows. The report owns plots."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

RESULTS = Path("report/results/timings.csv")
COLUMNS = (
    "run_id",
    "job",
    "engine",
    "corpus_rows",
    "seed",
    "backend",
    "elapsed_seconds",
    "grain",
)
# One grain per job. Spark and MapReduce share it.
GRAIN = {
    "demand_hotspots": "ward_id,hour_of_day,day_type",
    "cancellation_analysis": "ward_id,hour_of_day,day_type,vehicle_type",
    "ridership_trends": "day|month|station_peak",
    "cross_modal": "ridehail_metro:day|month;flights_ridehail:covered_day",
}


def append_timing(row: dict[str, Any], path: Path = RESULTS) -> None:
    """Append one plot-ready row. The header is written once."""
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.is_file()
    with path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        if new:
            writer.writeheader()
        writer.writerow({key: row[key] for key in COLUMNS})


def record_timing(
    job: str,
    params: dict[str, Any],
    elapsed: float,
    backend: str = "compose",
    path: Path = RESULTS,
) -> None:
    """Store one 1M comparison. The benchmark row points at the run."""
    from flowstate.config import get_settings
    from flowstate.config.postgres import ConfigStore

    seconds = round(float(elapsed), 3)
    ConfigStore(get_settings().config_database_url).insert_benchmark(
        run_id=str(params["run_id"]),
        engine=str(params["engine"]),
        corpus_rows=int(params["corpus_rows"]),
        seed=int(params["seed"]),
        elapsed_seconds=seconds,
    )
    append_timing(
        {
            "run_id": params["run_id"],
            "job": job,
            "engine": params["engine"],
            "corpus_rows": int(params["corpus_rows"]),
            "seed": int(params["seed"]),
            "backend": backend,
            "elapsed_seconds": seconds,
            "grain": GRAIN[job],
        },
        path,
    )

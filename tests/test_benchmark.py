"""#26 timing rows. The cluster is not required."""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from flowstate.benchmarks.runner import COLUMNS, GRAIN, append_timing
from flowstate.config.resolve import ConfigValidationError
from flowstate.jobs.registry import validate_job


def test_a_benchmark_run_is_one_of_the_four_jobs():
    names = (
        "demand_hotspots",
        "cancellation_analysis",
        "ridership_trends",
        "cross_modal",
    )
    for name in names:
        assert validate_job("benchmark", name, {}).name == name
        assert name in GRAIN


def test_other_jobs_are_not_benchmarks():
    with pytest.raises(ConfigValidationError):
        validate_job("benchmark", "hdfs_probe", {})
    with pytest.raises(ConfigValidationError):
        validate_job("benchmark", "scenario_effect", {"replay_run_id": "x"})


def test_timings_csv_keeps_one_header(tmp_path: Path):
    path = tmp_path / "results" / "timings.csv"
    row = {
        "run_id": "run-1",
        "job": "demand_hotspots",
        "engine": "spark",
        "corpus_rows": 1_000_000,
        "seed": 42,
        "backend": "compose",
        "elapsed_seconds": 1.5,
        "grain": GRAIN["demand_hotspots"],
    }
    append_timing(row, path)
    row["engine"] = "mapreduce"
    row["run_id"] = "run-2"
    append_timing(row, path)
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert list(rows[0]) == list(COLUMNS)
    assert rows[0]["run_id"] == "run-1"
    assert rows[1]["engine"] == "mapreduce"
    assert rows[0]["grain"] == rows[1]["grain"] == GRAIN["demand_hotspots"]
    assert rows[0]["corpus_rows"] == "1000000"
    assert rows[0]["seed"] == "42"


def test_the_makefile_has_no_benchmark_target():
    text = Path("Makefile").read_text()
    assert "benchmark" not in text

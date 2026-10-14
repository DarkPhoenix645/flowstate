"""DGCA monthly Bengaluru city pairs. No Kafka topic. No daily rows."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import pyarrow as pa

from flowstate.config import Settings
from flowstate.provenance import require_valid
from flowstate.staged import staged_parquet_path, write_parquet

BENGALURU = "BENGALURU"


def city_month_table(frame: pd.DataFrame) -> tuple[pa.Table, dict[str, Any]]:
    """Keep each ordered pair. Do not add a row to its reverse."""
    blr = frame[
        (frame["City1"] == BENGALURU) | (frame["City2"] == BENGALURU)
    ].copy()
    blr["year_month"] = (
        blr["Year"].astype(int).astype(str)
        + "-"
        + blr["Month"].astype(int).astype(str).str.zfill(2)
    )
    blr["direction"] = (
        blr["City1"].astype(str) + "->" + blr["City2"].astype(str)
    )
    blr = blr.sort_values(
        ["year_month", "City1", "City2"]
    ).reset_index(drop=True)
    keys = ["Year", "Month", "City1", "City2"]
    duplicate_pair_months = int(blr.duplicated(keys).sum())
    out = pd.DataFrame(
        {
            "year": blr["Year"].astype(int),
            "month": blr["Month"].astype(int),
            "city1": blr["City1"].astype(str),
            "city2": blr["City2"].astype(str),
            "year_month": blr["year_month"].astype(str),
            "direction": blr["direction"].astype(str),
            "pax_to_city2": blr["PaxToCity2"].astype(float),
            "pax_from_city2": blr["PaxFromCity2"].astype(float),
            "freight_to_city2": blr["FreightToCity2"].astype(float),
            "freight_from_city2": blr["FreightFromCity2"].astype(float),
            "mail_to_city2": blr["MailToCity2"].astype(float),
            "mail_from_city2": blr["MailFromCity2"].astype(float),
        }
    )
    table = pa.Table.from_pandas(out, preserve_index=False)
    months = sorted(out["year_month"].unique())
    shared = [
        month
        for month in months
        if "2024-10" <= month <= "2026-04"
    ]
    pairs = set(zip(blr["City1"], blr["City2"], strict=True))
    reverses = sum(
        1 for left, right in pairs if left != right and (right, left) in pairs
    )
    coverage = {
        "rows": len(out),
        "month_min": months[0] if months else None,
        "month_max": months[-1] if months else None,
        "alignment_months": len(shared),
        "ordered_pairs_with_reverse": reverses,
        "duplicate_pair_months": duplicate_pair_months,
        "veracity": [
            "One row is one ordered City1 -> City2 month.",
            "Do not add that row to the swapped pair.",
            "Some pair-months are repeated in the file. Both rows are kept. "
            "Do not add the repeats until that split is explained.",
            "Do not expand a month into days.",
            "Passengers are not flight counts.",
            "There is no DGCA Kafka topic.",
        ],
    }
    return table, coverage


def stage_dgca(settings: Settings) -> dict[str, Any]:
    require_valid(["dgca_domestic_city"])
    frame = pd.read_csv(
        settings.datasets_dir / "aviation" / "domestic" / "city.csv"
    )
    table, coverage = city_month_table(frame)
    path = staged_parquet_path(settings, "dgca", "city_month")
    write_parquet(path, table)
    path.with_suffix(".coverage.json").write_text(
        json.dumps(coverage, indent=2) + "\n"
    )
    return coverage

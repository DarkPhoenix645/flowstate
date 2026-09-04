"""data.gov.in MMRDA ridership XLSX → parquet."""

from __future__ import annotations

from flowstate.config import Settings


def run(settings: Settings) -> None:
    dest = settings.staged_dir / "mmrda_ridership"
    dest.mkdir(parents=True, exist_ok=True)
    print(f"TODO: parse MMRDA XLSX → {dest}")

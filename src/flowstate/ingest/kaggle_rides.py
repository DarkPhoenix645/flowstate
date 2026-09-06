"""kagglehub downloaders for ride-hailing CSVs (datasets #1, #2, #7)."""

from __future__ import annotations

from flowstate.config import Settings


def run(settings: Settings) -> None:
    dest = settings.staged_dir / "kaggle_rides"
    dest.mkdir(parents=True, exist_ok=True)
    print(
        f"TODO: kagglehub download → standardized parquet at {dest} "
        "(needs KAGGLE_API_TOKEN)"
    )

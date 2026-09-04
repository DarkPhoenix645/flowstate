"""Demand heatmaps by zone/hour. Submit on the Compose Spark cluster."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(
        f"TODO: demand hotspots from {settings.staged_dir} "
        f"(master={settings.spark_master})"
    )


if __name__ == "__main__":
    main()

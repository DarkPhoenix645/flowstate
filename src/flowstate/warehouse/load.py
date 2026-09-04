"""Load staged parquet into Hive."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(f"TODO: PARTITIONED load into Hive from {settings.lake_dir}")


if __name__ == "__main__":
    main()

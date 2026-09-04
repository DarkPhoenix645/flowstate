"""Cancellation root-cause (driver vs customer, by time/zone/vehicle)."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(f"TODO: cancellation analysis from {settings.staged_dir}")


if __name__ == "__main__":
    main()

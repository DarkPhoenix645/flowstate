"""Metro/aviation ridership trends + payment split."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(f"TODO: ridership trends from {settings.staged_dir}")


if __name__ == "__main__":
    main()

"""Metro ridership vs ride-hailing demand correlation."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(f"TODO: cross-modal correlation from {settings.staged_dir}")


if __name__ == "__main__":
    main()

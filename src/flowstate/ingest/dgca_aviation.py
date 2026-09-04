"""DGCA aviation stats loader."""

from __future__ import annotations

from flowstate.config import Settings


def run(settings: Settings) -> None:
    dest = settings.staged_dir / "dgca_aviation"
    dest.mkdir(parents=True, exist_ok=True)
    print(f"TODO: parse DGCA aviation tables → {dest}")

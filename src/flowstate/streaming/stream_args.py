"""Arguments for a stream run. The API passes them to spark-submit."""

from __future__ import annotations

import sys


def parse_stream_args(argv: list[str] | None = None) -> dict[str, str]:
    found: dict[str, str] = {}
    for item in argv if argv is not None else sys.argv[1:]:
        if not item.startswith("--") or "=" not in item:
            raise SystemExit(f"bad argument {item}")
        key, value = item[2:].split("=", 1)
        found[key] = value
    for key in ("trigger", "max-offsets", "kafka", "sink", "checkpoint"):
        if key not in found:
            raise SystemExit(f"missing --{key}")
    if found["trigger"] not in ("live", "replay"):
        raise SystemExit("trigger must be live or replay")
    offsets = int(found["max-offsets"])
    if offsets < 10_000 or offsets > 30_000:
        raise SystemExit("max-offsets is 10000 to 30000")
    return found

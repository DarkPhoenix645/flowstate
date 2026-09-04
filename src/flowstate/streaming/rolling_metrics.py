"""Structured Streaming rolling windows (submit via Compose Spark)."""

from __future__ import annotations

from flowstate.config import get_settings


def main() -> None:
    settings = get_settings()
    print(
        "TODO: 1-min tumbling + 5-min sliding windows, "
        f"watermark 10m; sink → {settings.stream_sink_dir}"
    )


if __name__ == "__main__":
    main()

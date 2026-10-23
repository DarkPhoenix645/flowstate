"""Active rides / zone. One Event-Detector node. Submit via make stream-consume.

Cancel rate and surge are other files. Layout: docs/agents/event-detector.md.
"""

from __future__ import annotations


def main() -> None:
    print(
        "TODO #22: active rides / zone only. "
        "Read flowstate.ridehail.activity.v1. "
        "Sink hdfs://namenode:9000/stream-sink/active_rides/. "
        "Checkpoint hdfs://namenode:9000/checkpoints/active_rides/. "
        "Policy: docs/agents/watermarks.md."
    )


if __name__ == "__main__":
    main()

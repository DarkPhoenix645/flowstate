"""Active rides / zone. One Event-Detector node. Start through the API.

Cancel rate and surge are other files. Layout: docs/agents/event-detector.md.
"""

from __future__ import annotations

import logging

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session, write_query
from flowstate.streaming.metrics import active_counts

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    args = load_args()
    spark = session("flowstate-active-rides")
    frame = active_counts(events(spark, args))
    query = write_query(frame, args, "active_rides")
    _log_sum(spark, args["sink"], query)
    spark.stop()


def _log_sum(spark, sink: str, query) -> None:
    if query.lastProgress is None:
        return
    try:
        total = (
            spark.read.parquet(sink)
            .agg({"activity_count": "sum"})
            .collect()
        )
    except Exception:
        log.info("active_rides sink is empty")
        return
    log.info("activity_count_sum %s", total[0][0])


if __name__ == "__main__":
    main()

"""Aviation archive. One Event-Detector node. Start through the API."""

from __future__ import annotations

import logging

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session, write_query
from flowstate.streaming.metrics import aviation_counts

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    args = load_args()
    spark = session("flowstate-aviation-activity")
    frame = aviation_counts(events(spark, args))
    write_query(frame, args, "aviation_activity")
    log.info("aviation_activity wrote %s", args["sink"])
    spark.stop()


if __name__ == "__main__":
    main()

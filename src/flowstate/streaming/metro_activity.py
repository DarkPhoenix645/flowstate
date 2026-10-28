"""Metro archive. One Event-Detector node. Start through the API."""

from __future__ import annotations

import logging

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session, write_query
from flowstate.streaming.metrics import metro_counts

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    args = load_args()
    spark = session("flowstate-metro-activity")
    write_query(metro_counts(events(spark, args)), args, "metro_activity")
    log.info("metro_activity wrote %s", args["sink"])
    spark.stop()


if __name__ == "__main__":
    main()

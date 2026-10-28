"""Bus archive. One Event-Detector node. Start through the API."""

from __future__ import annotations

import logging

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session, write_query
from flowstate.streaming.metrics import bus_counts

log = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    args = load_args()
    spark = session("flowstate-bus-activity")
    write_query(bus_counts(events(spark, args)), args, "bus_activity")
    log.info("bus_activity wrote %s", args["sink"])
    spark.stop()


if __name__ == "__main__":
    main()

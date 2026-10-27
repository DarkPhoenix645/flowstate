"""Q1 demand. Ward x hour-of-day x day-type. Compose Spark only."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from flowstate.batch.spark_jobs.analytics_common import (
    clock,
    corpus,
    parse_args,
    session,
    stamp_run,
    window_of,
    write_json,
    write_partition,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)
TABLE = "analytics_demand_hotspots"


def main() -> None:
    configure_logging()
    args = parse_args()
    spark = session("flowstate-demand-hotspots")
    base = clock(corpus(spark)).where(F.col("mode") == "ride_hail")
    window = window_of(base)
    counts = base.groupBy(
        F.col("pickup_zone").alias("ward_id"), "hour_of_day", "day_type"
    ).agg(F.count("*").alias("demand_count"))
    path = write_partition(
        stamp_run(counts, args, window), TABLE, args["run-id"]
    )
    rows = [
        {
            "ward_id": row.ward_id,
            "hour_of_day": row.hour_of_day,
            "day_type": row.day_type,
            "demand_count": row.demand_count,
        }
        for row in counts.collect()
    ]
    write_json(
        spark,
        f"hdfs://namenode:9000/user/hive/warehouse/surge_baseline/"
        f"run_id={args['run-id']}/baseline.json",
        {
            "source": "hdfs://namenode:9000/lake/rides_amplified",
            "grain": "ward_id x hour_of_day x day_type",
            "bundle_version": args["bundle-version"],
            "seed": int(args["seed"]),
            "corpus_rows": int(args["corpus-rows"]),
            "scale": int(args["corpus-rows"]),
            "rows": rows,
        },
    )
    spark.stop()
    log.info("wrote %s", path)


if __name__ == "__main__":
    main()

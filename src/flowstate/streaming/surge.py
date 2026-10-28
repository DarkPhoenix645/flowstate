"""Surge. Hour z-score against the #18 baseline. Ride-hail topic only."""

from __future__ import annotations

import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from flowstate.log import configure_logging
from flowstate.streaming.job import events, load_args, session
from flowstate.streaming.metrics import hour_demand, minute_bookings
from flowstate.streaming.surge_score import (
    day_type_days,
    expected_hour,
    poisson_z,
)

log = logging.getLogger(__name__)
_HOUR_SCHEMA = StructType(
    [
        StructField("hour_start", TimestampType(), True),
        StructField("ward_id", StringType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_type", StringType(), True),
        StructField("scenario_id", StringType(), True),
        StructField("evidence_grade", StringType(), True),
        StructField("demand", LongType(), True),
        StructField("expected", DoubleType(), True),
        StructField("z_score", DoubleType(), True),
    ]
)
BASE = (
    "hdfs://namenode:9000/user/hive/warehouse/"
    "analytics_demand_hotspots"
)


def main() -> None:
    configure_logging()
    args = load_args()
    if "baseline-run-id" not in args:
        raise SystemExit("missing --baseline-run-id")
    spark = session("flowstate-surge", one_core=True)
    baseline = _baseline(spark, args["baseline-run-id"])
    minutes = minute_bookings(events(spark, args))

    def write_hour(batch, epoch_id: int) -> None:  # noqa: ANN001
        del epoch_id
        if batch.isEmpty():
            return
        rows = []
        for item in hour_demand(batch).collect():
            stamp = item.hour_start
            if not isinstance(stamp, datetime):
                continue
            day_type = "weekday" if stamp.weekday() <= 4 else "weekend"
            expected = baseline.get((item.ward_id, stamp.hour, day_type))
            score = None
            if expected is not None:
                score = poisson_z(item.demand, expected)
            rows.append(
                (
                    stamp,
                    item.ward_id,
                    stamp.hour,
                    day_type,
                    item.scenario_id,
                    item.evidence_grade,
                    int(item.demand),
                    expected,
                    score,
                )
            )
        if not rows:
            return
        out = spark.createDataFrame(rows, _HOUR_SCHEMA)
        out.write.mode("append").parquet(args["sink"])

    writer = (
        minutes.writeStream.foreachBatch(write_hour)
        .option("checkpointLocation", args["checkpoint"])
        .outputMode("append")
        .queryName("surge")
    )
    if args["trigger"] == "live":
        writer = writer.trigger(processingTime="30 seconds")
    else:
        writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()
    log.info("surge wrote %s", args["sink"])
    spark.stop()


def _baseline(spark: SparkSession, run_id: str) -> dict[tuple, float]:
    path = f"{BASE}/run_id={run_id}"
    frame = spark.read.parquet(path)
    window = frame.select(
        F.min("window_start").alias("lo"), F.max("window_end").alias("hi")
    ).collect()[0]
    start = _as_date(window.lo)
    end = _as_date(window.hi)
    found: dict[tuple, float] = {}
    for row in frame.select(
        "ward_id", "hour_of_day", "day_type", "demand_count"
    ).collect():
        days = day_type_days(start, end, row.day_type)
        found[(row.ward_id, int(row.hour_of_day), row.day_type)] = (
            expected_hour(row.demand_count, days)
        )
    if not found:
        raise SystemExit("baseline has no rows")
    log.info("baseline rows %s", len(found))
    return found


def _as_date(value):
    if isinstance(value, datetime):
        return value.date()
    return datetime.fromisoformat(str(value)[:10]).date()


if __name__ == "__main__":
    main()

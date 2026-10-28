"""Q6. Baseline against fork, in every mode. Not a learned effect."""

from __future__ import annotations

import logging
import os
import sys

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from flowstate.batch.effect import hour_of, newest_replay_id, pair
from flowstate.batch.spark_jobs.analytics_common import (
    exists,
    parse_args,
    session,
    stamp_run,
    write_partition,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)
ROOT = "hdfs://namenode:9000/stream-sink"
SINKS = {
    "ride_hail": ("active_rides", "ward_id", "activity_count"),
    "metro": ("metro_activity", "station_id", "entries_count"),
    "bus": ("bus_activity", "stop_id", "boardings"),
    "aviation": ("aviation_activity", "airport", "event_count"),
}


def main() -> None:
    configure_logging()
    args = parse_args()
    replay = _flag("replay-run-id")
    spark = session("flowstate-scenario-effect")
    spark.sparkContext.setLogLevel("WARN")
    frames = []
    rates = None
    window = None
    for mode, (folder, place, value) in SINKS.items():
        path = f"{ROOT}/{folder}"
        if not exists(spark, path):
            _fail(spark, f"missing sink {folder}")
        frame = _one_replay(spark, spark.read.parquet(path), replay)
        window = _widen(window, _span(frame))
        if mode == "metro" and "exits_count" in frame.columns:
            frame = frame.withColumn(
                "entries_count",
                F.col("entries_count") + F.col("exits_count"),
            )
        hourly = hour_of(frame, place, value)
        if mode != "aviation" and "direction" not in hourly.columns:
            hourly = hourly.withColumn("direction", F.lit(None).cast("string"))
        frames.append(pair(hourly, mode))
        if mode == "ride_hail":
            rates = _cancel_rates(spark, replay)
    forks = set()
    for frame in frames:
        ids = frame.select("scenario_id").distinct().collect()
        forks.update(row.scenario_id for row in ids if row.scenario_id)
    if not forks:
        _fail(spark, "there is no fork")
    if len(forks) > 1:
        _fail(spark, "a replay accepts one scenario_id besides null")
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame)
    if rates is not None:
        out = out.join(rates, ["mode", "place_id", "hour_of_day"], "left")
    else:
        out = out.withColumn(
            "cancel_rate_baseline", F.lit(None).cast("double")
        ).withColumn("cancel_rate_fork", F.lit(None).cast("double"))
    out = out.drop("scenario_id").withColumn(
        "replay_run_id", F.lit(replay)
    )
    if window is None:
        _fail(spark, "there is no fork")
    path = write_partition(
        stamp_run(out, args, window),
        "analytics_scenario_effect",
        args["run-id"],
    )
    log.info("wrote %s", path)
    spark.stop()


def _one_replay(spark, frame: DataFrame, replay: str) -> DataFrame:
    """Keep the newest epoch of one API replay. Do not mix sessions."""
    if "source_run_id" not in frame.columns:
        _fail(spark, "sink has no source_run_id")
    found = [
        row.source_run_id
        for row in frame.select("source_run_id").distinct().collect()
        if row.source_run_id
    ]
    chosen = newest_replay_id(found, replay)
    if chosen is None:
        return frame.where(F.lit(False))
    return frame.where(F.col("source_run_id") == chosen)


def _span(frame: DataFrame) -> tuple[str, str] | None:
    row = frame.select(
        F.min(F.to_date("window_start")).alias("lo"),
        F.max(F.to_date("window_start")).alias("hi"),
    ).collect()[0]
    if row.lo is None or row.hi is None:
        return None
    return (str(row.lo), str(row.hi))


def _widen(
    current: tuple[str, str] | None, nxt: tuple[str, str] | None
) -> tuple[str, str] | None:
    if nxt is None:
        return current
    if current is None:
        return nxt
    return (min(current[0], nxt[0]), max(current[1], nxt[1]))


def _fail(spark, message: str) -> None:
    """Stop Spark, then write stdout. The runner appends stdout last."""
    try:
        spark.sparkContext.setLogLevel("OFF")
        spark.stop()
    finally:
        os.write(1, (message + "\n").encode())
        os._exit(1)


def _cancel_rates(spark, replay: str) -> DataFrame | None:
    path = f"{ROOT}/cancel_rate"
    if not exists(spark, path):
        return None
    frame = spark.read.parquet(path)
    if "source_run_id" not in frame.columns:
        return None
    frame = _one_replay(spark, frame, replay)
    hourly = hour_of(frame, "ward_id", "cancels").withColumnRenamed(
        "value", "cancels"
    )
    activity = hour_of(frame, "ward_id", "activity_count")
    joined = hourly.join(
        activity.select("hour_start", "place_id", "scenario_id", "value"),
        ["hour_start", "place_id", "scenario_id"],
        "left",
    )
    base = joined.where(F.col("scenario_id").isNull())
    fork = joined.where(F.col("scenario_id").isNotNull())
    both = base.alias("b").join(
        fork.alias("f"), ["hour_start", "place_id"], "full"
    )
    return both.select(
        F.lit("ride_hail").alias("mode"),
        F.coalesce(F.col("b.place_id"), F.col("f.place_id")).alias("place_id"),
        F.hour(
            F.coalesce(F.col("b.hour_start"), F.col("f.hour_start"))
        ).alias("hour_of_day"),
        _rate("b").alias("cancel_rate_baseline"),
        _rate("f").alias("cancel_rate_fork"),
    )


def _rate(side: str):
    cancels = F.col(f"{side}.cancels")
    total = F.col(f"{side}.value")
    return F.when(total.isNull() | (total == 0), F.lit(None)).otherwise(
        cancels / total
    )


def _flag(name: str) -> str:
    prefix = f"--{name}="
    for item in sys.argv[1:]:
        if item.startswith(prefix):
            return item[len(prefix) :]
    raise SystemExit(f"missing --{name}")


if __name__ == "__main__":
    main()

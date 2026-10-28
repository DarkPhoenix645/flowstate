"""Pair baseline and fork archive rows. The difference is not learned."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ASSUMPTION = "assumption_driven"


def newest_replay_id(ids: list[str], replay: str) -> str | None:
    """Newest envelope ``run_id`` for one API replay session.

    The envelope value is ``<session>.<epoch>``. The API id is the
    session. A value with no epoch sorts before ``session.0``.
    """
    matched = [
        item
        for item in ids
        if item == replay or item.startswith(f"{replay}.")
    ]
    if not matched:
        return None

    def epoch(item: str) -> int:
        if item == replay:
            return -1
        suffix = item[len(replay) + 1 :]
        if not suffix.isdigit():
            return -1
        return int(suffix)

    return max(matched, key=epoch)


def hour_of(frame: DataFrame, place: str, value: str) -> DataFrame:
    """Sum a 1-minute archive onto the session hour.

    The analytics session clock is Asia/Kolkata. ``window_start`` is
    already a timestamp, so a second zone shift would move the hour.
    """
    stamped = frame.withColumn(
        "hour_start", F.date_trunc("hour", F.col("window_start"))
    )
    keys = ["hour_start", place, "scenario_id"]
    if "direction" in frame.columns:
        keys.append("direction")
    if "source_run_id" in frame.columns:
        keys.append("source_run_id")
    graded = stamped
    if "evidence_grade" not in stamped.columns:
        graded = stamped.withColumn("evidence_grade", F.lit(ASSUMPTION))
    return (
        graded.groupBy(*keys)
        .agg(
            F.sum(F.col(value)).alias("value"),
            F.max("evidence_grade").alias("evidence_grade"),
        )
        .withColumnRenamed(place, "place_id")
    )


def pair(frame: DataFrame, mode: str) -> DataFrame:
    """One row per place-hour. Fork minus baseline. No learned label."""
    base = frame.where(F.col("scenario_id").isNull()).alias("b")
    fork = frame.where(F.col("scenario_id").isNotNull()).alias("f")
    keys = ["hour_start", "place_id"]
    if mode == "aviation":
        keys.append("direction")
    if "source_run_id" in frame.columns:
        keys.append("source_run_id")
    joined = base.join(fork, keys, "full")
    hour = F.coalesce(F.col("b.hour_start"), F.col("f.hour_start"))
    place = F.coalesce(F.col("b.place_id"), F.col("f.place_id"))
    baseline = F.coalesce(F.col("b.value"), F.lit(0.0))
    forked = F.coalesce(F.col("f.value"), F.lit(0.0))
    direction = (
        F.coalesce(F.col("b.direction"), F.col("f.direction"))
        if "direction" in frame.columns
        else F.lit(None).cast("string")
    )
    grade = (
        F.lit(ASSUMPTION)
        if mode in ("metro", "aviation")
        else F.coalesce(
            F.col("f.evidence_grade"),
            F.col("b.evidence_grade"),
            F.lit(ASSUMPTION),
        )
    )
    return joined.select(
        F.lit(mode).alias("mode"),
        place.alias("place_id"),
        F.hour(hour).alias("hour_of_day"),
        direction.alias("direction"),
        baseline.alias("baseline_value"),
        forked.alias("fork_value"),
        (forked - baseline).alias("difference"),
        F.when(baseline == 0, F.lit(None)).otherwise(forked / baseline).alias(
            "ratio"
        ),
        grade.alias("evidence_grade"),
        F.col("f.scenario_id").alias("scenario_id"),
    )

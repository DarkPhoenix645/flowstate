"""Check that lake corpus children match the amplify manifest.

Submit on Compose Spark. Do not import pydantic. Write
``rollup_conservation`` only when every check passes. A mismatch
leaves the warehouse path absent so the lake put is not complete.
"""

from __future__ import annotations

import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from flowstate.batch.conservation import compare

NAMENODE = "hdfs://namenode:9000"
CORPUS = f"{NAMENODE}/lake/rides_amplified"
MANIFEST = f"{CORPUS}/_meta/manifest.json"
ROLLUP = f"{NAMENODE}/user/hive/warehouse/rollup_conservation"

_RIDE_METRIC = {
    "booking_created": "booking",
    "completed_ride": "done_ride",
    "cancelled_ride": "cancel_ride",
}
_AIR_METRIC = {
    "observed_proxy": "observed_rows",
    "synthetic": "synthetic_rows",
}

_SCHEMA = StructType(
    [
        StructField("seed", LongType(), False),
        StructField("bundle_version", StringType(), False),
        StructField("rows", LongType(), False),
        StructField("checks", LongType(), False),
        StructField("failures", LongType(), False),
        StructField("ok", BooleanType(), False),
    ]
)


def main() -> None:
    spark = SparkSession.builder.appName(
        "flowstate-conservation-check"
    ).getOrCreate()
    manifest = json.loads(
        spark.sparkContext.wholeTextFiles(MANIFEST).values().first()
    )
    frame = spark.read.option("pathGlobFilter", "part-*.parquet").parquet(
        CORPUS
    )
    children = _children(frame)
    checks, issues = compare(
        manifest["parents"],
        children,
        rows=children["rows"],
        expected_rows=int(manifest["rows"]),
    )
    if issues:
        for issue in issues:
            print(issue, file=sys.stderr)
        spark.stop()
        raise SystemExit(1)
    row = [
        (
            int(manifest["seed"]),
            str(manifest["bundle_version"]),
            int(manifest["rows"]),
            int(checks),
            0,
            True,
        )
    ]
    spark.createDataFrame(row, schema=_SCHEMA).write.mode(
        "overwrite"
    ).parquet(ROLLUP)
    spark.stop()
    print(
        f"conservation ok checks={checks} rows={manifest['rows']} "
        f"seed={manifest['seed']}"
    )


def _children(frame):
    ride: dict[str, int] = {}
    ride_rows = (
        frame.filter(F.col("mode") == "ride_hail")
        .groupBy("parent_id", "event_type")
        .agg(F.count("*").alias("n"))
        .collect()
    )
    for row in ride_rows:
        metric = _RIDE_METRIC.get(row.event_type, row.event_type)
        ride[f"{row.parent_id}|{metric}"] = int(row.n)
    entries: dict[str, int] = {}
    exits: dict[str, int] = {}
    metro = (
        frame.filter(F.col("mode") == "metro")
        .groupBy("parent_id")
        .agg(
            F.sum(F.col("entries_count").cast("long")).alias("entries"),
            F.sum(F.col("exits_count").cast("long")).alias("exits"),
        )
        .collect()
    )
    for row in metro:
        entries[row.parent_id] = int(row.entries or 0)
        exits[row.parent_id] = int(row.exits or 0)
    bus: dict[str, int] = {}
    for row in (
        frame.filter(F.col("mode") == "bus")
        .groupBy("parent_id")
        .agg(
            F.sum(F.col("boardings").cast("long")).alias("boardings"),
            F.sum(F.col("alightings").cast("long")).alias("alightings"),
        )
        .collect()
    ):
        bus[f"{row.parent_id}|boardings"] = int(row.boardings or 0)
        bus[f"{row.parent_id}|alightings"] = int(row.alightings or 0)
    air: dict[str, int] = {}
    for row in (
        frame.filter(F.col("mode") == "aviation")
        .groupBy("parent_id", "provenance_status")
        .agg(F.count("*").alias("n"))
        .collect()
    ):
        kind = _AIR_METRIC.get(row.provenance_status, row.provenance_status)
        air[f"{row.parent_id}|{kind}"] = int(row.n)
    return {
        "rows": int(frame.count()),
        "ridehail": ride,
        "metro_entries": entries,
        "metro_exits": exits,
        "bus": bus,
        "aviation": air,
    }


if __name__ == "__main__":
    main()

"""Hive rides load. No Docker and no network."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pyarrow as pa

from flowstate.warehouse.load import (
    city_day_clock,
    hdfs_load_commands,
    partition_values,
)

ZONE = ZoneInfo("Asia/Kolkata")


def _city_day() -> pa.Table:
    return pa.table(
        {
            "date": ["2025-01-02", "2025-01-03"],
            "city": ["Bengaluru", "Bengaluru"],
            "booking": [10, None],
            "done_ride": [7, 0],
            "cancel_ride": [2, 1],
            "earning_per_completed_ride_proxy": [40.5, None],
            "drvr_cancel": [0, 0],
            "rider_cancel": [0, 0],
        }
    )


def test_city_day_clock_is_one_row_per_day():
    table = city_day_clock(_city_day(), zone=ZONE)
    assert table.column_names == [
        "trip_id",
        "mode",
        "timestamp",
        "pickup_zone",
        "drop_zone",
        "status",
        "fare",
        "cancel_reason",
        "source",
        "booking",
        "done_ride",
        "cancel_ride",
        "earning_per_completed_ride_proxy",
    ]
    assert "drvr_cancel" not in table.column_names
    assert "city" not in table.column_names
    assert "vehicle_type" not in table.column_names
    rows = table.to_pylist()
    assert rows[0]["trip_id"] == "namma_yatri_city_day:2025-01-02"
    assert rows[0]["mode"] == "ride_hail"
    assert rows[0]["source"] == "namma_yatri"
    assert rows[0]["fare"] is None
    assert rows[0]["status"] is None
    assert rows[0]["booking"] == 10
    assert rows[0]["done_ride"] == 7
    assert rows[1]["booking"] is None
    assert rows[1]["done_ride"] == 0
    moment = datetime.fromisoformat(rows[0]["timestamp"])
    assert moment.tzinfo is not None
    assert moment.hour == 0
    assert moment.utcoffset().total_seconds() == 5.5 * 3600


def test_partition_is_the_profile_and_assigned_auto():
    assert partition_values() == ("bengaluru", "auto")


def test_hdfs_load_puts_the_lake_file_and_does_not_delete():
    commands = hdfs_load_commands("bengaluru", "auto")
    flat = [" ".join(command) for command in commands]
    assert any("/lake/namma_yatri/city_day.parquet" in line for line in flat)
    assert any(
        "dfs -put -f" in line and "city=bengaluru/vehicle_type=auto" in line
        for line in flat
    )
    assert all("-rm" not in line for line in flat)

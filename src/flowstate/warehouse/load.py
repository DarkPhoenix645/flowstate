"""Load the city-day clock into the partitioned Hive ``rides`` table.

The bytes come from ``hdfs://namenode:9000/lake/namma_yatri/city_day.parquet``
(#16). The put uses ``docker exec`` and ``hdfs dfs -put``. There is no
WebHDFS client. ``city_hour`` and ``ward_hour`` stay on the lake.

Each output row is one clock day. ``trip_id`` is
``namma_yatri_city_day:{date}``. It is not a passenger trip. ``fare``
stays null. The driver and rider cancel columns are not copied.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from datetime import datetime, time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from flowstate.config import Settings, get_settings
from flowstate.lake.ridehail import HDFS_BIN, NAMENODE_LAKE_MOUNT
from flowstate.normalize.cities import DEFAULT_CITY_ID, load
from flowstate.schema import resolve_vehicle_type

LAKE_CITY_DAY = "/lake/namma_yatri/city_day.parquet"
WAREHOUSE_RIDES = "/user/hive/warehouse/rides"
CLOCK_COLUMNS = (
    "booking",
    "done_ride",
    "cancel_ride",
    "earning_per_completed_ride_proxy",
)
FILE_COLUMNS = (
    "trip_id",
    "mode",
    "timestamp",
    "pickup_zone",
    "drop_zone",
    "status",
    "fare",
    "cancel_reason",
    "source",
    *CLOCK_COLUMNS,
)

Runner = Callable[[list[str]], None]


def city_day_clock(table: pa.Table, *, zone) -> pa.Table:
    """Project a city-day table onto the non-partition ride columns.

    The source city name is not copied. ``vehicle_type`` is not a
    column here. The caller writes both as the partition directory.
    """
    frame = table.select(
        ["date", *CLOCK_COLUMNS]
    ).to_pydict()
    rows: list[dict[str, object]] = []
    for date, booking, done, cancel, proxy in zip(
        frame["date"],
        frame["booking"],
        frame["done_ride"],
        frame["cancel_ride"],
        frame["earning_per_completed_ride_proxy"],
        strict=True,
    ):
        day = str(date)[:10]
        moment = datetime.combine(
            datetime.fromisoformat(day).date(),
            time(0),
            tzinfo=zone,
        )
        rows.append(
            {
                "trip_id": f"namma_yatri_city_day:{day}",
                "mode": "ride_hail",
                "timestamp": moment.isoformat(),
                "pickup_zone": None,
                "drop_zone": None,
                "status": None,
                "fare": None,
                "cancel_reason": None,
                "source": "namma_yatri",
                "booking": booking,
                "done_ride": done,
                "cancel_ride": cancel,
                "earning_per_completed_ride_proxy": proxy,
            }
        )
    return pa.Table.from_pylist(rows, schema=_file_schema())


def partition_values() -> tuple[str, str]:
    """``(city, vehicle_type)`` for this load. The clock has no cab rows."""
    profile = load(DEFAULT_CITY_ID)
    return profile.id, resolve_vehicle_type("ride_hail")


def hdfs_load_commands(city: str, vehicle_type: str) -> list[list[str]]:
    """Get the lake file, then put the partition file. No ``-rm``."""
    src = f"{NAMENODE_LAKE_MOUNT}/hive-load/src/city_day.parquet"
    part = (
        f"{WAREHOUSE_RIDES}/city={city}/vehicle_type={vehicle_type}"
    )
    mounted = (
        f"{NAMENODE_LAKE_MOUNT}/hive-load/rides/"
        f"city={city}/vehicle_type={vehicle_type}/city_day.parquet"
    )
    return [
        _compose(HDFS_BIN, "dfsadmin", "-safemode", "wait"),
        _compose(HDFS_BIN, "dfs", "-get", "-f", LAKE_CITY_DAY, src),
        _compose(HDFS_BIN, "dfs", "-mkdir", "-p", part),
        _compose(
            HDFS_BIN,
            "dfs",
            "-put",
            "-f",
            mounted,
            f"{part}/city_day.parquet",
        ),
    ]


def load_rides(
    settings: Settings, run: Runner | None = None
) -> tuple[str, str, int]:
    """Project the lake city-day file and put it on the warehouse path."""
    city, vehicle_type = partition_values()
    runner = run or _docker_hdfs
    src_host = settings.lake_dir / "hive-load" / "src" / "city_day.parquet"
    part_host = (
        settings.lake_dir
        / "hive-load"
        / "rides"
        / f"city={city}"
        / f"vehicle_type={vehicle_type}"
        / "city_day.parquet"
    )
    commands = hdfs_load_commands(city, vehicle_type)
    runner(commands[0])
    src_host.parent.mkdir(parents=True, exist_ok=True)
    runner(commands[1])
    if not src_host.is_file():
        raise FileNotFoundError(
            f"{src_host} is missing after hdfs dfs -get {LAKE_CITY_DAY}"
        )
    table = city_day_clock(
        pq.read_table(src_host),
        zone=load(DEFAULT_CITY_ID).timezone,
    )
    part_host.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part_host)
    runner(commands[2])
    runner(commands[3])
    _remove_copy_in(settings.lake_dir / "hive-load" / "src")
    _remove_copy_in(settings.lake_dir / "hive-load" / "rides")
    return city, vehicle_type, table.num_rows


def _file_schema() -> pa.Schema:
    ints = {"booking", "done_ride", "cancel_ride"}
    fields = []
    for name in FILE_COLUMNS:
        if name in ints:
            fields.append(pa.field(name, pa.int64()))
        elif name == "earning_per_completed_ride_proxy" or name == "fare":
            fields.append(pa.field(name, pa.float64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


def _compose(binary: str, *args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "--env-file",
        "infra/kafka/cluster.env",
        "exec",
        "-T",
        "namenode",
        binary,
        *args,
    ]


def _docker_hdfs(argv: list[str]) -> None:
    result = subprocess.run(argv, capture_output=True, text=True)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)


def _remove_copy_in(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)


def main() -> None:
    city, vehicle_type, rows = load_rides(get_settings())
    print(
        f"loaded {rows} city-day rows into "
        f"{WAREHOUSE_RIDES}/city={city}/vehicle_type={vehicle_type}"
    )


if __name__ == "__main__":
    main()

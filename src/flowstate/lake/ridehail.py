"""Put staged ride-hail parquet on HDFS.

The host files stay in ``data/staged/namma_yatri/``. The namenode mount
``./data/lake`` -> ``/mnt/lake`` is the copy-in. The lake directory is
``hdfs://namenode:9000/lake/namma_yatri/``.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

from flowstate.config import Settings

RIDEHAIL_DATASET = "namma_yatri"
RIDEHAIL_PARQUET = (
    "city_day.parquet",
    "city_hour.parquet",
    "ward_hour.parquet",
)
HDFS_RIDEHAIL_DIR = "/lake/namma_yatri"
HDFS_BIN = "/opt/hadoop-3.2.1/bin/hdfs"
NAMENODE_LAKE_MOUNT = "/mnt/lake"

Runner = Callable[[list[str]], None]


def ridehail_hdfs_uri(settings: Settings, name: str) -> str:
    base = settings.hdfs_namenode.rstrip("/")
    return f"{base}{HDFS_RIDEHAIL_DIR}/{name}"


def staged_ridehail_files(settings: Settings) -> list[Path]:
    root = settings.staged_dir / RIDEHAIL_DATASET
    return [root / name for name in RIDEHAIL_PARQUET]


def put_ridehail(
    settings: Settings, run: Runner | None = None
) -> list[str]:
    """Copy the three ride-hail parquet files onto HDFS.

    Does not remove the host staged files. Does not run Hive DDL.
    Does not write ``rides_amplified``.
    """
    sources = staged_ridehail_files(settings)
    missing = [path for path in sources if not path.is_file()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(
            f"staged ride-hail parquet missing: {joined}"
        )
    stamps = {path: _stamp(path) for path in sources}
    copy_dir = settings.lake_dir / RIDEHAIL_DATASET
    try:
        for source in sources:
            _copy_in(source, copy_dir / source.name)
        _require_unchanged(sources, stamps)
        _hdfs_put(sources, run or _docker_hdfs)
        _remove_copy_in(copy_dir)
    finally:
        _require_unchanged(sources, stamps)
    return [ridehail_hdfs_uri(settings, path.name) for path in sources]


def hdfs_commands(
    names: tuple[str, ...] = RIDEHAIL_PARQUET,
) -> list[list[str]]:
    mount = f"{NAMENODE_LAKE_MOUNT}/{RIDEHAIL_DATASET}"
    commands = [
        _compose(HDFS_BIN, "dfsadmin", "-safemode", "wait"),
        _compose(HDFS_BIN, "dfs", "-mkdir", "-p", HDFS_RIDEHAIL_DIR),
    ]
    for name in names:
        commands.append(
            _compose(
                HDFS_BIN,
                "dfs",
                "-put",
                "-f",
                f"{mount}/{name}",
                f"{HDFS_RIDEHAIL_DIR}/{name}",
            )
        )
    commands.append(_compose(HDFS_BIN, "dfs", "-ls", HDFS_RIDEHAIL_DIR))
    return commands


def _hdfs_put(sources: list[Path], run: Runner) -> None:
    names = tuple(path.name for path in sources)
    for command in hdfs_commands(names):
        run(command)


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


def _copy_in(source: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    partial = dest.with_name(dest.name + ".partial")
    shutil.copyfile(source, partial)
    partial.replace(dest)


def _remove_copy_in(directory: Path) -> None:
    if not directory.is_dir():
        return
    for name in RIDEHAIL_PARQUET:
        path = directory / name
        if path.is_file():
            path.unlink()
    for path in directory.glob("*.partial"):
        if path.is_file():
            path.unlink()
    if not any(directory.iterdir()):
        directory.rmdir()


def _stamp(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return (stat.st_size, stat.st_mtime_ns)


def _require_unchanged(
    sources: list[Path], stamps: dict[Path, tuple[int, int]]
) -> None:
    for path in sources:
        if not path.is_file() or _stamp(path) != stamps[path]:
            raise RuntimeError(f"host staged file changed: {path}")


def _docker_hdfs(argv: list[str]) -> None:
    result = subprocess.run(argv, capture_output=True, text=True)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)

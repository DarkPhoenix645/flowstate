"""Put the amplified corpus on HDFS beside ``namma_yatri``.

Host ``data/staged/rides_amplified/`` stays. The copy-in is
``data/lake/rides_amplified/`` and is removed after the put. The
lake directory is ``hdfs://namenode:9000/lake/rides_amplified/``.
Part files sit in that directory. Manifest files sit in ``_meta``
so a parquet read does not open them.

``-rm -r -f`` applies only to that lake directory, so a second run
does not leave a stale part behind. Source tables are not arguments
to any command.
"""

from __future__ import annotations

import logging
import shutil
import sys
from pathlib import Path

from flowstate.config import Settings, get_settings
from flowstate.lake.ridehail import (
    HDFS_BIN,
    NAMENODE_LAKE_MOUNT,
    Runner,
    _compose,
    _copy_in,
    _docker_hdfs,
    _require_unchanged,
    _stamp,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)

HDFS_AMPLIFIED_DIR = "/lake/rides_amplified"
META_FILES = ("manifest.json", "bundle.json", "evaluation.json")


def amplified_hdfs_uri(settings: Settings, name: str) -> str:
    base = settings.hdfs_namenode.rstrip("/")
    return f"{base}{HDFS_AMPLIFIED_DIR}/{name}"


def hdfs_commands(part_names: tuple[str, ...]) -> list[list[str]]:
    mount = f"{NAMENODE_LAKE_MOUNT}/rides_amplified"
    commands = [
        _compose(HDFS_BIN, "dfsadmin", "-safemode", "wait"),
        _compose(
            HDFS_BIN, "dfs", "-rm", "-r", "-f", HDFS_AMPLIFIED_DIR
        ),
        _compose(
            HDFS_BIN,
            "dfs",
            "-mkdir",
            "-p",
            f"{HDFS_AMPLIFIED_DIR}/_meta",
        ),
    ]
    for name in part_names:
        commands.append(
            _compose(
                HDFS_BIN,
                "dfs",
                "-put",
                "-f",
                f"{mount}/{name}",
                f"{HDFS_AMPLIFIED_DIR}/{name}",
            )
        )
    for name in META_FILES:
        commands.append(
            _compose(
                HDFS_BIN,
                "dfs",
                "-put",
                "-f",
                f"{mount}/_meta/{name}",
                f"{HDFS_AMPLIFIED_DIR}/_meta/{name}",
            )
        )
    commands.append(
        _compose(HDFS_BIN, "dfs", "-ls", "-R", HDFS_AMPLIFIED_DIR)
    )
    return commands


def put_amplified(
    settings: Settings, run: Runner | None = None
) -> list[str]:
    """Copy the corpus onto HDFS. Leave the host corpus in place."""
    root = settings.staged_dir / "rides_amplified"
    parts = sorted(root.glob("part-*.parquet"))
    meta = [root / name for name in META_FILES]
    missing = [path for path in (*parts, *meta) if not path.is_file()]
    if not parts or missing:
        joined = ", ".join(str(path) for path in missing) or str(root)
        raise FileNotFoundError(
            f"amplified corpus is incomplete: {joined}"
        )
    stamps = {path: _stamp(path) for path in (*parts, *meta)}
    copy_root = settings.lake_dir / "rides_amplified"
    try:
        for path in parts:
            _copy_in(path, copy_root / path.name)
        for path in meta:
            _copy_in(path, copy_root / "_meta" / path.name)
        _require_unchanged([*parts, *meta], stamps)
        for command in hdfs_commands(tuple(path.name for path in parts)):
            (run or _docker_hdfs)(command)
        _remove_copy_in(copy_root)
    finally:
        _require_unchanged([*parts, *meta], stamps)
    uris = [amplified_hdfs_uri(settings, path.name) for path in parts]
    uris.extend(
        amplified_hdfs_uri(settings, f"_meta/{name}") for name in META_FILES
    )
    return uris


def _remove_copy_in(directory: Path) -> None:
    if directory.exists():
        shutil.rmtree(directory)


def main() -> None:
    configure_logging()
    try:
        uris = put_amplified(get_settings())
    except FileNotFoundError as exc:
        log.error("%s", exc)
        sys.exit(1)
    for uri in uris:
        log.info("%s", uri)


if __name__ == "__main__":
    main()

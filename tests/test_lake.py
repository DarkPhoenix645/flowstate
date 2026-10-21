from pathlib import Path

import pytest

from flowstate.config import Settings
from flowstate.lake import ridehail
from flowstate.lake.ridehail import (
    HDFS_BIN,
    HDFS_RIDEHAIL_DIR,
    RIDEHAIL_PARQUET,
    hdfs_commands,
    put_ridehail,
    ridehail_hdfs_uri,
)


def _settings(tmp_path: Path) -> Settings:
    staged = tmp_path / "staged" / "namma_yatri"
    staged.mkdir(parents=True)
    for name in RIDEHAIL_PARQUET:
        (staged / name).write_bytes(b"parquet-" + name.encode())
    (staged / "city_day.coverage.json").write_text("{}\n")
    hive = tmp_path / "lake" / "hive-load"
    hive.mkdir(parents=True)
    (hive / "metro_daily.parquet").write_bytes(b"keep")
    return Settings(data_dir=tmp_path)


def test_hdfs_commands_use_the_namenode_binary():
    commands = hdfs_commands()
    assert commands[0][-3:] == ["dfsadmin", "-safemode", "wait"]
    for command in commands:
        assert command[command.index("namenode") + 1] == HDFS_BIN
        assert "hive" not in command
        assert "-rm" not in command
    put = [
        command
        for command in commands
        if "-put" in command
    ]
    assert [command[-1] for command in put] == [
        f"{HDFS_RIDEHAIL_DIR}/{name}" for name in RIDEHAIL_PARQUET
    ]
    for command in put:
        assert command[-2].startswith("/mnt/lake/namma_yatri/")
    flat = [arg for command in commands for arg in command]
    assert not any("coverage.json" in arg for arg in flat)


def test_put_leaves_host_files_and_drops_the_copy_in(tmp_path: Path):
    settings = _settings(tmp_path)
    staged = settings.staged_dir / "namma_yatri"
    before = {path.name: path.read_bytes() for path in staged.iterdir()}
    seen: list[list[str]] = []

    def run(argv: list[str]) -> None:
        seen.append(argv)
        if "-put" in argv:
            name = Path(argv[-1]).name
            copy = settings.lake_dir / "namma_yatri" / name
            assert copy.read_bytes() == before[name]

    uris = put_ridehail(settings, run)
    assert uris == [
        ridehail_hdfs_uri(settings, name) for name in RIDEHAIL_PARQUET
    ]
    assert seen == hdfs_commands()
    assert {path.name: path.read_bytes() for path in staged.iterdir()} == before
    assert not (settings.lake_dir / "namma_yatri").exists()
    hive_load = settings.lake_dir / "hive-load" / "metro_daily.parquet"
    assert hive_load.read_bytes() == b"keep"


def test_missing_parquet_does_not_call_hdfs(tmp_path: Path):
    settings = _settings(tmp_path)
    (settings.staged_dir / "namma_yatri" / "ward_hour.parquet").unlink()

    def run(argv: list[str]) -> None:
        raise AssertionError(argv)

    with pytest.raises(FileNotFoundError, match="ward_hour.parquet"):
        put_ridehail(settings, run)
    assert not (settings.lake_dir / "namma_yatri").exists()


def test_hdfs_failure_keeps_host_files_and_the_copy_in(tmp_path: Path):
    settings = _settings(tmp_path)

    def run(argv: list[str]) -> None:
        if "-put" in argv:
            raise SystemExit(1)

    with pytest.raises(SystemExit):
        put_ridehail(settings, run)
    staged = settings.staged_dir / "namma_yatri"
    for name in RIDEHAIL_PARQUET:
        assert (staged / name).read_bytes().startswith(b"parquet-")
        assert (settings.lake_dir / "namma_yatri" / name).is_file()
    hive_load = settings.lake_dir / "hive-load" / "metro_daily.parquet"
    assert hive_load.read_bytes() == b"keep"


def test_put_aborts_when_the_host_file_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    settings = _settings(tmp_path)
    source = settings.staged_dir / "namma_yatri" / "city_day.parquet"
    real_copy = ridehail._copy_in

    def bad_copy(src: Path, dest: Path) -> None:
        real_copy(src, dest)
        if src == source:
            src.write_bytes(b"changed")

    monkeypatch.setattr(ridehail, "_copy_in", bad_copy)

    def run(argv: list[str]) -> None:
        raise AssertionError(argv)

    with pytest.raises(RuntimeError, match="host staged file changed"):
        put_ridehail(settings, run)

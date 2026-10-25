"""Lake put for the amplified corpus. No Docker and no network."""

from pathlib import Path

import pytest

from flowstate.config import Settings
from flowstate.lake.amplify import (
    HDFS_AMPLIFIED_DIR,
    hdfs_commands,
    put_amplified,
)


def _settings(tmp_path: Path) -> Settings:
    corpus = tmp_path / "staged" / "rides_amplified"
    corpus.mkdir(parents=True)
    (corpus / "part-00000.parquet").write_bytes(b"part-0")
    (corpus / "part-00001.parquet").write_bytes(b"part-1")
    for name in ("manifest.json", "bundle.json", "evaluation.json"):
        (corpus / name).write_text("{}\n")
    source = tmp_path / "staged" / "namma_yatri"
    source.mkdir()
    (source / "city_day.parquet").write_bytes(b"source-clock")
    return Settings(data_dir=tmp_path)


def test_commands_replace_only_the_amplified_directory():
    commands = hdfs_commands(("part-00000.parquet", "part-00001.parquet"))
    flat = [arg for command in commands for arg in command]
    assert HDFS_AMPLIFIED_DIR in flat
    assert any(
        arg == HDFS_AMPLIFIED_DIR and "-rm" in command
        for command in commands
        for arg in command
    )
    assert not any("namma_yatri" in arg for arg in flat)
    assert any(
        "/_meta/manifest.json" in arg for arg in flat
    )
    assert all(
        arg != "/lake" and not arg.startswith("/lake/namma")
        for arg in flat
    )


def test_put_leaves_the_host_corpus_and_the_source_clock(tmp_path: Path):
    settings = _settings(tmp_path)
    corpus = settings.staged_dir / "rides_amplified"
    source = settings.staged_dir / "namma_yatri" / "city_day.parquet"
    before = {path.name: path.read_bytes() for path in corpus.iterdir()}
    seen: list[list[str]] = []

    def run(argv: list[str]) -> None:
        seen.append(argv)

    uris = put_amplified(settings, run)
    assert seen == hdfs_commands(("part-00000.parquet", "part-00001.parquet"))
    assert any(uri.endswith("/part-00000.parquet") for uri in uris)
    assert any(uri.endswith("/_meta/manifest.json") for uri in uris)
    assert {path.name: path.read_bytes() for path in corpus.iterdir()} == before
    assert source.read_bytes() == b"source-clock"
    assert not (settings.lake_dir / "rides_amplified").exists()


def test_missing_manifest_does_not_call_hdfs(tmp_path: Path):
    settings = _settings(tmp_path)
    (settings.staged_dir / "rides_amplified" / "manifest.json").unlink()

    def run(argv: list[str]) -> None:
        raise AssertionError(argv)

    with pytest.raises(FileNotFoundError, match="manifest.json"):
        put_amplified(settings, run)
    assert not (settings.lake_dir / "rides_amplified").exists()

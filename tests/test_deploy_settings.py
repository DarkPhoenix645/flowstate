"""Deploy paths come from Settings, not the working directory."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from flowstate.config import Settings, get_settings, repo_root
from flowstate.lake.ridehail import ridehail_hdfs_uri
from flowstate.provenance import ProvenanceError, require_valid
from flowstate.stage.events import BUS_TOPIC, METRO_TOPIC
from flowstate.topic_names import TOPIC_BY_MODE


def _clear_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FLOWSTATE_DATA_DIR", raising=False)
    monkeypatch.delenv("FLOWSTATE_DATASETS_DIR", raising=False)


def test_a_relative_data_dir_is_under_the_repo(monkeypatch):
    _clear_paths(monkeypatch)
    settings = Settings(_env_file=None)
    assert settings.data_dir == repo_root() / "data"
    assert settings.datasets_dir == settings.data_dir / "raw"
    assert settings.datasets_dir == settings.raw_dir


def test_data_dir_alone_moves_raw(monkeypatch, tmp_path):
    _clear_paths(monkeypatch)
    settings = Settings(_env_file=None, data_dir=tmp_path)
    assert settings.datasets_dir == tmp_path / "raw"


def test_an_explicit_datasets_dir_stays(tmp_path):
    raw = tmp_path / "extracts"
    settings = Settings(
        _env_file=None, data_dir=tmp_path / "data", datasets_dir=raw
    )
    assert settings.datasets_dir == raw
    assert settings.raw_dir == tmp_path / "data" / "raw"


def test_require_valid_reads_the_manifest_under_datasets_dir(tmp_path):
    raw = tmp_path / "raw"
    target = raw / "note.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"ok")
    digest = hashlib.sha256(b"ok").hexdigest()
    manifest = raw / "provenance" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "dataset_id": "toy",
                        "valid": True,
                        "files": [
                            {
                                "path": "data/raw/note.txt",
                                "sha256": digest,
                            }
                        ],
                    }
                ]
            }
        )
    )
    require_valid(["toy"], raw)
    with pytest.raises(ProvenanceError, match="manifest.json"):
        require_valid(["toy"], tmp_path / "other")


def test_an_explicit_env_file_is_the_deploy_source(tmp_path, monkeypatch):
    _clear_paths(monkeypatch)
    env = tmp_path / "custom.env"
    data = tmp_path / "from-env"
    env.write_text(f"FLOWSTATE_DATA_DIR={data}\n")
    settings = get_settings(env)
    assert settings.data_dir == data
    assert settings.datasets_dir == data / "raw"
    monkeypatch.delenv("FLOWSTATE_DATA_DIR", raising=False)


def test_topic_names_are_defined_once():
    assert METRO_TOPIC == TOPIC_BY_MODE["metro"]
    assert BUS_TOPIC == TOPIC_BY_MODE["bus"]
    source = Path("src/flowstate/stage/events.py").read_text()
    assert "flowstate.metro.station_hour.v1" not in source
    assert "flowstate.bus.scheduled_stop.v1" not in source
    makefile = Path("Makefile").read_text()
    assert "TOPIC ?=" not in makefile


def test_the_host_lake_reads_hdfs_namenode(tmp_path):
    settings = Settings(
        _env_file=None,
        data_dir=tmp_path,
        hdfs_namenode="hdfs://example:9000",
    )
    uri = ridehail_hdfs_uri(settings, "city_day.parquet")
    assert uri == "hdfs://example:9000/lake/namma_yatri/city_day.parquet"

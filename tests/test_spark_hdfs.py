"""Compose Spark ships the Hive Hadoop client conf (ticket #7)."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
ADR_0003 = (
    ROOT / "docs" / "adr" / "0003-compose-cluster-host-uv.md"
).read_text(encoding="utf-8")

HADOOP_CONF_DIR = "/etc/hadoop"
CORE = "./infra/hive/core-site.xml:/etc/hadoop/core-site.xml:ro"
HDFS = "./infra/hive/hdfs-site.xml:/etc/hadoop/hdfs-site.xml:ro"
_SERVICE = re.compile(r"^  ([A-Za-z0-9_-]+):\r?\n", re.M)


def _service_block(name: str) -> str:
    match = re.search(rf"^  {re.escape(name)}:\r?\n", COMPOSE, re.M)
    if match is None:
        raise AssertionError(f"no compose service {name}")
    nxt = _SERVICE.search(COMPOSE, match.end())
    end = nxt.start() if nxt is not None else len(COMPOSE)
    return COMPOSE[match.start() : end]


def test_clients_mount_hadoop_conf_at_etc_hadoop():
    for name in ("spark", "spark-worker", "hive-metastore", "hiveserver2"):
        block = _service_block(name)
        assert f"HADOOP_CONF_DIR: {HADOOP_CONF_DIR}" in block, name
        assert CORE in block, name
        assert HDFS in block, name


def test_spark_keeps_data_mount_and_depends_on_hdfs():
    for name in ("spark", "spark-worker"):
        block = _service_block(name)
        assert "./data:/opt/data" in block, name
        assert "- namenode" in block, name
        assert "- datanode" in block, name


def test_daemons_do_not_overlay_generated_etc_hadoop():
    for name in ("namenode", "datanode"):
        block = _service_block(name)
        assert "infra/hive/core-site.xml" not in block, name
        assert "infra/hive/hdfs-site.xml" not in block, name


def test_adr_0003_does_not_require_opt_data_as_batch_input():
    assert "hdfs://namenode:9000/lake" in ADR_0003
    assert "HADOOP_CONF_DIR" in ADR_0003
    lowered = ADR_0003.lower()
    assert "must be visible inside spark at `/opt/data`" not in lowered

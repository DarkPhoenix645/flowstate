"""Coverage reports and content hashes for normalized tables."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


@dataclass
class TableBuild:
    table: pa.Table
    coverage: dict[str, Any]
    dates: set[str] = field(default_factory=set)
    extra: dict[str, Any] = field(default_factory=dict)


def write_table(
    directory: Path, name: str, built: TableBuild
) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.parquet"
    pq.write_table(built.table, path)
    coverage = dict(built.coverage)
    coverage["table"] = name
    coverage["rows"] = built.table.num_rows
    coverage["parquet"] = path.name
    coverage["sha256"] = sha256_file(path)
    write_json(directory / f"{name}.coverage.json", coverage)
    return coverage

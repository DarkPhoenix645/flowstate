"""Write and read staged parquet under data/staged/<dataset>/."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from flowstate.config import Settings
from flowstate.schema import RIDE_COLUMNS, RIDE_FIELDS

_ZONE_COLUMNS = frozenset({"pickup_zone", "drop_zone"})
_KINDS = dict(RIDE_FIELDS)


def staged_parquet_path(settings: Settings, dataset: str, name: str) -> Path:
    """Path of one staged parquet file.

    ``data/staged/<dataset>/<name>.parquet``. ``city`` and
    ``vehicle_type`` stay columns in the file. They are not directories.
    """
    dataset = _segment(dataset, "dataset")
    if name.endswith(".parquet"):
        name = name[: -len(".parquet")]
    name = _segment(name, "name")
    return settings.staged_dir / dataset / f"{name}.parquet"


def write_parquet(path: Path, table: pa.Table) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    return path


def read_parquet(path: Path) -> pa.Table:
    return pq.read_table(path)


def write_rides(
    rows: Sequence[Mapping[str, Any]],
    settings: Settings,
    dataset: str,
    name: str = "rides",
) -> Path:
    """Write ride rows. Canonical columns are first. Extras follow."""
    path = staged_parquet_path(settings, dataset, name)
    return write_parquet(path, rides_table(rows))


def read_rides(path: Path) -> list[dict[str, Any]]:
    table = read_parquet(path)
    missing = [c for c in RIDE_COLUMNS if c not in table.column_names]
    if missing:
        raise ValueError(f"parquet missing {missing}")
    return table.to_pylist()


def rides_table(rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    extra_names: list[str] = []
    seen: set[str] = set()
    coerced: list[dict[str, Any]] = []
    for row in rows:
        missing = [c for c in RIDE_COLUMNS if c not in row]
        if missing:
            raise ValueError(f"row missing {missing}")
        item = {name: _coerce(name, row[name]) for name in RIDE_COLUMNS}
        for key, value in row.items():
            if key in _KINDS:
                continue
            if key not in seen:
                extra_names.append(key)
                seen.add(key)
            item[key] = value
        coerced.append(item)

    columns: dict[str, list[Any]] = {name: [] for name in RIDE_COLUMNS}
    for name in extra_names:
        columns[name] = []
    for item in coerced:
        for name in RIDE_COLUMNS:
            columns[name].append(item[name])
        for name in extra_names:
            columns[name].append(item.get(name))

    schema = _arrow_schema(extra_names, columns)
    return pa.table(columns, schema=schema)


def _arrow_schema(
    extra_names: list[str], columns: dict[str, list[Any]]
) -> pa.Schema:
    fields = [_arrow_field(name, kind) for name, kind in RIDE_FIELDS]
    for name in extra_names:
        fields.append(pa.field(name, _extra_type(columns[name])))
    return pa.schema(fields)


def _arrow_field(name: str, kind: str) -> pa.Field:
    if kind == "string":
        return pa.field(name, pa.string())
    if kind == "number":
        return pa.field(name, pa.float64())
    raise ValueError(f"unknown field kind {kind}")


def _extra_type(values: list[Any]) -> pa.DataType:
    kinds = {_python_kind(v) for v in values if v is not None}
    if not kinds or kinds == {"string"}:
        return pa.string()
    if kinds == {"number"}:
        return pa.float64()
    raise TypeError(f"extra column has mixed types {sorted(kinds)}")


def _python_kind(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TypeError(f"unsupported extra value {value!r}")
    if isinstance(value, str):
        return "string"
    return "number"


def _segment(value: str, label: str) -> str:
    if (
        not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"bad staged {label}: {value!r}")
    return value


def _coerce(name: str, value: Any) -> Any:
    if name in _ZONE_COLUMNS:
        return _zone(name, value)
    if name == "timestamp":
        return _timestamp(value)
    if _KINDS[name] == "number":
        return _fare(value)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _zone(name: str, value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value.strip() == "":
        raise TypeError(f"{name} must be a string id")
    return value


def _timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("timestamp must be an ISO-8601 string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp needs a numeric offset")
    return value


def _fare(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("fare must be a number or null")
    return float(value)

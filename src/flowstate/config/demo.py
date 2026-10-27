"""Demo API routes beside model config. No run routes."""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import Any

from flowstate.config import get_settings
from flowstate.config.model import (
    BUS_PRIOR_FIELDS,
    CONFIG_SHOWN,
    CONFIG_WRITABLE,
    ResampleConfig,
    config_knob_allowed,
)
from flowstate.config.postgres import ConfigStore, resolve_active
from flowstate.config.resolve import (
    ConfigNotActiveError,
    ConfigValidationError,
)
from flowstate.provenance import manifest_path
from flowstate.resample.envelope import to_envelope
from flowstate.streaming.control import start_default

_SERVICES = (
    ("namenode", "127.0.0.1", 9870),
    ("kafka", "127.0.0.1", 9094),
    ("spark", "127.0.0.1", 8080),
    ("hive", "127.0.0.1", 10000),
    ("grafana", "127.0.0.1", 3000),
)
_OPENSKY = {"arrivals": "arrival", "departures": "departure"}
_EMPTY_BENCH = "no comparison has been run"


def demo_dispatch(
    store: ConfigStore,
    method: str,
    path: str,
    query: str,
    raw: bytes,
    settings: Any,
) -> tuple[int, dict[str, Any]] | None:
    """Handle a demo route. None when the path is not one of them."""
    from urllib.parse import parse_qs

    if path == "/api/config" and method == "GET":
        return 200, config_view(store)
    if path == "/api/config" and method == "PUT":
        return put_config(store, raw)
    if path == "/api/config/reset" and method == "POST":
        store.deactivate("resample_config")
        store.clear_run_config()
        return 200, config_view(store)
    if path == "/api/system" and method == "GET":
        return 200, {"services": system_status()}
    if path == "/api/datasets" and method == "GET":
        return datasets_view(settings)
    if path == "/api/datasets/opensky" and method == "GET":
        direction = parse_qs(query).get("direction", [""])[0]
        return opensky_view(settings, direction)
    if path == "/api/benchmarks" and method == "GET":
        rows = store.benchmarks()
        if not rows:
            return 404, {"error": _EMPTY_BENCH}
        return 200, {"benchmarks": rows}
    if path == "/api/events/snapshot" and method == "GET":
        return 200, {"fixture": True, "events": snapshot_events()}
    return None


def config_view(store: ConfigStore) -> dict[str, Any]:
    model, sources, version_id = _resample(store)
    knobs = []
    for name in (*CONFIG_WRITABLE, *CONFIG_SHOWN):
        knobs.append(
            {
                "name": name,
                "value": getattr(model, name),
                "source": sources[name],
                "editable": name in CONFIG_WRITABLE,
            }
        )
    seed = store.replay_seed()
    knobs.append(
        {
            "name": "replay_seed",
            "value": start_default("seed") if seed is None else seed,
            "source": "default" if seed is None else "db",
            "editable": True,
        }
    )
    payload: dict[str, Any] = {
        "knobs": knobs,
        "coverage": {
            "value": model.coverage.model_dump(mode="json"),
            "source": sources["coverage"],
            "editable": False,
        },
        "resample_config_version_id": version_id,
        "config_hash": _config_hash(store),
    }
    return payload


def put_config(store: ConfigStore, raw: bytes) -> tuple[int, dict[str, Any]]:
    body = _object(raw)
    seed, changes = _parse_config_put(body)
    model, _sources, _version = _resample(store)
    if changes:
        updated = model.model_copy(update=changes)
        store.put_version(
            "resample_config",
            updated.model_dump(mode="json"),
            note="api/config",
        )
    if seed is not None:
        store.set_replay_seed(seed)
    return 200, config_view(store)


def system_status() -> list[dict[str, Any]]:
    rows = []
    for name, host, port in _SERVICES:
        rows.append({"name": name, "port": port, "up": _port_open(host, port)})
    return rows


def datasets_view(settings: Any) -> tuple[int, dict[str, Any]]:
    path = manifest_path(settings.datasets_dir)
    if not path.is_file():
        return 404, {"error": f"missing {path.name}"}
    manifest = json.loads(path.read_text())
    rows = [
        {
            "dataset_id": item.get("dataset_id"),
            "valid": item.get("valid") is True,
            "invalid_reason": item.get("invalid_reason"),
        }
        for item in manifest.get("datasets") or []
    ]
    return 200, {"datasets": rows}


def opensky_view(settings: Any, direction: str) -> tuple[int, dict[str, Any]]:
    key = _OPENSKY.get(direction)
    if key is None:
        raise ConfigValidationError(
            "direction must be arrivals or departures"
        )
    from flowstate.ingest import opensky_flights as sky

    required = sky.windows()
    cache = settings.datasets_dir / "opensky" / "windows"
    cached = 0
    for start, end in required:
        name = (
            f"{sky.AIRPORT}_{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}Z.json"
        )
        nested = cache / key / name
        flat = cache / name
        if _nonempty(nested) or (key == "arrival" and _nonempty(flat)):
            cached += 1
    gap = len(required) - cached
    payload = {
        "direction": direction,
        "windows_cached": cached,
        "windows_required": len(required),
        "gap": gap,
    }
    if key == "departure" and gap:
        return 409, payload
    return 200, payload


def snapshot_events() -> list[dict[str, Any]]:
    row = {
        "trip_id": "fixture-1",
        "mode": "ride_hail",
        "event_type": "booking_created",
        "timestamp": "2025-08-20T10:00:00+05:30",
        "count": 1,
        "scenario_id": None,
        "provenance_status": "synthetic",
        "source_ids": "fixture",
        "model_bundle_version": "fixture",
        "profile_version": "fixture",
        "seed_id": "seed-42",
        "evidence_grade": "assumption_driven",
        "outside_training_window": "false",
        "pickup_zone": "b_1",
        "drop_zone": "b_2",
    }
    event = to_envelope(row)
    event["fixture"] = True
    return [event]


def _resample(
    store: ConfigStore,
) -> tuple[ResampleConfig, dict[str, str], str | None]:
    active = store.active()
    version = active.get("resample_config")
    if version is not None:
        from flowstate.config.postgres import validate_document

        model = validate_document("resample_config", version.content)
        assert isinstance(model, ResampleConfig)
        sources = {name: "db" for name in ResampleConfig.model_fields}
        return model, sources, version.id
    model = ResampleConfig.from_settings(get_settings())
    sources = {}
    for name in ResampleConfig.model_fields:
        if name in BUS_PRIOR_FIELDS and _env_present(name):
            sources[name] = "env"
        else:
            sources[name] = "default"
    return model, sources, None


def _parse_config_put(
    body: dict[str, Any],
) -> tuple[int | None, dict[str, Any]]:
    seed: int | None = None
    changes: dict[str, Any] = {}
    for name, value in body.items():
        if name == "replay_seed":
            if isinstance(value, bool) or not isinstance(value, int):
                raise ConfigValidationError("replay_seed must be an integer")
            seed = value
            continue
        if name not in CONFIG_WRITABLE:
            raise ConfigValidationError(f"{name} is not writable")
        if not config_knob_allowed(name, value):
            raise ConfigValidationError(f"{name} is outside the allowed set")
        changes[name] = value
    if seed is None and not changes:
        raise ConfigValidationError("body is empty")
    return seed, changes


def _config_hash(store: ConfigStore) -> str | None:
    try:
        return resolve_active(store).config_hash
    except ConfigNotActiveError:
        return None


def _env_present(name: str) -> bool:
    raw = os.environ.get("FLOWSTATE_" + name.upper())
    return raw is not None and raw.strip() != ""


def _object(raw: bytes) -> dict[str, Any]:
    if not raw:
        raise ConfigValidationError("body is required")
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise ConfigValidationError("body must be a JSON object")
    return body


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.3):
            return True
    except OSError:
        return False


def _nonempty(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0

"""The file-backed model-config seam. No Postgres."""

from __future__ import annotations

import json

import pytest

import resample_fixtures as fx
from flowstate.config import Settings
from flowstate.config.model import (
    BUS_PRIOR_FIELDS,
    ResampleConfig,
    canonical_json,
)
from flowstate.config.resolve import (
    FileConfigSource,
    MemoryConfigSource,
    resolve_models,
)
from flowstate.resample.bundle import BundleCache
from flowstate.resample.scenario import POLICY_PATH, load_policy

_PRIOR_ENV = tuple(
    "FLOWSTATE_" + name.upper() for name in BUS_PRIOR_FIELDS
)


def _clear_prior(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _PRIOR_ENV:
        monkeypatch.delenv(key, raising=False)


def test_whitespace_does_not_change_the_config_hash(tmp_path):
    raw = json.loads(POLICY_PATH.read_text())
    pretty = tmp_path / "policy.json"
    pretty.write_text(json.dumps(raw, indent=4))
    tight = tmp_path / "tight.json"
    tight.write_text(json.dumps(raw, separators=(",", ":")))
    settings = Settings(_env_file=None)
    left = FileConfigSource(settings, pretty).load()
    right = FileConfigSource(settings, tight).load()
    assert left.config_hash == right.config_hash
    assert left.kinds["scenario_policy"].source == "file"
    assert left.version_id("scenario_policy") is None
    raw["notes"] = raw.get("notes", "") + " edited"
    changed = tmp_path / "changed.json"
    changed.write_text(json.dumps(raw))
    edited = FileConfigSource(settings, changed).load()
    assert edited.config_hash != left.config_hash


def test_env_marks_only_the_bus_prior_field(monkeypatch):
    _clear_prior(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS", "3310000")
    settings = Settings(_env_file=None)
    resolved = FileConfigSource(settings).load()
    config = resolved.model.resample_config
    assert config.assumed_weekday_boardings == 3_310_000
    assert resolved.field_sources["assumed_weekday_boardings"] == "env"
    assert resolved.field_sources["assumed_bus_capacity"] == "default"
    assert resolved.field_sources["outside_widen"] == "default"
    assert "google_maps_api_key" not in type(config).model_fields
    assert "secret" not in canonical_json(config)


def test_unset_env_leaves_the_defaults(monkeypatch):
    _clear_prior(monkeypatch)
    settings = Settings(_env_file=None)
    resolved = FileConfigSource(settings).load()
    assert resolved.model.resample_config == ResampleConfig()
    assert set(resolved.field_sources.values()) == {"default"}


def test_a_low_above_a_high_is_rejected():
    with pytest.raises(ValueError, match="peak share"):
        ResampleConfig(bus_peak_share_low=0.8, bus_peak_share_high=0.2)


def test_a_policy_edit_hits_the_bundle_cache_and_a_rule_edit_misses():
    inputs = fx.fixture_inputs()
    spec = fx.fixture_spec()
    cache = BundleCache()
    config = ResampleConfig()
    policy = load_policy()
    first = resolve_models(
        policy, config, source="memory", version_id=None
    )
    cache.bundle(
        inputs,
        spec,
        config,
        seed=42,
        config_hash=first.config_hash,
    )
    edited = policy.model_copy(update={"notes": policy.notes + " x"})
    second = resolve_models(
        edited, config, source="memory", version_id=None
    )
    cache.bundle(
        inputs,
        spec,
        config,
        seed=42,
        config_hash=second.config_hash,
    )
    assert cache.builds == 1
    assert first.config_hash != second.config_hash
    moved = config.model_copy(
        update={
            "coverage": config.coverage.model_copy(
                update={"sparse_fraction": 0.9}
            )
        }
    )
    third = resolve_models(
        policy, moved, source="memory", version_id=None
    )
    cache.bundle(
        inputs,
        spec,
        moved,
        seed=42,
        config_hash=third.config_hash,
    )
    assert cache.builds == 2


def test_fit_and_gate_share_the_rule_on_the_config():
    import pyarrow as pa

    from flowstate.config.model import CoverageRule
    from flowstate.resample.aviation_coverage import covered_counts, gated_keys

    rows = []
    for day, count in (("2025-08-01", 40), ("2025-08-02", 4)):
        for index in range(count):
            rows.append(
                {
                    "direction": "arrival",
                    "local_date": day,
                    "query_status": "fetched",
                }
            )
    table = pa.Table.from_pylist(rows)
    rule = CoverageRule()
    kept = covered_counts(table, rule)
    gated = gated_keys(table, rule)
    assert "2025-08-01" in kept["arrival"]
    assert ("2025-08-01", "arrival") not in gated
    assert ("2025-08-02", "arrival") in gated
    loose = rule.model_copy(update={"sparse_fraction": 0.01})
    assert "2025-08-02" in covered_counts(table, loose)["arrival"]
    assert ("2025-08-02", "arrival") not in gated_keys(table, loose)
    config = ResampleConfig(coverage=loose)
    assert gated_keys(table, config.coverage) == gated_keys(table, loose)


def test_memory_source_round_trip():
    policy = load_policy()
    resolved = MemoryConfigSource(policy).load()
    assert resolved.kinds["resample_config"].source == "memory"
    assert resolved.model.scenario_policy.multiplier_floor == 0.2

"""Postgres model config. Unit tests need no database.

Integration tests skip when Postgres is down.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import psycopg
import pytest

import resample_fixtures as fx
from flowstate.config import Settings
from flowstate.config.model import (
    BUS_PRIOR_FIELDS,
    ResampleConfig,
    canonical_json,
)
from flowstate.config.postgres import (
    SCHEMA_VERSION,
    ConfigStore,
    PostgresConfigSource,
    field_diff,
    validate_document,
)
from flowstate.config.resolve import (
    ConfigNotActiveError,
    ConfigUnavailableError,
    ConfigValidationError,
    open_config_source,
    resolve_models,
)
from flowstate.config.seed import seed
from flowstate.resample import build_bundle, bundle_hash
from flowstate.resample.bundle import BundleCache
from flowstate.resample.scenario import load_policy
from flowstate.streaming.control import ControlMessage, StartRequest
from flowstate.streaming.producer import ReplayRequest, emit_plan
from flowstate.streaming.service import Runtime, Service
from flowstate.streaming.topics import AUDIT_TOPIC

_LOCAL_URL = (
    "postgresql://flowstate:flowstate@127.0.0.1:5432/flowstate"
)
_PRIOR_ENV = tuple(
    "FLOWSTATE_" + name.upper() for name in BUS_PRIOR_FIELDS
)
_SECRET = "s3cret-token"


def _clear_prior(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in _PRIOR_ENV:
        monkeypatch.delenv(key, raising=False)


def _policy_doc() -> dict:
    return load_policy().model_dump(mode="json")


def _event_keys(plan) -> list[tuple[str, str]]:
    keys = []
    for topic in plan.by_topic:
        keys.extend(
            (event["event_time"], event["event_id"])
            for event in plan.by_topic[topic]
        )
    return keys


def test_schema_version_matches_init_sql():
    text = Path("infra/postgres/init.sql").read_text()
    assert f"VALUES ({SCHEMA_VERSION})" in text


def test_the_runtime_source_is_postgres():
    settings = Settings(_env_file=None)
    assert isinstance(open_config_source(settings), PostgresConfigSource)


def test_unreachable_postgres_stops_without_the_secret():
    url = f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    settings = Settings(_env_file=None, config_database_url=url)
    source = open_config_source(settings)
    with pytest.raises(ConfigUnavailableError, match="unreachable") as caught:
        source.load()
    assert _SECRET not in str(caught.value)
    assert "ConfigStore" in repr(ConfigStore(url))
    assert _SECRET not in repr(ConfigStore(url))


def test_a_blank_database_url_uses_the_local_default():
    settings = Settings(_env_file=None, config_database_url="  ")
    assert "127.0.0.1:5432" in settings.config_database_url


def test_an_explicit_empty_url_stops():
    source = PostgresConfigSource(
        Settings(_env_file=None), database_url=""
    )
    with pytest.raises(ConfigUnavailableError, match="empty"):
        source.load()


def test_invalid_documents_raise_a_typed_error():
    unknown = _policy_doc()
    unknown["scenarios"]["made_up"] = dict(
        unknown["scenarios"]["custom_shock"]
    )
    with pytest.raises(ConfigValidationError, match="unknown scenario"):
        validate_document("scenario_policy", unknown)
    policy = _policy_doc()
    shares = dict(policy["scenarios"]["metro_disruption"])
    shares["shift_shares"] = {"ride_hail": 0.8, "bus": 0.8}
    policy = _policy_doc()
    policy["scenarios"]["metro_disruption"] = shares
    with pytest.raises(ConfigValidationError, match="shift shares"):
        validate_document("scenario_policy", policy)
    with pytest.raises(ConfigValidationError, match="less than or equal"):
        validate_document("resample_config", {"cab_share": 2})
    with pytest.raises(ConfigValidationError, match="peak share"):
        validate_document(
            "resample_config",
            {"bus_peak_share_low": 0.9, "bus_peak_share_high": 0.1},
        )
    with pytest.raises(ConfigValidationError, match="unknown kind"):
        validate_document("city_profile", {})
    store = ConfigStore(
        f"postgresql://flowstate:{_SECRET}@127.0.0.1:1/flowstate"
    )
    with pytest.raises(ConfigValidationError, match="unknown scenario"):
        store.put_version("scenario_policy", unknown)
    assert _SECRET not in str(store)


def test_field_diff_names_changed_leaves():
    changes = field_diff(
        {"notes": "a", "coverage": {"sparse_fraction": 0.5}},
        {"notes": "b", "coverage": {"sparse_fraction": 0.5}},
    )
    assert changes == {"notes": {"from": "a", "to": "b"}}


def test_a_pinned_config_replays_the_same_event_ids():
    inputs = fx.fixture_inputs()
    policy = load_policy()
    config = ResampleConfig()
    pinned = resolve_models(
        policy, config, source="db", version_id="4"
    )
    again = resolve_models(
        policy, config, source="db", version_id="9"
    )
    assert pinned.config_hash == again.config_hash
    bundle = build_bundle(
        inputs,
        fx.fixture_spec(),
        config,
        seed=42,
        config_hash=pinned.config_hash,
    )
    assert bundle_hash(bundle, pinned.config_hash) == bundle_hash(
        bundle, again.config_hash
    )
    request = ReplayRequest(
        bundle.version, 42, "2025-08-20", "2025-08-20", 60.0, 500.0
    )
    first = emit_plan(
        bundle,
        inputs,
        request,
        mode="replay",
        rows=1000,
        resolved=pinned,
    )
    second = emit_plan(
        bundle,
        inputs,
        request,
        mode="replay",
        rows=1000,
        resolved=again,
    )
    assert _event_keys(first) == _event_keys(second)
    assert first.audit[0]["config_hash"] == pinned.config_hash
    assert first.audit[0]["config_version"] == {
        "resample_config": "4",
        "scenario_policy": "4",
    }
    edited = policy.model_copy(update={"notes": policy.notes + " x"})
    changed = resolve_models(
        edited, config, source="db", version_id="5"
    )
    assert bundle_hash(bundle, pinned.config_hash) != bundle_hash(
        bundle, changed.config_hash
    )
    assert _SECRET not in canonical_json(config)


@pytest.fixture
def database_url():
    url = os.environ.get("FLOWSTATE_CONFIG_DATABASE_URL", _LOCAL_URL)
    try:
        with psycopg.connect(url, connect_timeout=2) as conn:
            conn.execute("SELECT version FROM schema_version")
    except psycopg.OperationalError:
        pytest.skip("Postgres is not available")
    with psycopg.connect(url) as conn:
        conn.execute(
            "TRUNCATE config_active, config_version RESTART IDENTITY"
        )
    return url


def _seed_hash():
    return resolve_models(
        load_policy(),
        ResampleConfig(),
        source="db",
        version_id=None,
        field_sources={name: "db" for name in ResampleConfig.model_fields},
    ).config_hash


@pytest.mark.integration
def test_an_empty_database_does_not_resolve(database_url):
    settings = Settings(_env_file=None, config_database_url=database_url)
    with pytest.raises(ConfigNotActiveError, match="config-seed"):
        open_config_source(settings).load()
    inserted = seed(database_url)
    assert inserted == ["scenario_policy", "resample_config"]
    resolved = open_config_source(settings).load()
    assert resolved.config_hash == _seed_hash()
    assert resolved.kinds["scenario_policy"].source == "db"
    assert resolved.version_id("scenario_policy") == "1"
    assert seed(database_url) == []


@pytest.mark.integration
def test_a_saved_version_overrides_only_its_kind(
    monkeypatch, database_url
):
    _clear_prior(monkeypatch)
    monkeypatch.setenv("FLOWSTATE_ASSUMED_WEEKDAY_BOARDINGS", "3310000")
    store = ConfigStore(database_url)
    resample = store.put_version(
        "resample_config", ResampleConfig().model_dump(mode="json")
    )
    policy = _policy_doc()
    policy["notes"] = policy["notes"] + " from db"
    saved = store.put_version("scenario_policy", policy, note="policy")
    settings = Settings(_env_file=None, config_database_url=database_url)
    resolved = PostgresConfigSource(
        settings, database_url=database_url
    ).load()
    assert resolved.kinds["scenario_policy"].source == "db"
    assert resolved.version_id("scenario_policy") == saved.id
    assert resolved.model.scenario_policy.notes.endswith("from db")
    assert resolved.kinds["resample_config"].source == "db"
    assert resolved.version_id("resample_config") == resample.id
    assert (
        resolved.model.resample_config.assumed_weekday_boardings
        == 4_600_000
    )
    assert resolved.field_sources["assumed_weekday_boardings"] == "db"
    stored = ResampleConfig().model_dump(mode="json")
    resample = store.put_version("resample_config", stored)
    resolved = PostgresConfigSource(
        settings, database_url=database_url
    ).load()
    assert resolved.kinds["resample_config"].source == "db"
    assert resolved.version_id("resample_config") == resample.id
    assert (
        resolved.model.resample_config.assumed_weekday_boardings
        == 4_600_000
    )
    assert set(resolved.field_sources.values()) == {"db"}
    assert database_url not in canonical_json(
        resolved.model.resample_config
    )


@pytest.mark.integration
def test_versions_are_immutable_and_activation_restores_the_hash(
    monkeypatch, database_url
):
    _clear_prior(monkeypatch)
    store = ConfigStore(database_url)
    settings = Settings(_env_file=None, config_database_url=database_url)
    store.put_version(
        "resample_config", ResampleConfig().model_dump(mode="json")
    )
    original = _policy_doc()
    first = store.put_version("scenario_policy", original, note="kept")
    file_hash = _seed_hash()
    edited = dict(original)
    edited["notes"] = original["notes"] + " edited"
    second = store.put_version("scenario_policy", edited)
    assert first.id != second.id
    conn = psycopg.connect(database_url)
    try:
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "UPDATE config_version SET note = 'nope' WHERE id = %s",
                (int(first.id),),
            )
        conn.rollback()
        with pytest.raises(psycopg.Error, match="immutable"):
            conn.execute(
                "DELETE FROM config_version WHERE id = %s",
                (int(first.id),),
            )
        conn.rollback()
        row = conn.execute(
            "SELECT note FROM config_version WHERE id = %s",
            (int(first.id),),
        ).fetchone()
    finally:
        conn.close()
    assert row[0] == "kept"
    listed = store.list_versions("scenario_policy")
    assert [item.id for item in listed] == [second.id, first.id]
    active = PostgresConfigSource(
        settings, database_url=database_url
    ).load()
    assert active.version_id("scenario_policy") == second.id
    assert active.config_hash != file_hash
    restored = store.activate("scenario_policy", first.id)
    assert restored.id == first.id
    back = PostgresConfigSource(
        settings, database_url=database_url
    ).load()
    assert back.config_hash == file_hash
    assert back.kinds["scenario_policy"].source == "db"
    store.deactivate("scenario_policy")
    with pytest.raises(ConfigNotActiveError, match="scenario_policy"):
        PostgresConfigSource(settings, database_url=database_url).load()
    assert store.list_versions("scenario_policy")
    with pytest.raises(ConfigValidationError, match="no scenario_policy"):
        store.activate("scenario_policy", "999999")
    changes = store.diff("scenario_policy", first.id, second.id)
    assert "notes" in changes["changes"]
    same = store.put_version("scenario_policy", original)
    assert same.sha256 == first.sha256
    assert same.id != first.id
    same_hash = PostgresConfigSource(
        settings, database_url=database_url
    ).load().config_hash
    store.activate("scenario_policy", first.id)
    other_hash = PostgresConfigSource(
        settings, database_url=database_url
    ).load().config_hash
    assert same_hash == other_hash == file_hash
    moved = store.put_version("scenario_policy", edited)
    moved_hash = PostgresConfigSource(
        settings, database_url=database_url
    ).load().config_hash
    assert moved_hash != file_hash
    assert moved.sha256 != first.sha256


@pytest.mark.integration
def test_invalid_input_adds_no_row(database_url):
    store = ConfigStore(database_url)
    before = store.list_versions("scenario_policy")
    policy = _policy_doc()
    policy["scenarios"]["made_up"] = dict(
        policy["scenarios"]["custom_shock"]
    )
    with pytest.raises(ConfigValidationError):
        store.put_version("scenario_policy", policy)
    assert store.list_versions("scenario_policy") == before
    with pytest.raises(ConfigValidationError):
        store.put_version(
            "resample_config",
            {"bus_peak_share_low": 0.9, "bus_peak_share_high": 0.1},
        )
    assert store.list_versions("resample_config") == []


@pytest.mark.integration
def test_schema_problems_stop_the_process(database_url):
    with psycopg.connect(database_url) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS empty_config")
    missing = database_url + "?options=-csearch_path%3Dempty_config"
    settings = Settings(_env_file=None)
    with pytest.raises(ConfigUnavailableError, match="schema is missing"):
        PostgresConfigSource(settings, database_url=missing).load()
    conn = psycopg.connect(database_url)
    try:
        conn.execute("UPDATE schema_version SET version = 99")
        conn.commit()
        with pytest.raises(ConfigUnavailableError, match="schema version"):
            PostgresConfigSource(
                settings, database_url=database_url
            ).load()
    finally:
        conn.execute(
            "UPDATE schema_version SET version = %s",
            (SCHEMA_VERSION,),
        )
        conn.commit()
        conn.close()


@pytest.mark.integration
def test_audits_list_the_version_and_replay_matches(
    monkeypatch, database_url
):
    _clear_prior(monkeypatch)
    store = ConfigStore(database_url)
    policy = load_policy().model_copy(
        update={"kia_catchment_wards": ["b_1", "b_2", "b_3", "b_4"]}
    )
    saved = store.put_version(
        "scenario_policy", policy.model_dump(mode="json"), note="pin"
    )
    resample = store.put_version(
        "resample_config", ResampleConfig().model_dump(mode="json")
    )
    settings = Settings(_env_file=None)
    source = PostgresConfigSource(settings, database_url=database_url)
    resolved = source.load()
    assert database_url not in json.dumps(
        {
            "hash": resolved.config_hash,
            "versions": {
                kind: resolved.version_id(kind)
                for kind in resolved.kinds
            },
        }
    )
    inputs = fx.fixture_inputs()
    cache = BundleCache()
    bundle = cache.bundle(
        inputs,
        fx.fixture_spec(),
        resolved.model.resample_config,
        seed=42,
        config_hash=resolved.config_hash,
    )
    runtime = Runtime(
        settings=settings,
        inputs=inputs,
        pins=fx.fixture_pins(),
        bundle=bundle,
        policy=resolved.model.scenario_policy,
        source=source,
        bundle_cache=cache,
    )
    recorder = _Recorder()
    moment = datetime.fromisoformat("2026-10-27T10:00:00+05:30")
    service = Service(
        runtime,
        recorder,
        clock=lambda: moment,
        idle_wait=0,
        status_every=1e9,
    )
    service.handle(
        (
            "control",
            ControlMessage(
                op="start",
                request_id="r-start",
                start=StartRequest(
                    mode="replay",
                    rows=1000,
                    seed=42,
                    window_start="2025-08-20",
                    window_end="2025-08-20",
                    session_id="pin",
                ),
            ),
        )
    )
    starts = [
        value
        for topic, _, value in recorder.sent
        if topic == AUDIT_TOPIC and value["record_type"] == "start"
    ]
    assert starts
    assert starts[0]["config_hash"] == resolved.config_hash
    assert starts[0]["config_version"] == {
        "scenario_policy": saved.id,
        "resample_config": resample.id,
    }
    assert cache.builds == 1
    request = ReplayRequest(
        bundle.version, 42, "2025-08-20", "2025-08-20", 60.0, 500.0
    )
    first = emit_plan(
        runtime.bundle,
        inputs,
        request,
        mode="replay",
        rows=1000,
        resolved=resolved,
    )
    second = emit_plan(
        runtime.bundle,
        inputs,
        request,
        mode="replay",
        rows=1000,
        resolved=source.load(),
    )
    assert _event_keys(first) == _event_keys(second)
    assert first.audit[0]["config_version"]["scenario_policy"] == saved.id
    assert first.audit[0]["config_hash"] == second.audit[0]["config_hash"]
    listed = json.dumps(
        [item.__dict__ for item in store.list_versions("scenario_policy")]
    )
    assert database_url not in listed
    assert _SECRET not in listed


class _Future:
    is_done = True
    exception = None

    def succeeded(self) -> bool:
        return True

    def get(self, timeout=None):
        return None


class _Recorder:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))
        return _Future()

    def flush(self):
        return None

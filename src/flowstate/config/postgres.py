"""Postgres store for model config.

An active row is the document for that kind. A kind with no active
row stops the caller. ``make config-seed`` inserts the first versions.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json
from pydantic import BaseModel, ValidationError

from flowstate.config.model import (
    ResampleConfig,
    content_sha256,
)
from flowstate.config.resolve import (
    ConfigNotActiveError,
    ConfigUnavailableError,
    ConfigValidationError,
    Resolved,
    resolve_models,
)

SCHEMA_VERSION = 3
KINDS = ("scenario_policy", "resample_config")
_CONNECT_TIMEOUT = 3


def validate_document(kind: str, content: Any) -> BaseModel:
    """Validate one kind. Raise ``ConfigValidationError`` on bad input."""
    if kind not in KINDS:
        raise ConfigValidationError(f"unknown kind {kind}")
    if not isinstance(content, dict):
        raise ConfigValidationError(f"{kind} must be a document")
    from flowstate.resample.scenario import ScenarioPolicy

    model_cls = ScenarioPolicy if kind == "scenario_policy" else ResampleConfig
    try:
        return model_cls.model_validate(content)
    except ValidationError as exc:
        raise ConfigValidationError(_validation_message(exc)) from None


def field_diff(
    left: Any, right: Any, prefix: str = ""
) -> dict[str, dict[str, Any]]:
    """Changed leaves. A missing side is ``None``."""
    if isinstance(left, dict) and isinstance(right, dict):
        changes: dict[str, dict[str, Any]] = {}
        for key in sorted(set(left) | set(right)):
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in left:
                changes[path] = {"from": None, "to": right[key]}
            elif key not in right:
                changes[path] = {"from": left[key], "to": None}
            else:
                changes.update(field_diff(left[key], right[key], path))
        return changes
    if left != right:
        return {prefix or "$": {"from": left, "to": right}}
    return {}


@dataclass(frozen=True)
class ConfigVersion:
    id: str
    kind: str
    sha256: str
    created_at: str
    note: str | None
    content: dict[str, Any]


class ConfigStore:
    """Service layer. No HTTP."""

    def __init__(self, database_url: str) -> None:
        self._url = database_url

    @property
    def database_url(self) -> str | None:
        return self._url

    def __repr__(self) -> str:
        return "ConfigStore(url hidden)"

    def put_version(
        self,
        kind: str,
        content: Any,
        note: str | None = None,
    ) -> ConfigVersion:
        """Validate, insert, and activate. No row on invalid input."""
        model = validate_document(kind, content)
        payload = model.model_dump(mode="json")
        digest = content_sha256(model)
        saved_note = _note(note)
        with self._session() as conn:
            with conn.transaction():
                row = conn.execute(
                    """
                    INSERT INTO config_version
                        (kind, content, sha256, note)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, kind, content, sha256, created_at, note
                    """,
                    (kind, Json(payload), digest, saved_note),
                ).fetchone()
                version = _as_version(row)
                _activate(conn, kind, int(version.id))
        return version

    def list_versions(self, kind: str) -> list[ConfigVersion]:
        _require_kind(kind)
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT id, kind, content, sha256, created_at, note
                FROM config_version
                WHERE kind = %s
                ORDER BY created_at DESC, id DESC
                """,
                (kind,),
            ).fetchall()
        return [_as_version(row) for row in rows]

    def diff(self, kind: str, from_id: str, to_id: str) -> dict[str, Any]:
        _require_kind(kind)
        with self._session() as conn:
            left = _fetch(conn, kind, from_id)
            right = _fetch(conn, kind, to_id)
        return {
            "kind": kind,
            "from": left.id,
            "to": right.id,
            "changes": field_diff(left.content, right.content),
        }

    def activate(self, kind: str, version_id: str) -> ConfigVersion:
        """Make one version of this kind active."""
        _require_kind(kind)
        with self._session() as conn:
            with conn.transaction():
                version = _fetch(conn, kind, version_id)
                _activate(conn, kind, int(version.id))
        return version

    def deactivate(self, kind: str) -> None:
        """Clear the active pointer. Versions stay."""
        _require_kind(kind)
        with self._session() as conn:
            with conn.transaction():
                conn.execute(
                    "DELETE FROM config_active WHERE kind = %s",
                    (kind,),
                )

    def active(self) -> dict[str, ConfigVersion]:
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT v.id, v.kind, v.content, v.sha256,
                       v.created_at, v.note
                FROM config_active AS a
                JOIN config_version AS v ON v.id = a.version_id
                """
            ).fetchall()
        found: dict[str, ConfigVersion] = {}
        for row in rows:
            version = _as_version(row)
            _check_stored(version)
            found[version.kind] = version
        return found

    def replay_seed(self) -> int | None:
        with self._session() as conn:
            row = conn.execute(
                "SELECT replay_seed FROM run_config WHERE id = 1"
            ).fetchone()
        if row is None:
            return None
        return int(row["replay_seed"])

    def set_replay_seed(self, seed: int) -> None:
        with self._session() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO run_config (id, replay_seed)
                    VALUES (1, %s)
                    ON CONFLICT (id)
                    DO UPDATE SET replay_seed = EXCLUDED.replay_seed
                    """,
                    (seed,),
                )

    def clear_run_config(self) -> None:
        with self._session() as conn:
            with conn.transaction():
                conn.execute("DELETE FROM run_config")

    def benchmarks(self) -> list[dict[str, Any]]:
        with self._session() as conn:
            rows = conn.execute(
                """
                SELECT run_id, engine, corpus_rows, seed,
                       elapsed_seconds, created_at
                FROM benchmark
                ORDER BY id DESC
                """
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            created = row["created_at"]
            if hasattr(created, "isoformat"):
                created = created.isoformat()
            out.append(
                {
                    "run_id": row["run_id"],
                    "engine": row["engine"],
                    "corpus_rows": row["corpus_rows"],
                    "seed": row["seed"],
                    "elapsed_seconds": row["elapsed_seconds"],
                    "created_at": str(created),
                }
            )
        return out

    @contextmanager
    def _session(self):
        conn = _connect(self._url)
        try:
            yield conn
        finally:
            conn.close()


def resolve_active(store: ConfigStore) -> Resolved:
    """Both kinds must have an active version."""
    active = store.active()
    missing = [kind for kind in KINDS if kind not in active]
    if missing:
        raise ConfigNotActiveError(
            "no active " + ", ".join(missing) + ". Run make config-seed."
        )
    policy = validate_document(
        "scenario_policy", active["scenario_policy"].content
    )
    resample = validate_document(
        "resample_config", active["resample_config"].content
    )
    return resolve_models(
        policy,
        resample,
        source="db",
        version_id=None,
        field_sources={name: "db" for name in ResampleConfig.model_fields},
        kind_meta={
            "scenario_policy": ("db", active["scenario_policy"].id),
            "resample_config": ("db", active["resample_config"].id),
        },
    )


class PostgresConfigSource:
    """Read the active versions from Postgres."""

    def __init__(
        self,
        settings: Any | None = None,
        database_url: str | None = None,
    ) -> None:
        self.settings = settings
        self.database_url = database_url

    def load(self) -> Resolved:
        from flowstate.config import get_settings

        settings = self.settings
        if settings is None:
            settings = get_settings()
        if self.database_url is None:
            url = settings.config_database_url
        else:
            url = self.database_url
        if not str(url or "").strip():
            raise ConfigUnavailableError(
                "FLOWSTATE_CONFIG_DATABASE_URL is empty"
            )
        return resolve_active(ConfigStore(url))


def _connect(url: str):
    try:
        # autocommit so the schema check does not leave a transaction
        # open. A later transaction() commits the write. close() rolls
        # back a transaction that is still open.
        conn = psycopg.connect(
            url,
            autocommit=True,
            connect_timeout=_CONNECT_TIMEOUT,
            row_factory=dict_row,
        )
    except psycopg.Error:
        raise ConfigUnavailableError("Postgres is unreachable") from None
    try:
        _check_schema(conn)
    except Exception:
        conn.close()
        raise
    return conn


def _check_schema(conn) -> None:
    try:
        rows = conn.execute(
            "SELECT version FROM schema_version"
        ).fetchall()
    except psycopg.Error:
        conn.rollback()
        raise ConfigUnavailableError("Postgres schema is missing") from None
    versions = [row["version"] for row in rows]
    if versions != [SCHEMA_VERSION]:
        found = versions[0] if len(versions) == 1 else versions
        raise ConfigUnavailableError(
            f"Postgres schema version {found} "
            f"does not match {SCHEMA_VERSION}"
        )


def _require_kind(kind: str) -> None:
    if kind not in KINDS:
        raise ConfigValidationError(f"unknown kind {kind}")


def _note(note: str | None) -> str | None:
    if note is None:
        return None
    text = str(note).strip()
    return text or None


def _activate(conn, kind: str, version_id: int) -> None:
    conn.execute(
        """
        INSERT INTO config_active (kind, version_id)
        VALUES (%s, %s)
        ON CONFLICT (kind)
        DO UPDATE SET version_id = EXCLUDED.version_id
        """,
        (kind, version_id),
    )


def _fetch(conn, kind: str, version_id: str) -> ConfigVersion:
    try:
        ident = int(version_id)
    except (TypeError, ValueError):
        raise ConfigValidationError(
            "version id must be an integer"
        ) from None
    row = conn.execute(
        """
        SELECT id, kind, content, sha256, created_at, note
        FROM config_version
        WHERE id = %s
        """,
        (ident,),
    ).fetchone()
    if row is None or row["kind"] != kind:
        raise ConfigValidationError(f"no {kind} version {version_id}")
    return _as_version(row)


def _check_stored(version: ConfigVersion) -> None:
    try:
        model = validate_document(version.kind, version.content)
    except ConfigValidationError:
        raise ConfigUnavailableError(
            f"stored {version.kind} failed validation"
        ) from None
    if content_sha256(model) != version.sha256:
        raise ConfigUnavailableError(
            f"stored {version.kind} sha256 does not match"
        )


def _as_version(row: dict[str, Any]) -> ConfigVersion:
    created = row["created_at"]
    if hasattr(created, "isoformat"):
        created = created.isoformat()
    content = row["content"]
    if isinstance(content, str):
        content = json.loads(content)
    return ConfigVersion(
        id=str(row["id"]),
        kind=row["kind"],
        sha256=row["sha256"],
        created_at=str(created),
        note=row["note"],
        content=content,
    )


def _validation_message(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        loc = ".".join(str(item) for item in err["loc"])
        parts.append(f"{loc}: {err['msg']}")
    return "; ".join(parts) or "invalid config"

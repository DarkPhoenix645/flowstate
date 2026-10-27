"""Resolve model config from Postgres, or from objects in tests.

The process reads active versions only. ``make config-seed`` loads
the first versions. There is no file source.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from flowstate.config.model import (
    ResampleConfig,
    config_hash_from,
    content_sha256,
)


class ModelConfig(BaseModel):
    """The two v1 kinds, already validated."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    scenario_policy: Any
    resample_config: ResampleConfig


@dataclass(frozen=True)
class KindInfo:
    sha256: str
    source: str
    version_id: str | None


@dataclass(frozen=True)
class Resolved:
    """What ``resolve`` returns."""

    model: ModelConfig
    kinds: dict[str, KindInfo]
    config_hash: str
    field_sources: dict[str, str] = field(default_factory=dict)

    def version_id(self, kind: str) -> str | None:
        return self.kinds[kind].version_id


class ConfigValidationError(Exception):
    """Invalid model config. No row is written."""


class ConfigUnavailableError(Exception):
    """Postgres cannot be used."""


class ConfigNotActiveError(ConfigUnavailableError):
    """A kind has no active version."""


class ConfigSource(Protocol):
    def load(self) -> Resolved:
        """Return the resolved config."""


def _db_fields() -> dict[str, str]:
    return {name: "db" for name in ResampleConfig.model_fields}


def resolve_models(
    policy: Any,
    resample: ResampleConfig,
    *,
    source: str,
    version_id: str | None,
    field_sources: dict[str, str] | None = None,
    kind_meta: dict[str, tuple[str, str | None]] | None = None,
) -> Resolved:
    policy_sha = content_sha256(policy)
    resample_sha = content_sha256(resample)
    parts = {
        "resample_config": resample_sha,
        "scenario_policy": policy_sha,
    }
    if kind_meta is None:
        kinds = {
            kind: KindInfo(parts[kind], source, version_id)
            for kind in parts
        }
    else:
        kinds = {
            kind: KindInfo(
                parts[kind], kind_meta[kind][0], kind_meta[kind][1]
            )
            for kind in parts
        }
    return Resolved(
        model=ModelConfig(
            scenario_policy=policy, resample_config=resample
        ),
        kinds=kinds,
        config_hash=config_hash_from(parts),
        field_sources=field_sources or _db_fields(),
    )


class MemoryConfigSource:
    """In-memory fake. Tests pass the objects they want resolved."""

    def __init__(
        self,
        policy: Any,
        resample: ResampleConfig | None = None,
        field_sources: dict[str, str] | None = None,
    ) -> None:
        self.policy = policy
        self.resample = resample if resample is not None else ResampleConfig()
        self.field_sources = field_sources

    def load(self) -> Resolved:
        sources = self.field_sources
        if sources is None:
            sources = {name: "default" for name in ResampleConfig.model_fields}
        return resolve_models(
            self.policy,
            self.resample,
            source="memory",
            version_id=None,
            field_sources=sources,
        )


def open_config_source(settings: Any | None = None) -> ConfigSource:
    """The Postgres source. This is the only runtime source."""
    if settings is None:
        from flowstate.config import get_settings

        settings = get_settings()
    from flowstate.config.postgres import PostgresConfigSource

    return PostgresConfigSource(settings)

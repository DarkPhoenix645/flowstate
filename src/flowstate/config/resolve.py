"""Resolve model config from files and env, or from objects in tests.

A file source reports ``file`` and no version id. Each
``resample_config`` field is ``default`` or ``env``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from flowstate.config.model import (
    BUS_PRIOR_FIELDS,
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


class ConfigSource(Protocol):
    def load(self) -> Resolved:
        """Return the resolved config."""


def _field_sources_from_env() -> dict[str, str]:
    sources: dict[str, str] = {}
    for name in ResampleConfig.model_fields:
        env = "FLOWSTATE_" + name.upper()
        raw = os.environ.get(env)
        present = raw is not None and raw.strip() != ""
        if name in BUS_PRIOR_FIELDS and present:
            sources[name] = "env"
        else:
            sources[name] = "default"
    return sources


def resolve_models(
    policy: Any,
    resample: ResampleConfig,
    *,
    source: str,
    version_id: str | None,
    field_sources: dict[str, str] | None = None,
) -> Resolved:
    policy_sha = content_sha256(policy)
    resample_sha = content_sha256(resample)
    parts = {
        "resample_config": resample_sha,
        "scenario_policy": policy_sha,
    }
    kinds = {
        kind: KindInfo(parts[kind], source, version_id) for kind in parts
    }
    return Resolved(
        model=ModelConfig(
            scenario_policy=policy, resample_config=resample
        ),
        kinds=kinds,
        config_hash=config_hash_from(parts),
        field_sources=field_sources or _field_sources_from_env(),
    )


class FileConfigSource:
    """Today's behaviour. Policy file plus env on the nine prior fields."""

    def __init__(
        self,
        settings: Any | None = None,
        policy_path: Path | None = None,
    ) -> None:
        self.settings = settings
        self.policy_path = policy_path

    def load(self) -> Resolved:
        from flowstate.config import get_settings
        from flowstate.resample.scenario import load_policy

        settings = self.settings
        if settings is None:
            settings = get_settings()
        policy = load_policy(self.policy_path)
        resample = ResampleConfig.from_settings(settings)
        return resolve_models(
            policy,
            resample,
            source="file",
            version_id=None,
            field_sources=_field_sources_from_env(),
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

"""Jobs the API may start. A name missing from this file cannot run."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from flowstate.config.resolve import ConfigValidationError
from flowstate.ingest.amplify import AmplifyRequest
from flowstate.streaming.control import StartRequest

ALLOWED_ROWS = (1_000_000, 10_000_000, 100_000_000)


class AnalyticsRequest(BaseModel):
    """No caller fields. The corpus identity comes from the lake manifest."""

    model_config = ConfigDict(extra="forbid")


class HdfsProbeRequest(BaseModel):
    """No parameters. The probe writes a fixed HDFS path."""

    model_config = ConfigDict(extra="forbid")


class JobSpec:
    def __init__(
        self,
        name: str,
        kind: str,
        engine: str,
        entrypoint: str,
        params_model: type[BaseModel],
    ) -> None:
        self.name = name
        self.kind = kind
        self.engine = engine
        self.entrypoint = entrypoint
        self.params_model = params_model


JOBS: dict[str, JobSpec] = {
    "amplify": JobSpec(
        name="amplify",
        kind="amplify",
        engine="python",
        entrypoint="flowstate.ingest.amplify",
        params_model=AmplifyRequest,
    ),
    "demand_hotspots": JobSpec(
        name="demand_hotspots",
        kind="analytics",
        engine="spark",
        entrypoint="flowstate.batch.spark_jobs.demand_hotspots",
        params_model=AnalyticsRequest,
    ),
    "cancellation_analysis": JobSpec(
        name="cancellation_analysis",
        kind="analytics",
        engine="spark",
        entrypoint="flowstate.batch.spark_jobs.cancellation_analysis",
        params_model=AnalyticsRequest,
    ),
    "ridership_trends": JobSpec(
        name="ridership_trends",
        kind="analytics",
        engine="spark",
        entrypoint="flowstate.batch.spark_jobs.ridership_trends",
        params_model=AnalyticsRequest,
    ),
    "cross_modal": JobSpec(
        name="cross_modal",
        kind="analytics",
        engine="spark",
        entrypoint="flowstate.batch.spark_jobs.cross_modal",
        params_model=AnalyticsRequest,
    ),
    "hdfs_probe": JobSpec(
        name="hdfs_probe",
        kind="analytics",
        engine="spark",
        entrypoint="flowstate.batch.spark_jobs.hdfs_probe",
        params_model=HdfsProbeRequest,
    ),
    "replay": JobSpec(
        name="replay",
        kind="replay",
        engine="python",
        entrypoint="flowstate.streaming.service",
        params_model=StartRequest,
    ),
}


def get_job(name: str) -> JobSpec:
    spec = JOBS.get(name)
    if spec is None:
        raise ConfigValidationError(f"{name} is not in the registry")
    return spec


def validate_job(kind: str, name: str, params: Any) -> JobSpec:
    spec = get_job(name)
    if spec.kind != kind:
        raise ConfigValidationError(
            f"{name} is a {spec.kind} job, not {kind}"
        )
    if not isinstance(params, dict):
        raise ConfigValidationError("params must be an object")
    try:
        model = spec.params_model.model_validate(params)
    except Exception as exc:
        raise ConfigValidationError(str(exc)) from None
    if spec.name == "amplify" and model.rows not in ALLOWED_ROWS:
        raise ConfigValidationError(
            "rows must be 1000000, 10000000, or 100000000"
        )
    return spec


def job_catalog() -> list[dict[str, Any]]:
    rows = []
    for spec in JOBS.values():
        rows.append(
            {
                "name": spec.name,
                "kind": spec.kind,
                "engine": spec.engine,
                "entrypoint": spec.entrypoint,
                "parameters": spec.params_model.model_json_schema(),
            }
        )
    return rows

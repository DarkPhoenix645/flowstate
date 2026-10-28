"""Job backends. The API calls the runner over HTTP."""

from __future__ import annotations

import json
import re
import threading
import urllib.error
import urllib.request
import uuid
from typing import Any, Protocol

from flowstate.config.resolve import ConfigValidationError
from flowstate.jobs.registry import JobSpec


class JobBackend(Protocol):
    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        """Start the job. Return an external id."""

    def status(self, external_id: str) -> dict[str, Any] | None:
        """Running, succeeded, failed, or cancelled. None if unknown."""

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        """Ask the backend to stop the job."""

    def log_tail(self, external_id: str) -> str:
        """Last lines of the job log."""


class FakeBackend:
    """In-memory backend for tests."""

    def __init__(
        self,
        result: str = "succeeded",
        log: str = "",
        hold: threading.Event | None = None,
    ) -> None:
        self.result = result
        self.log = log
        self.hold = hold
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._jobs: dict[str, dict[str, Any]] = {}

    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        self.calls.append((spec.name, params))
        external_id = f"fake-{len(self.calls)}"
        self._jobs[external_id] = {
            "status": "running",
            "log": "",
            "result_location": None,
        }

        def finish() -> None:
            if self.hold is not None:
                self.hold.wait(timeout=5)
            job = self._jobs.get(external_id)
            if job is None or job["status"] == "cancelled":
                return
            job["status"] = self.result
            job["log"] = self.log

        threading.Thread(target=finish, daemon=True).start()
        return external_id

    def status(self, external_id: str) -> dict[str, Any] | None:
        return self._jobs.get(external_id)

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(external_id)
        if job is None:
            return None
        job["status"] = "cancelled"
        return job

    def log_tail(self, external_id: str) -> str:
        job = self._jobs.get(external_id)
        if job is None:
            return ""
        return str(job["log"])


class ComposeBackend:
    """Host side of the runner. Cluster commands go over HTTP."""

    def __init__(self, runner_url: str) -> None:
        self._url = runner_url.rstrip("/")
        self._jobs: dict[str, dict[str, Any]] = {}

    def submit(self, spec: JobSpec, params: dict[str, Any]) -> str:
        external_id = str(uuid.uuid4())
        self._jobs[external_id] = {
            "status": "running",
            "log": "",
            "result_location": None,
        }
        threading.Thread(
            target=self._finish,
            args=(external_id, spec, params),
            daemon=True,
        ).start()
        return external_id

    def _finish(
        self, external_id: str, spec: JobSpec, params: dict[str, Any]
    ) -> None:
        try:
            if spec.name == "amplify":
                location, log = _amplify(self._url, params)
                state = "succeeded"
            elif spec.kind == "stream":
                location, log = _stream_job(
                    self._url, spec, params, external_id
                )
                state = "succeeded"
            elif str(params.get("engine") or spec.engine) == "mapreduce":
                location, log = _mr_job(
                    self._url, spec, params, external_id
                )
                state = "succeeded"
            elif spec.engine == "spark":
                location, log = _spark_job(self._url, spec, params)
                state = "succeeded"
            else:
                raise RuntimeError(f"compose has no handler for {spec.name}")
        except Exception as exc:
            self._store(external_id, "failed", str(exc), None)
            return
        self._store(external_id, state, log, location)

    def _store(
        self,
        external_id: str,
        status: str,
        log: str,
        location: str | None,
    ) -> None:
        job = self._jobs.get(external_id)
        if job is None or job["status"] == "cancelled":
            return
        self._jobs[external_id] = {
            "status": status,
            "log": log,
            "result_location": location,
        }

    def status(self, external_id: str) -> dict[str, Any] | None:
        return self._jobs.get(external_id)

    def cancel(self, external_id: str) -> dict[str, Any] | None:
        job = self._jobs.get(external_id)
        if job is None or job["status"] != "running":
            return job
        try:
            payload = _post_payload(
                self._url, {"op": "yarn_kill", "track": external_id}
            )
            job["log"] = str(payload.get("log") or job.get("log") or "")
            app = payload.get("application_id")
            if isinstance(app, str):
                job["application_id"] = app
        except Exception as exc:
            job["log"] = str(exc)
        try:
            _post_payload(
                self._url, {"op": "spark_kill", "track": external_id}
            )
        except Exception:
            pass
        job["status"] = "cancelled"
        return job

    def log_tail(self, external_id: str) -> str:
        job = self._jobs.get(external_id)
        if job is None:
            return ""
        return str(job["log"])


def configured_backends(runner_url: str) -> dict[str, JobBackend]:
    """compose is the only configured backend. dataproc is #72."""
    return {"compose": ComposeBackend(runner_url)}


_PROCESS: dict[str, JobBackend] | None = None


def process_backends(runner_url: str) -> dict[str, JobBackend]:
    """One backend set for the API process. The poller and requests share it."""
    global _PROCESS
    if _PROCESS is None:
        _PROCESS = configured_backends(runner_url)
    return _PROCESS


def require_backend(
    backends: dict[str, JobBackend], name: str
) -> JobBackend:
    backend = backends.get(name)
    if backend is None:
        raise ConfigValidationError(f"backend {name} is not configured")
    return backend


_ANALYTICS_DDL = {
    "demand_hotspots": ("analytics_demand_hotspots.sql",),
    "cancellation_analysis": ("analytics_cancellation_analysis.sql",),
    "ridership_trends": ("analytics_ridership_trends.sql",),
    "cross_modal": (
        "analytics_ridehail_metro.sql",
        "analytics_flights_ridehail.sql",
    ),
    "scenario_effect": ("analytics_scenario_effect.sql",),
}


def _spark_job(
    runner_url: str, spec: JobSpec, params: dict[str, Any]
) -> tuple[str, str]:
    script = "/opt/src/" + spec.entrypoint.replace(".", "/") + ".py"
    body: dict[str, Any] = {"op": "spark_submit", "script": script}
    if spec.name == "scenario_effect":
        replay = params.get("replay_run_id")
        if not replay:
            raise RuntimeError("scenario_effect needs replay_run_id")
        body["args"] = [
            *_analytics_args(params),
            f"--replay-run-id={replay}",
        ]
    elif spec.name != "hdfs_probe":
        body["args"] = _analytics_args(params)
    logs = [_post(runner_url, body)]
    for name in _ANALYTICS_DDL.get(spec.name, ()):
        logs.append(
            _post(
                runner_url,
                {
                    "op": "beeline",
                    "file": f"/opt/src/flowstate/warehouse/ddl/{name}",
                },
            )
        )
    if spec.name == "hdfs_probe":
        location = "hdfs://namenode:9000/tmp/spark_hdfs_probe"
    elif spec.name == "cross_modal":
        location = (
            "hdfs://namenode:9000/user/hive/warehouse/"
            "analytics_ridehail_metro"
        )
    else:
        table = _ANALYTICS_DDL[spec.name][0].removesuffix(".sql")
        location = f"hdfs://namenode:9000/user/hive/warehouse/{table}"
    return location, "\n".join(logs)


# Compose URIs. The jar does not contain them. #72 passes gs:// at 100M.
_MR_INPUT = "hdfs://namenode:9000/lake/rides_amplified"
_MR_WAREHOUSE = "hdfs://namenode:9000/user/hive/warehouse"
_MR_JAR = (
    "/opt/src/flowstate/batch/mapreduce/target/"
    "flowstate-mapreduce-0.1.0.jar"
)
_MR_MAIN = "com.flowstate.mapreduce.AnalyticsDriver"


_YARN_APP = re.compile(r"application_\d+_\d+")


def yarn_application_id(log: str) -> str | None:
    found = _YARN_APP.findall(log)
    return found[-1] if found else None


def mr_submit(
    spec: JobSpec, params: dict[str, Any], track: str = ""
) -> dict[str, Any]:
    """Runner body for one MapReduce analytics job."""
    if spec.name == "cross_modal":
        output = _MR_WAREHOUSE
    else:
        table = _ANALYTICS_DDL[spec.name][0].removesuffix(".sql")
        output = f"{_MR_WAREHOUSE}/{table}"
    args = [
        f"--job={spec.name}",
        f"--input={_MR_INPUT}",
        f"--output={output}",
        *_analytics_args(params),
    ]
    if spec.name == "ridership_trends":
        args.append(f"--metro-daily={_MR_WAREHOUSE}/metro_daily")
    body = {
        "op": "yarn_jar",
        "jar": _MR_JAR,
        "main": _MR_MAIN,
        "args": args,
    }
    if track:
        body["track"] = track
    return body


def _mr_job(
    runner_url: str,
    spec: JobSpec,
    params: dict[str, Any],
    track: str,
) -> tuple[str, str]:
    body = mr_submit(spec, params, track)
    logs = [_post(runner_url, body)]
    for name in _ANALYTICS_DDL.get(spec.name, ()):
        logs.append(
            _post(
                runner_url,
                {
                    "op": "beeline",
                    "file": f"/opt/src/flowstate/warehouse/ddl/{name}",
                },
            )
        )
    if spec.name == "cross_modal":
        location = f"{_MR_WAREHOUSE}/analytics_ridehail_metro"
    else:
        location = str(body["args"][2]).removeprefix("--output=")
    return location, "\n".join(logs)


_STREAM_SINK = {
    "active_rides": "active_rides",
    "cancel_rate": "cancel_rate",
    "surge": "surge",
    "metro_activity": "metro_activity",
    "bus_activity": "bus_activity",
    "aviation_activity": "aviation_activity",
}
_STREAM_TOPICS = {
    "active_rides": ("flowstate.ridehail.activity.v1",),
    "cancel_rate": ("flowstate.ridehail.activity.v1",),
    "surge": ("flowstate.ridehail.activity.v1",),
    "metro_activity": ("flowstate.metro.station_hour.v1",),
    "bus_activity": ("flowstate.bus.scheduled_stop.v1",),
    "aviation_activity": (
        "flowstate.aviation.arrival.v1",
        "flowstate.aviation.departure.v1",
    ),
}
_KAFKA_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"


def _stream_job(
    runner_url: str,
    spec: JobSpec,
    params: dict[str, Any],
    track: str,
) -> tuple[str, str]:
    name = _STREAM_SINK[spec.name]
    sink = f"/stream-sink/{name}"
    checkpoint = f"/checkpoints/{name}"
    if params.get("reset"):
        _post(runner_url, {"op": "hdfs_rm", "dest": sink})
        _post(runner_url, {"op": "hdfs_rm", "dest": checkpoint})
        for topic in _STREAM_TOPICS[spec.name]:
            _reset_topic(topic)
    topics = ",".join(_STREAM_TOPICS[spec.name])
    script = "/opt/src/" + spec.entrypoint.replace(".", "/") + ".py"
    args = [
        f"--trigger={params.get('trigger') or 'replay'}",
        f"--max-offsets={int(params.get('max_offsets') or 20000)}",
        "--kafka=kafka:9092",
        f"--topics={topics}",
        f"--sink=hdfs://namenode:9000{sink}",
        f"--checkpoint=hdfs://namenode:9000{checkpoint}",
    ]
    conf = ["spark.sql.shuffle.partitions=4"]
    if spec.name == "surge":
        baseline = params.get("baseline_run_id")
        if not baseline:
            raise RuntimeError("surge needs baseline_run_id")
        args.append(f"--baseline-run-id={baseline}")
        conf.extend(["spark.cores.max=1", "spark.executor.cores=1"])
    log = _post(
        runner_url,
        {
            "op": "spark_submit",
            "script": script,
            "args": args,
            "conf": conf,
            "packages": _KAFKA_PACKAGES,
            "track": track,
        },
    )
    return f"hdfs://namenode:9000{sink}", log


def _reset_topic(name: str) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    from flowstate.config import get_settings
    from flowstate.streaming.topics import ipv4_bootstrap

    admin = KafkaAdminClient(
        bootstrap_servers=ipv4_bootstrap(
            get_settings().kafka_bootstrap_servers
        )
    )
    try:
        admin.delete_topics([name])
    except Exception:
        pass
    try:
        admin.create_topics(
            [NewTopic(name=name, num_partitions=1, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def _analytics_args(params: dict[str, Any]) -> list[str]:
    run_id = str(params.get("run_id") or "")
    if not run_id:
        raise RuntimeError("analytics job needs run_id")
    return [
        f"--run-id={run_id}",
        f"--corpus-rows={int(params['corpus_rows'])}",
        f"--seed={int(params['seed'])}",
        f"--bundle-hash={params['bundle_hash']}",
        f"--bundle-version={params['bundle_version']}",
    ]


def _amplify(runner_url: str, params: dict[str, Any]) -> tuple[str, str]:
    from flowstate.config import get_settings
    from flowstate.ingest.amplify import AmplifyRequest
    from flowstate.normalize.cities import DEFAULT_CITY_ID
    from flowstate.normalize.places.review import require_station_reviews
    from flowstate.resample import run_amplify

    request = AmplifyRequest.model_validate(
        {
            key: value
            for key, value in params.items()
            if key not in ("run_id", "engine")
        }
    )
    settings = get_settings()
    require_station_reviews(settings.data_dir, DEFAULT_CITY_ID)
    run_amplify(settings, request.rows, request.seed)
    from flowstate.lake.amplify import (
        HDFS_AMPLIFIED_DIR,
        META_FILES,
        amplified_hdfs_uri,
    )
    from flowstate.lake.ridehail import _copy_in

    root = settings.staged_dir / "rides_amplified"
    parts = sorted(root.glob("part-*.parquet"))
    copy_root = settings.lake_dir / "rides_amplified"
    for path in parts:
        _copy_in(path, copy_root / path.name)
    for name in META_FILES:
        _copy_in(root / name, copy_root / "_meta" / name)
    logs = [
        _post(runner_url, {"op": "hdfs_rm", "dest": HDFS_AMPLIFIED_DIR})
    ]
    logs.append(
        _post(
            runner_url,
            {
                "op": "hdfs_mkdir",
                "dest": f"{HDFS_AMPLIFIED_DIR}/_meta",
            },
        )
    )
    for path in parts:
        logs.append(
            _post(
                runner_url,
                {
                    "op": "hdfs_put",
                    "src": f"/opt/data/lake/rides_amplified/{path.name}",
                    "dest": f"{HDFS_AMPLIFIED_DIR}/{path.name}",
                },
            )
        )
    for name in META_FILES:
        logs.append(
            _post(
                runner_url,
                {
                    "op": "hdfs_put",
                    "src": f"/opt/data/lake/rides_amplified/_meta/{name}",
                    "dest": f"{HDFS_AMPLIFIED_DIR}/_meta/{name}",
                },
            )
        )
    logs.append(
        _post(
            runner_url,
            {
                "op": "spark_submit",
                "script": (
                    "/opt/src/flowstate/batch/spark_jobs/"
                    "conservation_check.py"
                ),
            },
        )
    )
    logs.append(
        _post(
            runner_url,
            {
                "op": "beeline",
                "file": (
                    "/opt/src/flowstate/warehouse/ddl/"
                    "rollup_conservation.sql"
                ),
            },
        )
    )
    location = amplified_hdfs_uri(settings, "")
    return location.rstrip("/"), "\n".join(logs)


def _post(runner_url: str, body: dict[str, Any]) -> str:
    return str(_post_payload(runner_url, body).get("log") or "")


def _post_payload(
    runner_url: str, body: dict[str, Any]
) -> dict[str, Any]:
    raw = json.dumps(body).encode()
    request = urllib.request.Request(
        runner_url + "/ops",
        data=raw,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=3600) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(detail[-4000:]) from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"runner is unreachable: {exc.reason}") from None
    if not payload.get("ok"):
        raise RuntimeError(str(payload.get("log") or "runner failed"))
    return payload

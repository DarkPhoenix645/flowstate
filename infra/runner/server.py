"""Runner shim. Four command shapes. No free shell string.

Listen on 0.0.0.0 inside the container. Compose publishes
127.0.0.1 only.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOST = "0.0.0.0"
PORT = 8091
HDFS = "/opt/hadoop-3.2.1/bin/hdfs"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
BEELINE = "/opt/spark/bin/beeline"
YARN = "/opt/hadoop-3.2.1/bin/yarn"
SRC = Path("/opt/src/flowstate")
LAKE = Path("/opt/data/lake")
DDL = SRC / "warehouse" / "ddl"
JARS = Path("/opt/src/flowstate/batch/mapreduce/target")
RM_ALLOWED = {
    "/lake/rides_amplified",
    "/stream-sink/active_rides",
    "/stream-sink/cancel_rate",
    "/stream-sink/surge",
    "/checkpoints/active_rides",
    "/checkpoints/cancel_rate",
    "/checkpoints/surge",
    "/stream-sink/metro_activity",
    "/stream-sink/bus_activity",
    "/stream-sink/aviation_activity",
    "/checkpoints/metro_activity",
    "/checkpoints/bus_activity",
    "/checkpoints/aviation_activity",
}
HIVE_URL = "jdbc:hive2://hiveserver2:10000"
_APP_ID = re.compile(r"application_\d+_\d+")
_TRACK = re.compile(r"[A-Za-z0-9_-]{1,80}")
_LOCK = threading.Lock()
_YARN: dict[str, dict[str, object]] = {}
_SPARK: dict[str, dict[str, object]] = {}


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.serve_forever()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        try:
            payload = _table_get(self.path)
        except ValueError as exc:
            self._send(422, {"ok": False, "error": str(exc)})
            return
        except Exception as exc:
            self._send(500, {"ok": False, "error": str(exc)})
            return
        self._send(200, payload)

    def do_POST(self) -> None:
        if self.path.rstrip("/") != "/ops":
            self._send(404, {"ok": False, "log": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError("body must be an object")
            op = body.get("op")
            if op == "yarn_jar":
                code, payload = _follow(body, _YARN)
                self._send(code, payload)
                return
            if op == "yarn_kill":
                self._send(200, _yarn_kill(body))
                return
            if op == "spark_submit" and body.get("track"):
                code, payload = _follow(body, _SPARK)
                self._send(code, payload)
                return
            if op == "spark_kill":
                self._send(200, _spark_kill(body))
                return
            argv = _argv(body)
        except (json.JSONDecodeError, ValueError) as exc:
            self._send(422, {"ok": False, "log": str(exc)})
            return
        env = os.environ.copy()
        env["HADOOP_CONF_DIR"] = "/etc/hadoop"
        env["PYTHONPATH"] = "/opt/src"
        completed = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        # Stdout last, so a one-line failure stays when stderr is long.
        log = (completed.stderr + completed.stdout)[-4000:]
        code = 200 if completed.returncode == 0 else 500
        self._send(code, {"ok": completed.returncode == 0, "log": log})

    def _send(self, status: int, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt: str, *args: object) -> None:
        return


def _runner_env() -> dict[str, str]:
    env = os.environ.copy()
    env["HADOOP_CONF_DIR"] = "/etc/hadoop"
    env["PYTHONPATH"] = "/opt/src"
    return env


def _follow(
    body: dict[str, object], table: dict[str, dict[str, object]]
) -> tuple[int, dict[str, object]]:
    argv = _argv(body)
    track = str(body.get("track") or "")
    if track and _TRACK.fullmatch(track) is None:
        raise ValueError("track is not a name")
    proc = subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_runner_env(),
    )
    if track:
        with _LOCK:
            table[track] = {"proc": proc, "app_id": None}
    app_id = None
    chunks: list[str] = []
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            chunks.append(line)
            found = _APP_ID.search(line)
            if found is None:
                continue
            app_id = found.group(0)
            if not track:
                continue
            with _LOCK:
                row = table.get(track)
                if row is not None:
                    row["app_id"] = app_id
        code = proc.wait()
    finally:
        if track:
            with _LOCK:
                table.pop(track, None)
    log = "".join(chunks)[-3800:]
    if app_id:
        log = f"{log}\n{app_id}"
    return (200 if code == 0 else 500), {
        "ok": code == 0,
        "log": log,
        "application_id": app_id,
    }


def _yarn_kill(body: dict[str, object]) -> dict[str, object]:
    track = str(body.get("track") or "")
    application = str(body.get("application") or "")
    if track and _TRACK.fullmatch(track) is None:
        raise ValueError("track is not a name")
    if application and _APP_ID.fullmatch(application) is None:
        raise ValueError("application id is not a yarn id")
    proc = None
    with _LOCK:
        row = _YARN.get(track)
        if row is not None and not application:
            application = str(row.get("app_id") or "")
        if row is not None:
            proc = row.get("proc")
    if application:
        completed = subprocess.run(
            [YARN, "application", "-kill", application],
            capture_output=True,
            text=True,
            env=_runner_env(),
            check=False,
        )
        log = (completed.stdout + completed.stderr)[-4000:]
    elif isinstance(proc, subprocess.Popen):
        proc.terminate()
        log = "terminated the yarn client"
    else:
        log = "no yarn application for this run"
    return {"ok": True, "log": log, "application_id": application or None}


def _spark_kill(body: dict[str, object]) -> dict[str, object]:
    track = str(body.get("track") or "")
    if track and _TRACK.fullmatch(track) is None:
        raise ValueError("track is not a name")
    proc = None
    with _LOCK:
        row = _SPARK.get(track)
        if row is not None:
            proc = row.get("proc")
    if isinstance(proc, subprocess.Popen):
        proc.terminate()
        log = "terminated the spark client"
    else:
        log = "no spark application for this run"
    return {"ok": True, "log": log, "application_id": None}


def _table_get(path: str) -> list[dict[str, object]]:
    """Allowlisted Hive read. Grafana cannot send SQL."""
    import csv
    from urllib.parse import parse_qs, urlparse

    from flowstate.warehouse.grafana_tables import (
        chart_rows,
        stream_sql,
        warehouse_sql,
    )

    parsed = urlparse(path)
    parts = [item for item in parsed.path.split("/") if item]
    query = parse_qs(parsed.query)
    if len(parts) == 3 and parts[0] == "warehouse" and parts[2] == "runs":
        return _run_ids(parts[1])
    if len(parts) != 2 or parts[0] not in ("warehouse", "stream"):
        raise ValueError("path is not a table")
    kind, table = parts
    if kind == "warehouse":
        run_id = (query.get("run_id") or [None])[0]
        sql = warehouse_sql(table, run_id)
    else:
        ward = (query.get("ward") or ["kia"])[0]
        sql = stream_sql(table, ward)
    completed = subprocess.run(
        [
            BEELINE,
            "-u",
            HIVE_URL,
            "--silent=true",
            "--outputformat=csv2",
            "--showHeader=true",
            "-e",
            sql,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout)[-500:]
        raise RuntimeError(detail)
    prompt = re.compile(r"^\d+: jdbc:hive2://[^>]+> ")
    lines = []
    for line in completed.stdout.splitlines():
        line = prompt.sub("", line).replace("\x00", "").strip()
        if not line or line.startswith(("SLF4J", "INFO", "WARNING")):
            continue
        lines.append(line)
    rows = []
    for row in csv.DictReader(lines):
        item = {}
        for key, value in row.items():
            if not key:
                continue
            item[key.split(".")[-1]] = value
        rows.append(item)
    view = (query.get("view") or [None])[0]
    if view:
        return chart_rows(table, view, rows)
    return rows


def _run_ids(table: str) -> list[dict[str, object]]:
    from flowstate.warehouse.grafana_tables import WAREHOUSE

    if table not in WAREHOUSE or not table.startswith("analytics_"):
        raise ValueError("table has no run_id")
    location = f"/user/hive/warehouse/{table}"
    completed = subprocess.run(
        [HDFS, "dfs", "-ls", location],
        capture_output=True,
        text=True,
        check=False,
    )
    found: list[tuple[str, str]] = []
    for line in completed.stdout.splitlines():
        bits = line.split()
        if len(bits) < 8 or "run_id=" not in bits[-1]:
            continue
        stamp = f"{bits[-3]} {bits[-2]}"
        run_id = bits[-1].rstrip("/").rsplit("run_id=", 1)[-1]
        found.append((stamp, run_id))
    found.sort(reverse=True)
    return [{"run_id": run_id} for _, run_id in found]


def _argv(body: object) -> list[str]:
    if not isinstance(body, dict):
        raise ValueError("body must be an object")
    op = body.get("op")
    if op == "spark_submit":
        script = _under(str(body.get("script")), SRC, ".py")
        extra = [_checked_arg(item) for item in body.get("args") or []]
        argv = [SPARK_SUBMIT, "--master", "spark://spark:7077"]
        for item in body.get("conf") or []:
            argv.extend(["--conf", _checked_arg(item)])
        packages = str(body.get("packages") or "")
        if packages:
            argv.extend(["--packages", _checked_arg(packages)])
        argv.append(str(script))
        argv.extend(extra)
        return argv
    if op == "hdfs_put":
        src = _under(str(body.get("src")), LAKE, "")
        dest = _hdfs_dest(str(body.get("dest")))
        return [HDFS, "dfs", "-put", "-f", str(src), dest]
    if op == "hdfs_mkdir":
        dest = _hdfs_dest(str(body.get("dest")))
        return [HDFS, "dfs", "-mkdir", "-p", dest]
    if op == "hdfs_rm":
        dest = str(body.get("dest"))
        if dest not in RM_ALLOWED:
            raise ValueError("dest is not removable")
        return [HDFS, "dfs", "-rm", "-r", "-f", dest]
    if op == "beeline":
        sql = _under(str(body.get("file")), DDL, ".sql")
        return [BEELINE, "-u", HIVE_URL, "-f", str(sql)]
    if op == "yarn_jar":
        jar = _under(str(body.get("jar")), JARS, ".jar")
        main = str(body.get("main") or "")
        if not main.replace(".", "").replace("_", "").isalnum():
            raise ValueError("main class is not a name")
        extra = [_checked_arg(item) for item in body.get("args") or []]
        return [YARN, "jar", str(jar), main, *extra]
    raise ValueError("op is not allowed")


def _under(raw: str, root: Path, suffix: str) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        raise ValueError("path must be absolute")
    resolved = path.resolve()
    if root != resolved and root not in resolved.parents:
        raise ValueError("path is outside the allowed root")
    if suffix and resolved.suffix != suffix:
        raise ValueError(f"path must end with {suffix}")
    return resolved


def _checked_arg(item: object) -> str:
    if not isinstance(item, str):
        raise ValueError("arg must be a string")
    if item.startswith("--"):
        name, sep, value = item.partition("=")
        if not sep or not name.strip("-") or not value:
            raise ValueError("arg is not allowed")
        if not set(value) <= _ARG_CHARS:
            raise ValueError("arg is not allowed")
        return item
    if not set(item) <= _ARG_CHARS:
        raise ValueError("arg is not allowed")
    return item


_ARG_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:/=-,"
)


def _hdfs_dest(dest: str) -> str:
    if ".." in dest or not dest.startswith(("/lake/", "/tmp/")):
        raise ValueError("dest is not an allowed HDFS path")
    allowed = set(
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789_./-"
    )
    if not set(dest) <= allowed:
        raise ValueError("dest has a character that is not allowed")
    return dest


if __name__ == "__main__":
    main()

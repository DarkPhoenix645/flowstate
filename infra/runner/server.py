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
RM_ALLOWED = {"/lake/rides_amplified"}
HIVE_URL = "jdbc:hive2://hiveserver2:10000"
_APP_ID = re.compile(r"application_\d+_\d+")
_TRACK = re.compile(r"[A-Za-z0-9_-]{1,80}")
_LOCK = threading.Lock()
_YARN: dict[str, dict[str, object]] = {}


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    server.serve_forever()


class Handler(BaseHTTPRequestHandler):
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
                code, payload = _yarn_jar(body)
                self._send(code, payload)
                return
            if op == "yarn_kill":
                self._send(200, _yarn_kill(body))
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
        log = (completed.stdout + completed.stderr)[-4000:]
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


def _yarn_jar(body: dict[str, object]) -> tuple[int, dict[str, object]]:
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
            _YARN[track] = {"proc": proc, "app_id": None}
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
                row = _YARN.get(track)
                if row is not None:
                    row["app_id"] = app_id
        code = proc.wait()
    finally:
        if track:
            with _LOCK:
                _YARN.pop(track, None)
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


def _argv(body: object) -> list[str]:
    if not isinstance(body, dict):
        raise ValueError("body must be an object")
    op = body.get("op")
    if op == "spark_submit":
        script = _under(str(body.get("script")), SRC, ".py")
        extra = [_checked_arg(item) for item in body.get("args") or []]
        return [
            SPARK_SUBMIT,
            "--master",
            "spark://spark:7077",
            str(script),
            *extra,
        ]
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
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:/-"
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

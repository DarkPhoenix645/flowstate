"""Runner shim. Four command shapes. No free shell string.

Listen on 0.0.0.0 inside the container. Compose publishes
127.0.0.1 only.
"""

from __future__ import annotations

import json
import os
import subprocess
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
JARS = Path("/opt/jars")
RM_ALLOWED = {"/lake/rides_amplified"}
HIVE_URL = "jdbc:hive2://hiveserver2:10000"


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


def _argv(body: object) -> list[str]:
    if not isinstance(body, dict):
        raise ValueError("body must be an object")
    op = body.get("op")
    if op == "spark_submit":
        script = _under(str(body.get("script")), SRC, ".py")
        return [
            SPARK_SUBMIT,
            "--master",
            "spark://spark:7077",
            str(script),
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
        return [YARN, "jar", str(jar), main]
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

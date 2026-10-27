"""The one HTTP process. Model config, the demo facade, and runs.

Bind is loopback by default. There is no auth in v1.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from flowstate.api.runs import run_dispatch
from flowstate.config import get_settings
from flowstate.config.demo import demo_dispatch
from flowstate.config.postgres import (
    KINDS,
    ConfigStore,
    ConfigVersion,
    resolve_active,
)
from flowstate.config.resolve import (
    ConfigNotActiveError,
    ConfigUnavailableError,
    ConfigValidationError,
)
from flowstate.log import configure_logging

log = logging.getLogger(__name__)
_PATH = re.compile(
    r"^/api/model-config/(?P<kind>[^/]+)"
    r"(?P<tail>/versions|/diff|/activate|/deactivate)?$"
)
_MAX_BODY = 1_000_000


def dispatch(
    store: ConfigStore,
    method: str,
    target: str,
    raw: bytes,
    settings: Any = None,
    backends: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    """One request. Returns a status code and a JSON object."""
    if settings is None:
        settings = get_settings()
    split = urlsplit(target)
    path = split.path.rstrip("/") or "/"
    try:
        handled = run_dispatch(
            store, method, path, split.query, raw, settings, backends
        )
        if handled is not None:
            return handled
        if not path.startswith("/api/model-config"):
            handled = demo_dispatch(
                store, method, path, split.query, raw, settings
            )
            if handled is not None:
                return handled
            return 404, {"error": "not found"}
        match = _PATH.fullmatch(path)
        if match is None:
            return 404, {"error": "not found"}
        kind = match.group("kind")
        tail = match.group("tail")
        if kind not in KINDS:
            return 422, {"error": f"unknown kind {kind}"}
        return _route(store, method, kind, tail, split.query, raw)
    except ConfigValidationError as exc:
        return 422, {"error": str(exc)}
    except ConfigNotActiveError as exc:
        return 404, {"error": str(exc)}
    except ConfigUnavailableError as exc:
        return 503, {"error": str(exc)}
    except json.JSONDecodeError:
        return 400, {"error": "body is not JSON"}


def serve(store: ConfigStore, host: str, port: int) -> ThreadingHTTPServer:
    handler = _handler(store)

    class Server(ThreadingHTTPServer):
        allow_reuse_address = True

    return Server((host, port), handler)


def main() -> None:
    configure_logging()
    settings = get_settings()
    store = ConfigStore(settings.config_database_url)
    store.active()
    from flowstate.jobs.backend import process_backends
    from flowstate.jobs.service import reconcile
    from flowstate.jobs.store import RunStore

    runs = RunStore(store.database_url)
    backends = process_backends(settings.runner_url)
    reconcile(runs, backends)

    def _poll() -> None:
        while True:
            reconcile(runs, backends)
            time.sleep(1)

    threading.Thread(target=_poll, daemon=True).start()
    server = serve(store, settings.api_host, settings.api_port)
    host, port = server.server_address[:2]
    log.info("demo API on http://%s:%s", host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _route(
    store: ConfigStore,
    method: str,
    kind: str,
    tail: str | None,
    query: str,
    raw: bytes,
) -> tuple[int, dict[str, Any]]:
    if method == "GET" and tail is None:
        return 200, _active(store, kind)
    if method == "GET" and tail == "/versions":
        versions = [
            _version_json(item, content=True)
            for item in store.list_versions(kind)
        ]
        return 200, {"kind": kind, "versions": versions}
    if method == "GET" and tail == "/diff":
        params = parse_qs(query)
        try:
            from_id = params["from"][0]
            to_id = params["to"][0]
        except KeyError:
            raise ConfigValidationError("diff needs from and to") from None
        return 200, store.diff(kind, from_id, to_id)
    if method == "PUT" and tail is None:
        content, note = _document(raw)
        saved = store.put_version(kind, content, note=note)
        return 200, _version_json(saved, content=True)
    if method == "POST" and tail == "/activate":
        body = _object(raw)
        version_id = body.get("version_id")
        if not version_id:
            raise ConfigValidationError("activate needs version_id")
        saved = store.activate(kind, str(version_id))
        return 200, _version_json(saved, content=False)
    if method == "POST" and tail == "/deactivate":
        store.deactivate(kind)
        return 200, {"kind": kind, "active": False}
    return 405, {"error": "method not allowed"}


def _active(store: ConfigStore, kind: str) -> dict[str, Any]:
    active = store.active()
    version = active.get(kind)
    if version is None:
        raise ConfigNotActiveError(
            f"no active {kind}. Run make config-seed."
        )
    payload = _version_json(version, content=True)
    try:
        payload["config_hash"] = resolve_active(store).config_hash
    except ConfigNotActiveError:
        payload["config_hash"] = None
    return payload


def _document(raw: bytes) -> tuple[Any, str | None]:
    body = _object(raw)
    if "content" in body:
        return body["content"], body.get("note")
    return body, None


def _object(raw: bytes) -> dict[str, Any]:
    if not raw:
        raise ConfigValidationError("body is required")
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise ConfigValidationError("body must be a JSON object")
    return body


def _version_json(version: ConfigVersion, *, content: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "version_id": version.id,
        "kind": version.kind,
        "sha256": version.sha256,
        "created_at": version.created_at,
        "note": version.note,
        "source": "db",
    }
    if content:
        payload["content"] = version.content
    return payload


def _handler(store: ConfigStore):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._serve()

        def do_PUT(self) -> None:
            self._serve()

        def do_POST(self) -> None:
            self._serve()

        def _serve(self) -> None:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length > _MAX_BODY:
                self._send(413, {"error": "body is too large"})
                return
            raw = self.rfile.read(length) if length else b""
            status, payload = dispatch(store, self.command, self.path, raw)
            self._send(status, payload)

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, fmt: str, *args: Any) -> None:
            log.info("%s", fmt % args)

    return Handler


if __name__ == "__main__":
    main()

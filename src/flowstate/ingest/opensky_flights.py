"""Fetch VOBL arrivals and departures for the Bengaluru alignment window.

Window: 2024-10-26 00:00 IST through 2026-04-15 23:59:59 IST.
Each query spans at most 2 UTC calendar days. Cost is 30 flight-credits
per query. One direction is 269 windows, 8,070 credits. Both directions
from an empty cache are 16,140 credits.

Credentials come from Settings.opensky_clients(). Up to 10 pairs.
Pair 1 is unsuffixed. Pairs 2-10 use _2 .. _10.
Unprefixed OPENSKY_CLIENT_ID is ignored.

One worker per client. A window file that already exists and is non-empty
is not fetched again. Arrival files left in the cache root by the serial
run are moved into windows/arrival/ before any request. When every client
is under 30 credits or returns 429, the fetch stops. It does not wait for
the daily refill. Exit code is 2. An incomplete departure extract does not
mark a complete arrival extract invalid.

Arrival event time is lastSeen, stored as arrival_time_proxy_utc.
Departure event time is firstSeen on the departure endpoint, stored as
departure_time_proxy_utc. estArrivalAirport may be null. The row is kept.
Dedupe key is (icao24, firstSeen) per direction.

Per-window status is 200, 404 (no flights), failed, or quota. Those are
distinct. A sparse departure window (low count, not empty) is still 200
and is flagged; it is not written as zero flights. Canonical CSVs keep
the UTC proxy fields and add IST date/hour bins. T33 reads those CSVs.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import requests
from dotenv import load_dotenv

log = logging.getLogger("opensky")

ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
ENDPOINTS = {
    "arrival": "https://opensky-network.org/api/flights/arrival",
    "departure": "https://opensky-network.org/api/flights/departure",
}
AIRPORT = "VOBL"
CREDITS_PER_WINDOW = 30
MAX_ATTEMPTS = 5
DIRECTIONS = ("arrival", "departure")
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UTC = dt.UTC
START = dt.datetime(2024, 10, 26, 0, 0, tzinfo=IST).astimezone(UTC)
END = dt.datetime(2026, 4, 15, 23, 59, 59, tzinfo=IST).astimezone(UTC)
OPENSKY_DIR = ROOT / "flowstate-datasets" / "opensky"
CACHE = OPENSKY_DIR / "windows"
LEGACY_CACHE = ROOT / ".scratch" / "dl" / "opensky"
# 2-UTC-day departure windows at mid/recent dates are ~330-375 events
# (docs/data-sources.md, checked 2026-10-11). Below this floor is sparse,
# not a zero-flight day.
DEPARTURE_SPARSE_BELOW = 150
OK_WINDOW_STATUSES = frozenset({"200", "404", "cached"})

Window = tuple[str, dt.datetime, dt.datetime]


@dataclass
class WindowFetch:
    """One OpenSky window response. Tests may return a bare list instead."""

    flights: list[dict]
    status: str = "200"
    http_status: int | None = 200
    retry_count: int = 0


class OpenSkyCreditsExhausted(RuntimeError):
    """Every configured client is below one window of flight credits."""


class OpenSkyFetchBusy(RuntimeError):
    """Another process already holds the OpenSky fetch lock."""


@contextmanager
def fetch_lock():
    """One fetch process. A second one exits before any HTTP."""
    path = _lock_path()
    try:
        _ensure_lock_byte(path)
        handle = path.open("r+b")
    except PermissionError as exc:
        raise _busy() from exc
    try:
        _try_lock(handle)
    except OpenSkyFetchBusy:
        handle.close()
        raise
    try:
        yield
    finally:
        _unlock(handle)
        handle.close()


def _lock_path() -> Path:
    OPENSKY_DIR.mkdir(parents=True, exist_ok=True)
    return OPENSKY_DIR / ".fetch.lock"


def _ensure_lock_byte(path: Path) -> None:
    """Windows cannot read a byte another process has locked."""
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("ab") as prep:
        if prep.seek(0, os.SEEK_END) == 0:
            prep.write(b"\0")
            prep.flush()


def _busy() -> OpenSkyFetchBusy:
    return OpenSkyFetchBusy(
        "OpenSky fetch already running. Refusing to start a second one."
    )


def _try_lock(handle) -> None:
    if sys.platform == "win32":
        import msvcrt

        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise _busy() from exc
        return
    import fcntl

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise _busy() from exc


def _unlock(handle) -> None:
    if sys.platform == "win32":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass
class Client:
    index: int
    client_id: str
    secret: str
    remaining: int | None = None
    exhausted: bool = False
    retry_after_s: int | None = None
    token: str | None = field(default=None, repr=False)
    expires_at: dt.datetime | None = None

    def label(self) -> str:
        return f"pair {self.index}"


def load_clients(settings: object) -> list[Client]:
    from flowstate.config import Settings

    if not isinstance(settings, Settings):
        raise TypeError(settings)
    raw = settings.opensky_clients()
    if not raw:
        raise RuntimeError(
            "No OpenSky credentials. Set FLOWSTATE_OPENSKY_CLIENT_ID and "
            "FLOWSTATE_OPENSKY_CLIENT_SECRET. Pairs 2-10 use the "
            "_2 through _10 suffix."
        )
    clients = [
        Client(index=index, client_id=client_id, secret=secret)
        for index, client_id, secret in raw
    ]
    log.info("OpenSky pairs configured: %s", len(clients))
    return clients


def windows() -> list[tuple[dt.datetime, dt.datetime]]:
    out: list[tuple[dt.datetime, dt.datetime]] = []
    cur = START
    while cur <= END:
        end = min(_two_partitions_end(cur), END)
        out.append((cur, end))
        cur = end + dt.timedelta(seconds=1)
    return out


def _two_partitions_end(cur: dt.datetime) -> dt.datetime:
    midnight = (cur + dt.timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return midnight + dt.timedelta(days=1) - dt.timedelta(seconds=1)


def cache_path(
    direction: str, start: dt.datetime, end: dt.datetime
) -> Path:
    name = f"{AIRPORT}_{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}Z.json"
    return CACHE / direction / name


def _flat_arrival_path(start: dt.datetime, end: dt.datetime) -> Path:
    return CACHE / cache_path("arrival", start, end).name


def _nonempty(path: Path) -> bool:
    return path.is_file() and path.stat().st_size > 0


def window_file(
    direction: str, start: dt.datetime, end: dt.datetime
) -> Path | None:
    nested = cache_path(direction, start, end)
    if _nonempty(nested):
        return nested
    if direction == "arrival":
        flat = _flat_arrival_path(start, end)
        if _nonempty(flat):
            return flat
    return None


def adopt_flat_arrival_cache() -> int:
    """Move serial-run arrival files into windows/arrival/."""
    if not CACHE.is_dir():
        return 0
    moved = 0
    for path in list(CACHE.glob("*.json")):
        if not _nonempty(path):
            continue
        dest = CACHE / "arrival" / path.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        if _nonempty(dest):
            continue
        os.replace(path, dest)
        moved += 1
    if moved:
        log.info("moved %s flat arrival files into windows/arrival", moved)
    return moved


def _link_legacy(start: dt.datetime, end: dt.datetime, dest: Path) -> bool:
    legacy = LEGACY_CACHE / dest.name
    if not _nonempty(legacy):
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        try:
            dest.hardlink_to(legacy.resolve())
        except OSError:
            dest.write_bytes(legacy.read_bytes())
    return _nonempty(dest)


def _token(client: Client) -> str:
    if (
        client.token
        and client.expires_at
        and dt.datetime.now(UTC) < client.expires_at
    ):
        return client.token
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client.client_id,
            "client_secret": client.secret,
        },
        timeout=30,
    )
    if response.status_code != 200:
        client.exhausted = True
        client.remaining = 0
        raise OpenSkyCreditsExhausted(
            f"OpenSky login failed for {client.label()}: "
            f"HTTP {response.status_code}"
        )
    data = response.json()
    client.token = data["access_token"]
    client.expires_at = dt.datetime.now(UTC) + dt.timedelta(
        seconds=int(data.get("expires_in", 1800)) - 30
    )
    return client.token


def _mark_exhausted(client: Client, response: requests.Response) -> None:
    client.exhausted = True
    raw = response.headers.get("X-Rate-Limit-Remaining")
    if raw is not None and raw.isdigit():
        client.remaining = int(raw)
    else:
        client.remaining = 0
    retry = response.headers.get("X-Rate-Limit-Retry-After-Seconds")
    if retry and retry.isdigit():
        client.retry_after_s = int(retry)
    log.error(
        "OpenSky credits expended for %s. remaining=%s retry_after_s=%s",
        client.label(),
        client.remaining,
        client.retry_after_s,
    )


def fetch_window(
    client: Client,
    direction: str,
    start: dt.datetime,
    end: dt.datetime,
) -> WindowFetch:
    response = requests.get(
        ENDPOINTS[direction],
        params={
            "airport": AIRPORT,
            "begin": int(start.timestamp()),
            "end": int(end.timestamp()),
        },
        headers={"Authorization": f"Bearer {_token(client)}"},
        timeout=180,
    )
    remaining = response.headers.get("X-Rate-Limit-Remaining")
    if remaining is not None and remaining.isdigit():
        client.remaining = int(remaining)
    if response.status_code == 429:
        _mark_exhausted(client, response)
        raise OpenSkyCreditsExhausted(client.label())
    if response.status_code == 404:
        log.info(
            "%s %s %s → %s: no flights. credits left %s",
            client.label(),
            direction,
            start.strftime("%Y-%m-%d %H:%M"),
            end.strftime("%Y-%m-%d %H:%M"),
            client.remaining,
        )
        return WindowFetch(flights=[], status="404", http_status=404)
    if response.status_code != 200:
        raise RuntimeError(
            f"OpenSky HTTP {response.status_code} for {client.label()} "
            f"{direction} {start.isoformat()} → {end.isoformat()}"
        )
    if client.remaining is not None and client.remaining < CREDITS_PER_WINDOW:
        client.exhausted = True
        log.error(
            "OpenSky credits expended for %s after this window. "
            "remaining=%s, which is below %s.",
            client.label(),
            client.remaining,
            CREDITS_PER_WINDOW,
        )
    flights = response.json()
    log.info(
        "%s %s %s → %s: %s flights. credits left %s",
        client.label(),
        direction,
        start.strftime("%Y-%m-%d %H:%M"),
        end.strftime("%Y-%m-%d %H:%M"),
        len(flights),
        client.remaining,
    )
    return WindowFetch(flights=flights, status="200", http_status=200)


def _write_window(path: Path, flights: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(".json.partial")
    partial.write_text(json.dumps(flights))
    os.replace(partial, path)


def window_status_path(
    direction: str, start: dt.datetime, end: dt.datetime
) -> Path:
    data = cache_path(direction, start, end)
    return data.with_name(data.name.replace(".json", ".status.json"))


def coverage_manifest_path(direction: str) -> Path:
    return OPENSKY_DIR / f"coverage_{direction}.json"


def quality_path(direction: str) -> Path:
    return OPENSKY_DIR / f"quality_{direction}.json"


def coerce_fetch(raw: WindowFetch | list[dict]) -> WindowFetch:
    if isinstance(raw, WindowFetch):
        return raw
    if not raw:
        return WindowFetch(flights=[], status="404", http_status=404)
    return WindowFetch(flights=raw, status="200", http_status=200)


def ist_bins(epoch: int | None) -> tuple[str | None, int | None]:
    """Calendar date and hour in IST from a Unix UTC epoch."""
    if epoch is None:
        return None, None
    instant = dt.datetime.fromtimestamp(int(epoch), UTC).astimezone(IST)
    return instant.date().isoformat(), instant.hour


def _is_sparse_departure(status: str, event_count: int) -> bool:
    return status == "200" and 0 < event_count < DEPARTURE_SPARSE_BELOW


def write_window_status(
    direction: str,
    start: dt.datetime,
    end: dt.datetime,
    *,
    status: str,
    event_count: int,
    http_status: int | None = None,
    retry_count: int = 0,
    fetched_at: str | None = None,
) -> dict:
    sparse = direction == "departure" and _is_sparse_departure(
        status, event_count
    )
    record = {
        "direction": direction,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "status": status,
        "http_status": http_status,
        "event_count": event_count,
        "retry_count": retry_count,
        "fetched_at": fetched_at or dt.datetime.now(UTC).isoformat(),
        "sparse": sparse,
    }
    path = window_status_path(direction, start, end)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record, indent=2))
    return record


def read_window_status(
    direction: str, start: dt.datetime, end: dt.datetime
) -> dict:
    path = window_status_path(direction, start, end)
    if path.is_file() and path.stat().st_size > 0:
        return json.loads(path.read_text())
    data = window_file(direction, start, end)
    if data is None:
        return {
            "direction": direction,
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
            "status": "unknown",
            "http_status": None,
            "event_count": 0,
            "retry_count": 0,
            "fetched_at": None,
            "sparse": False,
        }
    flights = json.loads(data.read_text())
    count = len(flights)
    status = "404" if count == 0 else "cached"
    return {
        "direction": direction,
        "start_utc": start.isoformat(),
        "end_utc": end.isoformat(),
        "status": status,
        "http_status": None,
        "event_count": count,
        "retry_count": 0,
        "fetched_at": None,
        "sparse": direction == "departure"
        and _is_sparse_departure("200", count),
    }


def stamp_unfetched(status: str) -> None:
    """Mark windows with no payload and no failed sidecar."""
    for direction in DIRECTIONS:
        for start, end in windows():
            if window_file(direction, start, end) is not None:
                continue
            existing = window_status_path(direction, start, end)
            if existing.is_file() and existing.stat().st_size > 0:
                saved = json.loads(existing.read_text())
                if saved.get("status") == "failed":
                    continue
            write_window_status(
                direction,
                start,
                end,
                status=status,
                event_count=0,
                http_status=None,
            )


def coverage_report(direction: str = "arrival") -> dict:
    needed = windows()
    cached = 0
    gaps: list[dict] = []
    sparse_windows: list[dict] = []
    status_counts: dict[str, int] = {}
    for start, end in needed:
        if window_file(direction, start, end) is not None:
            cached += 1
        elif direction == "arrival" and _link_legacy(
            start, end, cache_path(direction, start, end)
        ):
            cached += 1
        record = read_window_status(direction, start, end)
        kind = str(record.get("status") or "unknown")
        status_counts[kind] = status_counts.get(kind, 0) + 1
        if kind not in OK_WINDOW_STATUSES:
            gaps.append(
                {
                    "start_utc": record["start_utc"],
                    "end_utc": record["end_utc"],
                    "status": kind,
                    "event_count": record.get("event_count", 0),
                }
            )
        if direction == "departure" and record.get("sparse"):
            sparse_windows.append(
                {
                    "start_utc": record["start_utc"],
                    "end_utc": record["end_utc"],
                    "status": kind,
                    "event_count": record.get("event_count", 0),
                }
            )
    return {
        "direction": direction,
        "windows_required": len(needed),
        "windows_cached": cached,
        "complete": len(gaps) == 0,
        "credits_per_window": CREDITS_PER_WINDOW,
        "credits_if_uncached": (len(needed) - cached) * CREDITS_PER_WINDOW,
        "window_start_utc": START.isoformat(),
        "window_end_utc": END.isoformat(),
        "status_counts": status_counts,
        "gaps": gaps,
        "sparse_windows": sparse_windows,
        "sparse_below": (
            DEPARTURE_SPARSE_BELOW if direction == "departure" else None
        ),
    }


def coverage(direction: str = "arrival") -> dict:
    report = coverage_report(direction)
    return {
        "direction": report["direction"],
        "windows_required": report["windows_required"],
        "windows_cached": report["windows_cached"],
        "complete": report["complete"],
        "credits_per_window": report["credits_per_window"],
        "credits_if_uncached": report["credits_if_uncached"],
        "window_start_utc": report["window_start_utc"],
        "window_end_utc": report["window_end_utc"],
        "status_counts": report["status_counts"],
        "gaps": report["gaps"],
        "sparse_windows": report["sparse_windows"],
    }


def invalid_reason(direction: str) -> str | None:
    report = coverage_report(direction)
    if report["complete"]:
        return None
    gaps = report["gaps"]
    shown = gaps[:8]
    bits = [f"{item['status']} {item['start_utc'][:10]}" for item in shown]
    extra = f" (+{len(gaps) - 8} more)" if len(gaps) > 8 else ""
    return (
        f"OpenSky {direction} window incomplete: "
        f"{report['windows_cached']}/{report['windows_required']} "
        f"windows on disk. Gaps: {', '.join(bits)}{extra}"
    )


def _status_path(direction: str) -> Path:
    return OPENSKY_DIR / f"status_{direction}.json"


def _write_status(direction: str, status: dict) -> None:
    path = _status_path(direction)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2))
    log.info("wrote %s", path.name)


def csv_path(direction: str) -> Path:
    kind = "arrivals" if direction == "arrival" else "departures"
    return OPENSKY_DIR / f"VOBL_{kind}_2024-10-26_2026-04-15.csv"


def write_csv(direction: str) -> dict:
    import csv

    merged: dict[tuple[str, int], dict] = {}
    duplicates = 0
    nulls = {
        "icao24": 0,
        "firstSeen": 0,
        "estArrivalAirport": 0,
        "estDepartureAirport": 0,
    }
    for start, end in windows():
        path = window_file(direction, start, end)
        if path is None:
            continue
        for flight in json.loads(path.read_text()):
            if not flight.get("icao24"):
                nulls["icao24"] += 1
            if flight.get("firstSeen") in (None, ""):
                nulls["firstSeen"] += 1
            if not flight.get("estArrivalAirport"):
                nulls["estArrivalAirport"] += 1
            if not flight.get("estDepartureAirport"):
                nulls["estDepartureAirport"] += 1
            key = (
                flight.get("icao24") or "",
                int(flight.get("firstSeen") or 0),
            )
            if key in merged:
                duplicates += 1
            merged[key] = flight
    proxy = "lastSeen" if direction == "arrival" else "firstSeen"
    if direction == "arrival":
        rows = sorted(merged.values(), key=lambda f: f.get("lastSeen") or 0)
        fieldnames = [
            "icao24",
            "callsign",
            "first_seen_utc",
            "arrival_time_proxy_utc",
            "est_departure_airport",
            "est_arrival_airport",
            "ist_date",
            "ist_hour",
        ]
    else:
        rows = sorted(merged.values(), key=lambda f: f.get("firstSeen") or 0)
        fieldnames = [
            "icao24",
            "callsign",
            "departure_time_proxy_utc",
            "last_seen_utc",
            "est_departure_airport",
            "est_arrival_airport",
            "ist_date",
            "ist_hour",
        ]
    dest = csv_path(direction)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for flight in rows:
            callsign = (flight.get("callsign") or "").strip()
            date_bin, hour_bin = ist_bins(flight.get(proxy))
            base = {
                "icao24": flight.get("icao24"),
                "callsign": callsign,
                "est_departure_airport": flight.get("estDepartureAirport"),
                "est_arrival_airport": flight.get("estArrivalAirport"),
                "ist_date": date_bin,
                "ist_hour": hour_bin,
            }
            if direction == "arrival":
                base["first_seen_utc"] = flight.get("firstSeen")
                base["arrival_time_proxy_utc"] = flight.get("lastSeen")
            else:
                base["departure_time_proxy_utc"] = flight.get("firstSeen")
                base["last_seen_utc"] = flight.get("lastSeen")
            writer.writerow(base)
    quality = {
        "direction": direction,
        "rows": len(rows),
        "duplicates": duplicates,
        "nulls": nulls,
        "dedupe_key": ["icao24", "firstSeen"],
        "proxy_field": proxy,
        "time_kind": (
            "arrival_time_proxy"
            if direction == "arrival"
            else "departure_time_proxy"
        ),
    }
    quality_path(direction).write_text(json.dumps(quality, indent=2))
    log.info("wrote %s (%s unique %s)", dest.name, len(rows), direction)
    return quality


def _build_pending() -> deque[Window]:
    adopt_flat_arrival_cache()
    pending: deque[Window] = deque()
    for direction in DIRECTIONS:
        for start, end in windows():
            dest = cache_path(direction, start, end)
            if _nonempty(dest):
                continue
            if direction == "arrival" and window_file(direction, start, end):
                continue
            if direction == "arrival" and _link_legacy(start, end, dest):
                log.info("reused legacy cache %s", dest.name)
                continue
            pending.append((direction, start, end))
    return pending


def _finalize(exhausted: bool) -> dict[str, dict]:
    if exhausted:
        stamp_unfetched("quota")
    out: dict[str, dict] = {}
    for direction in DIRECTIONS:
        quality = write_csv(direction)
        status = coverage_report(direction)
        status["credits_exhausted"] = exhausted
        status["rows"] = quality["rows"]
        status["duplicates"] = quality["duplicates"]
        status["nulls"] = quality["nulls"]
        _write_status(direction, status)
        coverage_manifest_path(direction).write_text(
            json.dumps(status, indent=2)
        )
        out[direction] = status
    return out


def run_pool(clients: list[Client], fetch_fn) -> dict[str, dict]:
    """Fetch pending windows. One thread per client."""
    pending = _build_pending()
    inflight: set[tuple[str, dt.datetime, dt.datetime]] = set()
    attempts: dict[tuple[str, dt.datetime, dt.datetime], int] = {}
    failed: set[tuple[str, dt.datetime, dt.datetime]] = set()
    cond = threading.Condition()

    def _arrivals_inflight() -> bool:
        return any(item[0] == "arrival" for item in inflight)

    def claim() -> Window | None:
        with cond:
            while True:
                if pending:
                    if (
                        pending[0][0] == "departure"
                        and _arrivals_inflight()
                    ):
                        cond.wait()
                        continue
                    item = pending.popleft()
                    inflight.add(item)
                    return item
                if inflight:
                    cond.wait()
                    continue
                return None

    def release(item: Window, *, requeue: bool) -> None:
        with cond:
            inflight.discard(item)
            if requeue:
                pending.appendleft(item)
            cond.notify_all()

    def worker(client: Client) -> None:
        while True:
            item = claim()
            if item is None:
                return
            direction, start, end = item
            dest = cache_path(direction, start, end)
            if _nonempty(dest):
                release(item, requeue=False)
                continue
            try:
                raw = fetch_fn(client, direction, start, end)
            except OpenSkyCreditsExhausted:
                client.exhausted = True
                release(item, requeue=True)
                return
            except Exception:
                log.exception(
                    "OpenSky %s %s → %s failed on %s",
                    direction,
                    start.isoformat(),
                    end.isoformat(),
                    client.label(),
                )
                key = item
                attempts[key] = attempts.get(key, 0) + 1
                if attempts[key] >= MAX_ATTEMPTS:
                    write_window_status(
                        direction,
                        start,
                        end,
                        status="failed",
                        event_count=0,
                        retry_count=attempts[key],
                    )
                    with cond:
                        inflight.discard(item)
                        failed.add(item)
                        cond.notify_all()
                    continue
                release(item, requeue=True)
                time.sleep(1)
                continue
            result = coerce_fetch(raw)
            _write_window(dest, result.flights)
            write_window_status(
                direction,
                start,
                end,
                status=result.status,
                event_count=len(result.flights),
                http_status=result.http_status,
                retry_count=attempts.get(item, 0),
            )
            release(item, requeue=False)
            if client.exhausted or (
                client.remaining is not None
                and client.remaining < CREDITS_PER_WINDOW
            ):
                client.exhausted = True
                return

    if clients:
        with ThreadPoolExecutor(max_workers=len(clients)) as pool:
            list(pool.map(worker, clients))

    with cond:
        live = any(not client.exhausted for client in clients)
    credit_stop = (
        bool(clients)
        and bool(pending or inflight)
        and all(client.exhausted for client in clients)
    )
    bug = bool(pending or inflight) and live and bool(clients)
    statuses = _finalize(credit_stop)
    if bug:
        raise RuntimeError(
            "OpenSky workers stopped with windows left and a live client."
        )
    if credit_stop:
        log.error(
            "Stopped with credits exhausted. "
            "arrivals %s/%s, departures %s/%s.",
            statuses["arrival"]["windows_cached"],
            statuses["arrival"]["windows_required"],
            statuses["departure"]["windows_cached"],
            statuses["departure"]["windows_required"],
        )
        raise OpenSkyCreditsExhausted(
            "OpenSky flight credits are exhausted on every configured "
            f"client. A window costs {CREDITS_PER_WINDOW} credits."
        )
    if failed or pending or inflight:
        messages = [
            reason
            for reason in (
                invalid_reason("arrival"),
                invalid_reason("departure"),
            )
            if reason
        ]
        raise RuntimeError(
            "OpenSky fetch finished with gaps. " + "; ".join(messages)
        )
    return statuses


def fetch(settings: object) -> dict[str, dict]:
    with fetch_lock():
        clients = load_clients(settings)
        for direction in DIRECTIONS:
            info = coverage(direction)
            log.info(
                "%s %s/%s cached, %s credits still required, %s pairs",
                direction,
                info["windows_cached"],
                info["windows_required"],
                info["credits_if_uncached"],
                len(clients),
            )
        return run_pool(clients, fetch_window)


def probe(settings: object) -> list[dict]:
    """One arrival request per pair. Does not write the cache.

    Remaining credits are the header after that request. A 429 means
    the pair cannot buy another window.
    """
    with fetch_lock():
        return _probe(settings)


def _probe(settings: object) -> list[dict]:
    clients = load_clients(settings)
    start, end = windows()[0]

    def one(client: Client) -> dict:
        try:
            fetch_window(client, "arrival", start, end)
        except OpenSkyCreditsExhausted:
            pass
        except RuntimeError:
            log.error("OpenSky probe failed for %s", client.label())
            return {
                "index": client.index,
                "remaining": client.remaining,
                "exhausted": True,
                "login_failed": True,
            }
        return {
            "index": client.index,
            "remaining": client.remaining,
            "exhausted": client.exhausted
            or (
                client.remaining is not None
                and client.remaining < CREDITS_PER_WINDOW
            ),
            "login_failed": False,
        }

    with ThreadPoolExecutor(max_workers=len(clients)) as pool:
        rows = list(pool.map(one, clients))
    usable = sum(
        row["remaining"] or 0
        for row in rows
        if not row["exhausted"] and row["remaining"] is not None
    )
    log.info(
        "probe pairs=%s usable_credits_after_probe=%s",
        len(rows),
        usable,
    )
    for row in rows:
        log.info(
            "probe pair=%s remaining=%s exhausted=%s login_failed=%s",
            row["index"],
            row["remaining"],
            row["exhausted"],
            row["login_failed"],
        )
    return rows


def run(settings: object) -> None:
    """Fetch both directions. Exit 2 if every client is out of credits."""
    t0 = time.perf_counter()
    try:
        status = fetch(settings)
    except OpenSkyFetchBusy as exc:
        log.error("%s", exc)
        raise SystemExit(1) from exc
    except OpenSkyCreditsExhausted as exc:
        log.error("%s", exc)
        raise SystemExit(2) from exc
    log.info(
        "TIMING series=opensky wall_s=%.3f arrival_windows=%s "
        "arrival_rows=%s departure_windows=%s departure_rows=%s",
        time.perf_counter() - t0,
        status["arrival"].get("windows_cached"),
        status["arrival"].get("rows"),
        status["departure"].get("windows_cached"),
        status["departure"].get("rows"),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    import sys

    from flowstate.config import get_settings

    settings = get_settings()
    if "--probe" in sys.argv:
        try:
            probe(settings)
        except OpenSkyFetchBusy as exc:
            log.error("%s", exc)
            raise SystemExit(1) from exc
        return
    if "--plan" in sys.argv:
        clients = load_clients(settings)
        for direction in DIRECTIONS:
            info = coverage(direction)
            log.info(
                "plan %s: %s/%s cached, %s credits still required",
                direction,
                info["windows_cached"],
                info["windows_required"],
                info["credits_if_uncached"],
            )
        log.info("plan pairs=%s", len(clients))
        return
    run(settings)


if __name__ == "__main__":
    main()

"""Fetch VOBL arrivals for the Bengaluru alignment window.

Window: 2024-10-26 00:00 IST through 2026-04-15 23:59:59 IST.
Each query spans at most 2 UTC calendar days. Cost is 30 flight-credits
per query. A full uncached run is about 270 queries, about 8,100 credits.
One OpenSky client has 4,000 credits per day, so one client cannot finish.

Credentials come from Settings.opensky_clients(). Up to 4 pairs:

    FLOWSTATE_OPENSKY_CLIENT_ID / FLOWSTATE_OPENSKY_CLIENT_SECRET
    FLOWSTATE_OPENSKY_CLIENT_ID_2 / FLOWSTATE_OPENSKY_CLIENT_SECRET_2
    ...
    FLOWSTATE_OPENSKY_CLIENT_ID_4 / FLOWSTATE_OPENSKY_CLIENT_SECRET_4

Cached window files are not re-fetched. When every configured client has
fewer than 30 credits left, or returns 429, the fetch stops. It does not
sleep until the quota refills. The process exits non-zero after writing
whatever windows are already on disk.

lastSeen is stored as arrival_time_proxy_utc. That is an estimated arrival,
not a verified touchdown. Dedupe key is (icao24, firstSeen). No filter on
callsign or origin.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
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
API = "https://opensky-network.org/api/flights/arrival"
AIRPORT = "VOBL"
CREDITS_PER_WINDOW = 30
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UTC = dt.UTC
START = dt.datetime(2024, 10, 26, 0, 0, tzinfo=IST).astimezone(UTC)
END = dt.datetime(2026, 4, 15, 23, 59, 59, tzinfo=IST).astimezone(UTC)
CACHE = ROOT / "flowstate-datasets" / "opensky" / "windows"
CSV = (
    ROOT
    / "flowstate-datasets"
    / "opensky"
    / "VOBL_arrivals_2024-10-26_2026-04-15.csv"
)
LEGACY_CACHE = ROOT / ".scratch" / "dl" / "opensky"


class OpenSkyCreditsExhausted(RuntimeError):
    """Every configured client is below one window of flight credits."""


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
        return f"client {self.index} ({self.client_id})"


def load_clients(settings: object) -> list[Client]:
    from flowstate.config import Settings

    if not isinstance(settings, Settings):
        raise TypeError(settings)
    raw = settings.opensky_clients()
    if not raw:
        raise RuntimeError(
            "No OpenSky credentials. Set FLOWSTATE_OPENSKY_CLIENT_ID and "
            "FLOWSTATE_OPENSKY_CLIENT_SECRET. Pairs 2-4 use the "
            "_2, _3, and _4 suffix."
        )
    clients = [
        Client(index=index, client_id=client_id, secret=secret)
        for index, client_id, secret in raw
    ]
    log.info(
        "OpenSky clients configured: %s",
        ", ".join(client.label() for client in clients),
    )
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


def cache_path(start: dt.datetime, end: dt.datetime) -> Path:
    name = f"{AIRPORT}_{start:%Y%m%dT%H%M}_{end:%Y%m%dT%H%M}Z.json"
    return CACHE / name


def _link_legacy(start: dt.datetime, end: dt.datetime, dest: Path) -> bool:
    legacy = LEGACY_CACHE / dest.name
    if not legacy.exists() or legacy.stat().st_size == 0:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not dest.exists():
        try:
            dest.hardlink_to(legacy.resolve())
        except OSError:
            dest.write_bytes(legacy.read_bytes())
    return True


def _token(client: Client) -> str:
    if (
        client.token
        and client.expires_at
        and dt.datetime.now(UTC) < client.expires_at
    ):
        return client.token
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client.client_id,
            "client_secret": client.secret,
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"OpenSky login failed for {client.label()}: HTTP {r.status_code}"
        )
    data = r.json()
    client.token = data["access_token"]
    client.expires_at = dt.datetime.now(UTC) + dt.timedelta(
        seconds=int(data.get("expires_in", 1800)) - 30
    )
    return client.token


def _pick(clients: list[Client]) -> Client:
    usable = [
        c
        for c in clients
        if not c.exhausted
        and (c.remaining is None or c.remaining >= CREDITS_PER_WINDOW)
    ]
    if not usable:
        lines = []
        for c in clients:
            lines.append(
                f"  {c.label()}: remaining={c.remaining} "
                f"exhausted={c.exhausted} retry_after_s={c.retry_after_s}"
            )
        raise OpenSkyCreditsExhausted(
            "OpenSky flight credits are exhausted on every configured "
            "client. A window costs "
            f"{CREDITS_PER_WINDOW} credits. No further requests will be "
            "sent, and this process will not wait for the daily refill.\n"
            + "\n".join(lines)
        )
    return usable[0]


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
        "OpenSky credit limit expended for %s. remaining=%s retry_after_s=%s",
        client.label(),
        client.remaining,
        client.retry_after_s,
    )


def fetch_window(
    client: Client, start: dt.datetime, end: dt.datetime
) -> list[dict]:
    r = requests.get(
        API,
        params={
            "airport": AIRPORT,
            "begin": int(start.timestamp()),
            "end": int(end.timestamp()),
        },
        headers={"Authorization": f"Bearer {_token(client)}"},
        timeout=180,
    )
    remaining = r.headers.get("X-Rate-Limit-Remaining")
    if remaining is not None and remaining.isdigit():
        client.remaining = int(remaining)
    if r.status_code == 429:
        _mark_exhausted(client, r)
        raise OpenSkyCreditsExhausted(client.label())
    if r.status_code == 404:
        log.info(
            "%s %s → %s: no flights. credits left %s",
            client.label(),
            start.strftime("%Y-%m-%d %H:%M"),
            end.strftime("%Y-%m-%d %H:%M"),
            client.remaining,
        )
        return []
    if r.status_code != 200:
        raise RuntimeError(
            f"OpenSky HTTP {r.status_code} for {client.label()} "
            f"{start.isoformat()} → {end.isoformat()}: {r.text[:240]}"
        )
    if client.remaining is not None and client.remaining < CREDITS_PER_WINDOW:
        client.exhausted = True
        log.error(
            "OpenSky credit limit expended for %s after this window. "
            "remaining=%s, which is below %s.",
            client.label(),
            client.remaining,
            CREDITS_PER_WINDOW,
        )
    flights = r.json()
    log.info(
        "%s %s → %s: %s flights. credits left %s",
        client.label(),
        start.strftime("%Y-%m-%d %H:%M"),
        end.strftime("%Y-%m-%d %H:%M"),
        len(flights),
        client.remaining,
    )
    return flights


def coverage() -> dict:
    needed = windows()
    cached = 0
    for start, end in needed:
        path = cache_path(start, end)
        if path.exists() and path.stat().st_size > 0:
            cached += 1
        elif _link_legacy(start, end, path):
            cached += 1
    return {
        "windows_required": len(needed),
        "windows_cached": cached,
        "complete": cached == len(needed),
        "credits_per_window": CREDITS_PER_WINDOW,
        "credits_if_uncached": (len(needed) - cached) * CREDITS_PER_WINDOW,
        "window_start_utc": START.isoformat(),
        "window_end_utc": END.isoformat(),
    }


def _write_status(status: dict) -> None:
    path = CACHE.parent / "status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(status, indent=2))
    log.info("wrote %s", path)


def write_csv() -> int:
    merged: dict[tuple[str, int], dict] = {}
    for start, end in windows():
        path = cache_path(start, end)
        if not path.exists() or path.stat().st_size == 0:
            continue
        for flight in json.loads(path.read_text()):
            key = (flight.get("icao24", ""), int(flight.get("firstSeen") or 0))
            merged[key] = flight
    rows = sorted(merged.values(), key=lambda f: f.get("lastSeen") or 0)
    CSV.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with CSV.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "icao24",
                "callsign",
                "first_seen_utc",
                "arrival_time_proxy_utc",
                "est_departure_airport",
                "est_arrival_airport",
            ],
        )
        w.writeheader()
        for flight in rows:
            callsign = flight.get("callsign") or ""
            w.writerow(
                {
                    "icao24": flight.get("icao24"),
                    "callsign": callsign.strip(),
                    "first_seen_utc": flight.get("firstSeen"),
                    "arrival_time_proxy_utc": flight.get("lastSeen"),
                    "est_departure_airport": flight.get("estDepartureAirport"),
                    "est_arrival_airport": flight.get("estArrivalAirport"),
                }
            )
    log.info("wrote %s (%s unique arrivals)", CSV, len(rows))
    return len(rows)


def fetch(settings: object) -> dict:
    clients = load_clients(settings)
    needed = windows()
    log.info(
        "%s windows, %s → %s. Uncached cost %s credits.",
        len(needed),
        START.isoformat(),
        END.isoformat(),
        coverage()["credits_if_uncached"],
    )
    CACHE.mkdir(parents=True, exist_ok=True)
    try:
        for start, end in needed:
            dest = cache_path(start, end)
            if dest.exists() and dest.stat().st_size > 0:
                continue
            if _link_legacy(start, end, dest):
                log.info("reused legacy cache %s", dest.name)
                continue
            while True:
                client = _pick(clients)
                try:
                    flights = fetch_window(client, start, end)
                except OpenSkyCreditsExhausted:
                    continue
                dest.write_text(json.dumps(flights))
                break
    except OpenSkyCreditsExhausted as exc:
        status = coverage()
        status["credits_exhausted"] = True
        status["error"] = str(exc)
        status["rows"] = write_csv()
        _write_status(status)
        log.error(str(exc))
        log.error(
            "Stopped at %s/%s windows. Extract marked incomplete.",
            status["windows_cached"],
            status["windows_required"],
        )
        raise
    status = coverage()
    status["credits_exhausted"] = False
    status["rows"] = write_csv()
    _write_status(status)
    if not status["complete"]:
        raise RuntimeError(
            "OpenSky fetch finished without a credit error but "
            f"{status['windows_cached']}/{status['windows_required']} "
            "windows are on disk."
        )
    return status


def run(settings: object) -> None:
    """Fetch the alignment window. Exit 2 if every client is out of credits."""
    try:
        fetch(settings)
    except OpenSkyCreditsExhausted as exc:
        log.error(str(exc))
        raise SystemExit(2) from exc


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    import sys

    from flowstate.config import get_settings

    settings = get_settings()
    if "--plan" in sys.argv:
        info = coverage()
        clients = load_clients(settings)
        log.info(
            "plan: %s/%s windows cached, %s credits still required, %s clients",
            info["windows_cached"],
            info["windows_required"],
            info["credits_if_uncached"],
            len(clients),
        )
        return
    run(settings)


if __name__ == "__main__":
    main()

"""Hash local Bengaluru sources and record how each file was obtained.

`make acquire` writes flowstate-datasets/provenance/manifest.json from files
already on disk. `make acquire-fetch` re-downloads the HTTP sources first
(Namma Yatri city-day gzip, BMRCL RTI workbooks), then hashes them.

Does not re-fetch OpenSky (credit cost). It renames last_seen_utc to
arrival_time_proxy_utc if the old header is still there.

Does not fetch DGCA, BMTC, or the metro tracker. Those copies were already
in flowstate-datasets/. The manifest records the upstream repo and the local
mtime. No git commit is pinned, because this workspace does not contain
those clones.

Mumbai is out of scope.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ingest.manifest")

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "flowstate-datasets"
OUT = DATA / "provenance" / "manifest.json"
CSV_OPENSKY = (
    DATA / "opensky" / "VOBL_arrivals_2024-10-26_2026-04-15.csv"
)

NY_GZIP_URL = (
    "https://d11gklsvr97l1g.cloudfront.net/open/json-data/"
    "city_agg_cumulative_trends_new_key.json.gz"
)
RTI_URLS = {
    "aug_station_hourly.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "45259d6e-41b4-4012-8553-0d27219f83a7/download/"
        "a4ef58a3-29de-4787-b68e-56d716d0a95d.xlsx"
    ),
    "aug_entry_exit_od.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "e30ecb5f-e5f9-4971-8dd6-74d6966c33eb/download/"
        "12984f77-9bca-4854-a0d6-a527976080ac.xlsx"
    ),
    "sep_station_hourly.xlsx": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "6127e293-0f45-4a36-8f6b-2a3e23ce1c17/download/"
        "station-hourly.xlsx"
    ),
    "station_codes.csv": (
        "https://data.opencity.in/dataset/"
        "369f18e0-4342-4809-b380-44f1d21d904f/resource/"
        "1ec4f39a-eede-44d4-8e1d-e8658cb89762/download/"
        "c47d24a2-8c27-4c3a-a9cd-05c0beafc83d.csv"
    ),
}


def now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def file_record(path: Path, retrieved_at: str | None = None) -> dict:
    st = path.stat()
    stamp = retrieved_at or datetime.fromtimestamp(st.st_mtime, UTC).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    log.info("hashing %s", path.relative_to(ROOT))
    return {
        "path": str(path.relative_to(ROOT)),
        "bytes": st.st_size,
        "sha256": sha256(path),
        "retrieved_at_utc": stamp,
        "retrieved_at_is": "download" if retrieved_at else "file mtime",
    }


def download(url: str, dest: Path) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("GET %s", url)
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    dest.write_bytes(r.content)
    stamp = now()
    log.info("saved %s (%s bytes)", dest.name, dest.stat().st_size)
    return stamp


def metro_check() -> dict:
    path = DATA / "namma-metro" / "NammaMetro_Ridership_Processed.csv"
    df = pd.read_csv(path)
    df["d"] = pd.to_datetime(df["Record Date"], format="mixed", dayfirst=True)
    commute_ok = (df["Commute"] == df["Smart Cards"] + df["NCMC"]).all()
    casual_ok = (
        df["Casual"] == df["Tokens"] + df["QR"] + df["Group Ticket"]
    ).all()
    total_ok = (df["Total"] == df["Commute"] + df["Casual"]).all()
    days = pd.date_range(df["d"].min(), df["d"].max())
    have = set(df["d"].dt.normalize())
    missing = [d.strftime("%Y-%m-%d") for d in days if d not in have]
    return {
        "rows": int(len(df)),
        "duplicate_dates": int(df["d"].duplicated().sum()),
        "min": str(df["d"].min().date()),
        "max": str(df["d"].max().date()),
        "dates_after_2026_10_10": int((df["d"] > "2026-10-10").sum()),
        "missing_dates_in_span": len(missing),
        "formula_commute_eq_smartcards_plus_ncmc": bool(commute_ok),
        "formula_casual_eq_tokens_qr_group": bool(casual_ok),
        "formula_total_eq_commute_plus_casual": bool(total_ok),
        "do_not_use_dates_after": "2026-10-10",
    }


def dates_check() -> dict:
    path = DATA / "significant-dates" / "significant_dates.csv"
    df = pd.read_csv(path)
    return {
        "rows": int(len(df)),
        "duplicate_dates": int(df["Date"].duplicated().sum()),
        "min": str(df["Date"].min()),
        "max": str(df["Date"].max()),
        "event_counts": df["Event"].value_counts().to_dict(),
        "note": (
            "Festival and public-observance labels. Not a complete event "
            "calendar. Not the metro tracker's significant_dates.csv."
        ),
    }


def dgca_check() -> dict:
    path = DATA / "aviation" / "domestic" / "city.csv"
    df = pd.read_csv(path, usecols=["Year", "Month", "City1", "City2"])
    blr = df[(df.City1 == "BENGALURU") | (df.City2 == "BENGALURU")]
    ym = blr.Year.astype(str) + "-" + blr.Month.astype(str).str.zfill(2)
    return {
        "file": "flowstate-datasets/aviation/domestic/city.csv",
        "rows": int(len(df)),
        "bengaluru_pair_rows": int(len(blr)),
        "bengaluru_month_min": str(ym.min()),
        "bengaluru_month_max": str(ym.max()),
        "formula": (
            "Keep rows where City1 or City2 is BENGALURU. "
            "Month key is Year-Month. Do not expand a month into days."
        ),
    }


def build(fetch: bool) -> None:
    retrieved: dict[str, str] = {}
    if fetch:
        log.info("re-downloading HTTP sources")
        dest = (
            DATA / "namma-yatri" / "city_agg_cumulative_trends_new_key.json.gz"
        )
        retrieved[str(dest.relative_to(ROOT))] = download(NY_GZIP_URL, dest)
        for name, url in RTI_URLS.items():
            dest = DATA / "bmrcl-rti" / name
            retrieved[str(dest.relative_to(ROOT))] = download(url, dest)
    else:
        log.info("not downloading. Hashing files already on disk.")

    log.info("checking metro total formulas")
    metro = metro_check()
    log.info(
        "metro formulas commute=%s casual=%s total=%s",
        metro["formula_commute_eq_smartcards_plus_ncmc"],
        metro["formula_casual_eq_tokens_qr_group"],
        metro["formula_total_eq_commute_plus_casual"],
    )
    log.info("checking significant dates")
    holidays = dates_check()
    log.info(
        "significant dates rows=%s duplicate_dates=%s",
        holidays["rows"],
        holidays["duplicate_dates"],
    )
    log.info("checking DGCA Bengaluru slice")
    dgca = dgca_check()

    def rec(path: Path) -> dict:
        rel = str(path.relative_to(ROOT))
        return file_record(path, retrieved.get(rel))

    bmtc_files = sorted((DATA / "bmtc").glob("*.txt"))
    manifest = {
        "generated_at_utc": now(),
        "script": "flowstate.ingest",
        "scope": "Bengaluru only. Mumbai excluded. License inventory skipped.",
        "datasets": [
            {
                "dataset_id": "namma_yatri_city_day",
                "producer": "Namma Yatri operator open-data feed",
                "archive": None,
                "url": NY_GZIP_URL,
                "immutable_version": "live endpoint, no revision id",
                "grain": "city x day",
                "transformation": (
                    "None. Filter city_code=BLR and booking_type=Normal "
                    "at model-input time. Purple is a separate accessibility "
                    "product and is not in the learned clock."
                ),
                "role": "Layer 1 ride-hail clock",
                "file": rec(
                    DATA
                    / "namma-yatri"
                    / "city_agg_cumulative_trends_new_key.json.gz"
                ),
            },
            {
                "dataset_id": "namma_yatri_city_hour",
                "producer": "Namma Yatri operator open-data feed",
                "archive": "DigitalIndiaArchiver/NammaYatriStats",
                "url": (
                    "https://d11gklsvr97l1g.cloudfront.net/open/json-data/"
                    "city_agg_live_trends_new_key.json"
                ),
                "immutable_version": (
                    "git commits listed in archiver-cache/city/commits.tsv"
                ),
                "grain": "city x hour x vehicle",
                "transformation": (
                    "scripts/ny_archiver_stitch.py --series city. "
                    "Bengaluru rows only. Oldest commit first. "
                    "Later snapshot overwrites "
                    "(date, hour, trip_category, vehicle_type)."
                ),
                "role": "Layer 2 shape prior",
                "coverage": (
                    "namma-yatri/stitched/blr_city_hourly.coverage.json"
                ),
                "overwrites": (
                    "namma-yatri/stitched/blr_city_hourly.overwrites.json"
                ),
                "file": rec(DATA / "namma-yatri/stitched/blr_city_hourly.csv")
                if (DATA / "namma-yatri/stitched/blr_city_hourly.csv").exists()
                else {"path": None, "note": "run make ny-stitch-city first"},
            },
            {
                "dataset_id": "namma_yatri_ward_hour",
                "producer": "Namma Yatri operator open-data feed",
                "archive": "DigitalIndiaArchiver/NammaYatriStats",
                "url": (
                    "https://d11gklsvr97l1g.cloudfront.net/open/json-data/"
                    "trends_live_ward_new_key.json"
                ),
                "immutable_version": (
                    "git commits listed in archiver-cache/ward/commits.tsv"
                ),
                "grain": "ward x hour",
                "transformation": (
                    "scripts/ny_archiver_stitch.py --series ward. "
                    "Oldest commit first. Later snapshot overwrites "
                    "(ward_num, date, hour)."
                ),
                "role": "ride-hail zone placement",
                "coverage": (
                    "namma-yatri/stitched/blr_ward_hourly.coverage.json"
                ),
                "file": rec(DATA / "namma-yatri/stitched/blr_ward_hourly.csv"),
            },
            {
                "dataset_id": "bmrcl_rti",
                "producer": "BMRCL, via RTI reply published by OpenCity",
                "credit": "Vivek Mathew",
                "package": (
                    "https://data.opencity.in/dataset/"
                    "bmrcl-station-wise-ridership-data"
                ),
                "immutable_version": (
                    "OpenCity resource ids 45259d6e, e30ecb5f, "
                    "6127e293, 1ec4f39a"
                ),
                "grain": "station x hour, and station-pair x hour for August",
                "transformation": (
                    "No reshape yet. aug_station_hourly.xlsx is 2025-08-01 "
                    "to 2025-08-18. sep_station_hourly.xlsx has Entry and "
                    "Exit sheets for 2025-09. aug_entry_exit_od.xlsx is the "
                    "August station-pair matrix. Missing hours stay missing."
                ),
                "role": "metro hour-grain truth",
                "files": [
                    rec(DATA / "bmrcl-rti" / name) for name in RTI_URLS
                ],
            },
            {
                "dataset_id": "opensky_vobl_arrivals",
                "producer": "The OpenSky Network",
                "url": "https://opensky-network.org/api/flights/arrival",
                "immutable_version": "API query, not a versioned file",
                "citation": (
                    "Schaefer, Strohmeier, Lenders, Martinovic, Wilhelm. "
                    "Bringing Up OpenSky. IPSN 2014, pp. 83-94. "
                    "https://opensky-network.org"
                ),
                "grain": "one arrival event",
                "transformation": (
                    "scripts/opensky_arrivals.py. airport=VOBL. "
                    "Window 2024-10-26 00:00 IST through "
                    "2026-04-15 23:59 IST. "
                    "Each query spans at most 2 UTC calendar days. "
                    "HTTP 404 means no flights. "
                    "Dedupe key is (icao24, firstSeen). "
                    "No filter on callsign or origin. "
                    "lastSeen is stored as arrival_time_proxy_utc "
                    "(estimated arrival, not verified touchdown). "
                    "Clients rotate. Under 30 credits, the client is spent."
                ),
                "role": "aviation events for the alignment window",
                "file": rec(CSV_OPENSKY)
                if CSV_OPENSKY.exists()
                else {
                    "path": None,
                    "note": "alignment extract not written yet",
                },
            },
            {
                "dataset_id": "namma_metro_daily",
                "producer": "BMRCL daily figures, scraped by thecont1",
                "upstream": (
                    "https://github.com/thecont1/namma-metro-ridership-tracker"
                ),
                "immutable_version": "local copy, commit not pinned",
                "grain": "one network-day",
                "transformation": (
                    "None. Use the Total column. Do not also sum "
                    "the component columns."
                ),
                "checks": metro,
                "role": "Layer 1 metro clock",
                "file": rec(
                    DATA / "namma-metro" / "NammaMetro_Ridership_Processed.csv"
                ),
            },
            {
                "dataset_id": "dgca_domestic_city",
                "producer": "DGCA, parsed by Vonter/india-aviation-traffic",
                "upstream": "https://github.com/Vonter/india-aviation-traffic",
                "immutable_version": "local copy, commit not pinned",
                "grain": "year-month x city pair",
                "transformation": dgca["formula"],
                "checks": dgca,
                "role": "Layer 1 monthly aviation",
                "file": rec(DATA / "aviation" / "domestic" / "city.csv"),
            },
            {
                "dataset_id": "bmtc_gtfs",
                "producer": "Namma BMTC app, mirrored by Vonter/bmtc-gtfs",
                "upstream": "https://github.com/Vonter/bmtc-gtfs",
                "immutable_version": "feed_version 20260907, commit not pinned",
                "grain": "schedule. Not ridership.",
                "transformation": (
                    "Not built yet. Layer 3 bus events will be conditioned "
                    "on this timetable and labeled synthetic."
                ),
                "role": "Layer 3 bus conditioning and stop coordinates",
                "files": [rec(p) for p in bmtc_files],
            },
            {
                "dataset_id": "bengaluru_significant_dates",
                "producer": (
                    "Kaggle dataset "
                    "maheshshantaram/significant-dates-in-bangalore-india"
                ),
                "immutable_version": "Kaggle dataset version 13 at download",
                "grain": "one event-day",
                "transformation": "None.",
                "checks": holidays,
                "role": "optional holiday flags for Layer 1",
                "file": rec(
                    DATA / "significant-dates" / "significant_dates.csv"
                ),
            },
        ],
    }
    _stamp_validity(manifest["datasets"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(manifest, indent=2))
    log.info("wrote %s", OUT.relative_to(ROOT))


def _opensky_mod():
    from flowstate.ingest import opensky_arrivals

    return opensky_arrivals


def _stamp_validity(datasets: list[dict]) -> None:
    sky = _opensky_mod().coverage()
    status_path = DATA / "opensky" / "status.json"
    if status_path.exists():
        sky.update(json.loads(status_path.read_text()))
    reasons = {
        "namma_yatri_city_day": _reason_city_day(),
        "namma_yatri_city_hour": _reason_file(
            DATA / "namma-yatri/stitched/blr_city_hourly.csv",
            DATA / "namma-yatri/stitched/blr_city_hourly.coverage.json",
        ),
        "namma_yatri_ward_hour": _reason_file(
            DATA / "namma-yatri/stitched/blr_ward_hourly.csv",
            DATA / "namma-yatri/stitched/blr_ward_hourly.coverage.json",
        ),
        "bmrcl_rti": _reason_rti(),
        "opensky_vobl_arrivals": None
        if sky.get("complete")
        else (
            "OpenSky alignment window incomplete: "
            f"{sky.get('windows_cached')}/{sky.get('windows_required')} "
            "windows on disk. "
            + (
                "Credit limit expended. " + str(sky.get("error", ""))
                if sky.get("credits_exhausted")
                else "Run make ingest DATASET=aviation."
            )
        ),
        "namma_metro_daily": _reason_metro(),
        "dgca_domestic_city": _reason_dgca(),
        "bmtc_gtfs": _reason_bmtc(),
        "bengaluru_significant_dates": _reason_dates(),
    }
    for ds in datasets:
        reason = reasons.get(ds["dataset_id"])
        ds["valid"] = reason is None
        ds["invalid_reason"] = reason
        if ds["dataset_id"] == "opensky_vobl_arrivals":
            ds["checks"] = {
                k: sky.get(k)
                for k in (
                    "windows_required",
                    "windows_cached",
                    "complete",
                    "credits_if_uncached",
                    "credits_exhausted",
                )
            }


def _reason_file(csv: Path, coverage: Path) -> str | None:
    if not csv.exists():
        return f"missing {csv.name}"
    if not coverage.exists():
        return f"missing {coverage.name}"
    return None


def _reason_city_day() -> str | None:
    path = DATA / "namma-yatri" / "city_agg_cumulative_trends_new_key.json.gz"
    if not path.exists():
        return "missing city-day gzip"
    import gzip

    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    rows = json.loads(raw)
    dates = {
        r["date"]
        for r in rows
        if r.get("city_code") == "BLR" and r.get("booking_type") == "Normal"
    }
    if len(dates) < 537:
        return f"BLR Normal days {len(dates)}, expected at least 537"
    return None


def _reason_rti() -> str | None:
    missing = [n for n in RTI_URLS if not (DATA / "bmrcl-rti" / n).exists()]
    if missing:
        return "missing " + ", ".join(missing)
    return None


def _reason_metro() -> str | None:
    info = metro_check()
    if not info["formula_total_eq_commute_plus_casual"]:
        return "Total != Commute + Casual"
    if not info["formula_commute_eq_smartcards_plus_ncmc"]:
        return "Commute != Smart Cards + NCMC"
    if not info["formula_casual_eq_tokens_qr_group"]:
        return "Casual != Tokens + QR + Group Ticket"
    return None


def _reason_dgca() -> str | None:
    info = dgca_check()
    if info["bengaluru_pair_rows"] < 1:
        return "no BENGALURU rows in domestic/city.csv"
    if info["bengaluru_month_max"] < "2026-04":
        return "BENGALURU months end before the alignment window"
    return None


def _reason_bmtc() -> str | None:
    path = DATA / "bmtc" / "feed_info.txt"
    if not path.exists():
        return "missing feed_info.txt"
    if "20260907" not in path.read_text():
        return "feed_version is not 20260907"
    if not (DATA / "bmtc" / "stops.txt").exists():
        return "missing stops.txt"
    return None


def _reason_dates() -> str | None:
    info = dates_check()
    if info["rows"] != 137:
        return f"expected 137 rows, found {info['rows']}"
    if info["duplicate_dates"]:
        return "duplicate dates"
    return None


def refresh() -> None:
    """Rewrite the manifest from the files currently on disk."""
    build(False)


def validate() -> int:
    if not OUT.exists():
        log.error("No manifest. Run make ingest DATASET=manifest.")
        return 1
    manifest = json.loads(OUT.read_text())
    bad: list[str] = []
    for ds in manifest["datasets"]:
        files = ds.get("files") or []
        if ds.get("file"):
            files = [ds["file"], *files]
        for item in files:
            rel = item.get("path")
            if not rel:
                bad.append(f"{ds['dataset_id']}: no path")
                continue
            path = ROOT / rel
            if not path.exists():
                bad.append(f"{ds['dataset_id']}: missing {rel}")
                continue
            got = sha256(path)
            if got != item.get("sha256"):
                bad.append(
                    f"{ds['dataset_id']}: hash mismatch for {rel}. "
                    "Run make ingest DATASET=manifest."
                )
        if ds.get("valid") is not True:
            bad.append(
                f"{ds['dataset_id']}: not valid. {ds.get('invalid_reason')}"
            )
    if bad:
        for line in bad:
            log.error(line)
        return 1
    log.info("manifest matches disk and every dataset is valid")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(prog="flowstate.ingest.manifest")
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()
    if args.validate:
        sys.exit(validate())
    refresh()


if __name__ == "__main__":
    main()

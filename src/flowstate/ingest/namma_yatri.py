"""Re-fetch and stitch Namma Yatri archiver snapshots.

Producer: Namma Yatri open-data endpoints.
Archive: DigitalIndiaArchiver/NammaYatriStats (third party, not the producer).
This script is the transformation. Re-run with `make ny-stitch-ward` or
`make ny-stitch-city`. Existing snapshot files are skipped, so a second run
only fills gaps and rewrites the CSV and coverage manifest.

Merge rule: snapshots applied oldest commit first. A later snapshot overwrites
the same key. City key is (date, hour, trip_category, vehicle_type), Bengaluru
rows only. Ward key is (ward_num, date, hour).

Cache and outputs live under flowstate-datasets/ (gitignored):
  namma-yatri/archiver-cache/<series>/
  namma-yatri/stitched/<output csv + coverage json>
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ny-stitch")

REPO = "DigitalIndiaArchiver/NammaYatriStats"
ROOT = Path(__file__).resolve().parents[3]

SERIES = {
    "city": {
        "git_path": "raw-data/opendata/city_agg_live_trends_new_key.json",
        "out_name": "blr_city_hourly.csv",
        "key_cols": ["date", "hour", "trip_category", "vehicle_type"],
    },
    "ward": {
        "git_path": "raw-data/opendata/trends_live_ward_new_key.json",
        "out_name": "blr_ward_hourly.csv",
        "key_cols": ["ward_num", "date", "hour"],
    },
}


def _data(datasets_dir: Path) -> Path:
    return datasets_dir / "namma-yatri"


def cache_dir(series: str, datasets_dir: Path | None = None) -> Path:
    root = _data(datasets_dir or (ROOT / "flowstate-datasets"))
    path = root / "archiver-cache" / series
    path.mkdir(parents=True, exist_ok=True)
    return path


def out_dir(datasets_dir: Path | None = None) -> Path:
    root = _data(datasets_dir or (ROOT / "flowstate-datasets"))
    path = root / "stitched"
    path.mkdir(parents=True, exist_ok=True)
    return path


def load_commits(series: str, refresh: bool) -> list[tuple[str, str]]:
    spec = SERIES[series]
    path = cache_dir(series) / "commits.tsv"
    if refresh or not path.exists():
        log.info("asking GitHub for commits of %s", spec["git_path"])
        r = subprocess.run(
            [
                "gh",
                "api",
                "--paginate",
                f"repos/{REPO}/commits?path={spec['git_path']}&per_page=100",
                "--jq",
                r'.[] | "\(.sha)\t\(.commit.author.date)"',
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        path.write_text(r.stdout)
        log.info("wrote commit list %s", path)
    else:
        log.info("using cached commit list %s", path)
        log.info("pass --refresh-commits to refetch the list")
    rows = []
    for line in path.read_text().splitlines():
        sha, ts = line.split("\t", 1)
        rows.append((sha, ts))
    # gh returns newest first; stitch oldest first
    rows.reverse()
    return rows


def fetch_one(series: str, sha: str) -> Path | None:
    spec = SERIES[series]
    dest = cache_dir(series) / f"{sha}.json"
    if dest.exists() and dest.stat().st_size > 0:
        log.debug("cache hit %s", sha[:10])
        return dest
    url = f"https://raw.githubusercontent.com/{REPO}/{sha}/{spec['git_path']}"
    for attempt in range(1, 5):
        try:
            log.info("downloading %s attempt %s", sha[:10], attempt)
            r = requests.get(url, timeout=180)
            if r.status_code == 200 and r.text.startswith("["):
                dest.write_text(r.text)
                log.info("saved %s (%s bytes)", sha[:10], dest.stat().st_size)
                return dest
            log.warning("bad response %s status %s", sha[:10], r.status_code)
        except requests.RequestException as exc:
            log.warning("download error %s: %s", sha[:10], exc)
    log.error("gave up on %s", sha[:10])
    return None


def coverage(commits: list[tuple[str, str]], observed: pd.Series) -> dict:
    commit_dates = sorted({ts[:10] for _, ts in commits})
    start = datetime.fromisoformat(commit_dates[0])
    end = datetime.fromisoformat(commit_dates[-1])
    span = []
    d = start
    while d <= end:
        span.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    have = set(commit_dates)
    obs = sorted(pd.to_datetime(observed).dt.strftime("%Y-%m-%d").unique())
    return {
        "n_commits": len(commits),
        "distinct_commit_dates": len(commit_dates),
        "commit_date_min": commit_dates[0],
        "commit_date_max": commit_dates[-1],
        "calendar_days_in_commit_span": len(span),
        "commit_dates_missing": [x for x in span if x not in have],
        "observed_dates": len(obs),
        "observed_date_min": obs[0] if obs else None,
        "observed_date_max": obs[-1] if obs else None,
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def row_key(series: str, row: dict) -> tuple:
    if series == "city":
        return (
            row.get("date"),
            row.get("hour"),
            row.get("trip_category"),
            row.get("vehicle_type"),
        )
    return (row.get("ward_num"), row.get("date"), row.get("hour"))


def row_val(row: dict) -> tuple:
    return (
        row.get("done_ride"),
        row.get("booking"),
        row.get("earning"),
        row.get("cancel_ride"),
    )


def check_overwrites(series: str, commits: list[tuple[str, str]]) -> dict:
    """Count keys that reappear in a later snapshot with a different value.

    A repeat with the same value means last-write-wins is a no-op.
    A repeat with a different value means the stitch changes history.
    """
    log.info("overwrite check: comparing repeated keys across snapshots")
    seen: dict[tuple, tuple] = {}
    repeated = 0
    changed = 0
    examples: list[dict] = []
    for i, (sha, ts) in enumerate(commits, 1):
        path = cache_dir(series) / f"{sha}.json"
        if not path.exists() or path.stat().st_size == 0:
            continue
        try:
            rows = json.loads(path.read_text())
        except json.JSONDecodeError:
            log.warning("skip bad json %s", sha[:10])
            continue
        current: dict[tuple, tuple] = {}
        for row in rows:
            if series == "city" and row.get("city_code") != "BLR":
                continue
            current[row_key(series, row)] = row_val(row)
        for key, val in current.items():
            if key not in seen:
                continue
            repeated += 1
            if seen[key] != val:
                changed += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "key": [
                                None if x is None else str(x) for x in key
                            ],
                            "older": list(seen[key]),
                            "newer": list(val),
                            "newer_commit": sha[:10],
                            "newer_commit_time": ts,
                        }
                    )
        seen.update(current)
        if i % 100 == 0:
            log.info(
                "overwrite progress %s/%s repeated=%s changed=%s",
                i,
                len(commits),
                repeated,
                changed,
            )
    report = {
        "repeated_keys": repeated,
        "changed_keys": changed,
        "unchanged_keys": repeated - changed,
        "changed_fraction": (changed / repeated) if repeated else None,
        "fields_compared": ["done_ride", "booking", "earning", "cancel_ride"],
        "examples": examples,
    }
    log.info(
        "overwrite result: %s repeated keys, %s changed (%.4f)",
        repeated,
        changed,
        report["changed_fraction"] or 0,
    )
    return report


def stitch(series: str, refresh: bool, workers: int) -> None:
    spec = SERIES[series]
    log.info(
        "series=%s archive=%s file=%s",
        series,
        REPO,
        spec["git_path"],
    )
    log.info("cache %s", cache_dir(series))
    log.info("output %s", out_dir() / spec["out_name"])
    log.info(
        "merge: oldest commit first; a later snapshot overwrites key %s",
        spec["key_cols"],
    )
    commits = load_commits(series, refresh)
    log.info("%s commits to apply, oldest first", len(commits))
    failed = 0
    before = 0
    acc: pd.DataFrame | None = None
    pending: list[pd.DataFrame] = []
    with ThreadPoolExecutor(workers) as pool:
        futures = [pool.submit(fetch_one, series, sha) for sha, _ in commits]
        for i, fut in enumerate(futures, 1):
            path = fut.result()
            if path is None:
                failed += 1
            else:
                rows = json.loads(path.read_text())
                if series == "city":
                    rows = [r for r in rows if r.get("city_code") == "BLR"]
                if rows:
                    pending.append(pd.DataFrame(rows))
            if len(pending) >= 40 or (i == len(futures) and pending):
                chunk = pd.concat(pending, ignore_index=True)
                before += len(chunk)
                acc = (
                    chunk
                    if acc is None
                    else pd.concat([acc, chunk], ignore_index=True)
                )
                for col in spec["key_cols"]:
                    if col not in acc.columns:
                        acc[col] = None
                acc = acc.drop_duplicates(spec["key_cols"], keep="last")
                pending.clear()
            if i % 25 == 0 or i == len(futures):
                rows_now = 0 if acc is None else len(acc)
                log.info(
                    "progress %s/%s snapshots, failed %s, stitched rows %s",
                    i,
                    len(commits),
                    failed,
                    rows_now,
                )
    log.info("snapshots done, failed %s", failed)
    df = acc if acc is not None else pd.DataFrame()
    out = out_dir() / spec["out_name"]
    df.to_csv(out, index=False)
    manifest = coverage(commits, df["date"])
    manifest.update(
        {
            "series": series,
            "git_path": spec["git_path"],
            "archive": REPO,
            "producer": "Namma Yatri open data (operator feed)",
            "key_cols": spec["key_cols"],
            "merge_rule": (
                "oldest commit first; later snapshot overwrites the same key"
            ),
            "rows_before_dedupe": before,
            "rows_after_dedupe": len(df),
            "failed_snapshots": failed,
            "output": str(out.relative_to(ROOT)),
            "script": "flowstate.ingest.namma_yatri",
        }
    )
    man_path = out.with_suffix(".coverage.json")
    man_path.write_text(json.dumps(manifest, indent=2))
    log.info("wrote %s (%s rows)", out, len(df))
    log.info(
        "commits %s on %s dates; calendar span %s; missing commit dates %s",
        manifest["n_commits"],
        manifest["distinct_commit_dates"],
        manifest["calendar_days_in_commit_span"],
        len(manifest["commit_dates_missing"]),
    )
    log.info(
        "observed %s → %s (%s dates)",
        manifest["observed_date_min"],
        manifest["observed_date_max"],
        manifest["observed_dates"],
    )
    log.info("manifest %s", man_path)
    report = check_overwrites(series, commits)
    ow_path = out.with_suffix(".overwrites.json")
    ow_path.write_text(json.dumps(report, indent=2))
    log.info("wrote %s", ow_path)


NY_GZIP_URL = (
    "https://d11gklsvr97l1g.cloudfront.net/open/json-data/"
    "city_agg_cumulative_trends_new_key.json.gz"
)


def run(settings: object) -> None:
    """Download the city-day feed and stitch both hourly archives."""
    from flowstate.config import Settings

    if not isinstance(settings, Settings):
        raise TypeError(settings)
    dest = (
        settings.datasets_dir
        / "namma-yatri"
        / "city_agg_cumulative_trends_new_key.json.gz"
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("GET %s", NY_GZIP_URL)
    response = requests.get(NY_GZIP_URL, timeout=180)
    response.raise_for_status()
    dest.write_bytes(response.content)
    log.info("saved %s (%s bytes)", dest.name, dest.stat().st_size)
    stitch("city", False, 8)
    stitch("ward", False, 8)


if __name__ == "__main__":
    from flowstate.config import get_settings

    run(get_settings())

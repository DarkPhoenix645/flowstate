"""Re-fetch and stitch Namma Yatri archiver snapshots.

Producer: Namma Yatri open-data endpoints.
Archive: DigitalIndiaArchiver/NammaYatriStats (third party, not the producer).
Run `make ingest DATASET=ridehail`. `gh auth login` is required.
Commit lists and snapshot bodies both use that token.
Requests are serial. GitHub's primary limit is 5000 per hour.
A low remaining count or a 403/429 waits until the reset.
Existing snapshot files are skipped, so a second run only fills gaps.

Merge rule: snapshots applied oldest commit first. A later snapshot overwrites
the same key. City key is (date, hour, trip_category, vehicle_type), Bengaluru
rows only. Ward key is (ward_num, date, hour).

Cache and outputs live under data/raw/ (gitignored):
  namma-yatri/archiver-cache/<series>/
  namma-yatri/stitched/<output csv + coverage json>
"""

from __future__ import annotations

import base64
import json
import logging
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from flowstate.ingest.github import github_get as _github_get
from flowstate.ingest.github import github_token
from flowstate.ingest.github import too_large as _too_large
from flowstate.log import configure_logging

log = logging.getLogger("ny-stitch")

REPO = "DigitalIndiaArchiver/NammaYatriStats"
ROOT = Path(__file__).resolve().parents[3]
_API = "https://api.github.com"
# Leave headroom for the rest of `gh` during a long stitch.
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
    if datasets_dir is None:
        from flowstate.config import get_settings

        datasets_dir = get_settings().datasets_dir
    root = _data(datasets_dir)
    path = root / "archiver-cache" / series
    path.mkdir(parents=True, exist_ok=True)
    return path


def out_dir(datasets_dir: Path | None = None) -> Path:
    if datasets_dir is None:
        from flowstate.config import get_settings

        datasets_dir = get_settings().datasets_dir
    root = _data(datasets_dir)
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


@dataclass(frozen=True)
class Fetch:
    path: Path | None
    seconds: float
    nbytes: int
    kind: str  # cache, download, empty, failed


def _contents_url(series: str, sha: str) -> str:
    spec = SERIES[series]
    return (
        f"{_API}/repos/{REPO}/contents/{spec['git_path']}?ref={sha}"
    )


def _fetch_large_blob(
    series: str, sha: str, token: str
) -> str | None:
    """Contents API stops at 1 MB. The blobs API allows 100 MB."""
    meta = _github_get(
        _contents_url(series, sha),
        token,
        "application/vnd.github+json",
    )
    if meta.status_code != 200:
        log.warning(
            "blob meta %s status %s", sha[:10], meta.status_code
        )
        return None
    git_url = str(meta.json().get("git_url") or "")
    if not git_url:
        return None
    blob = _github_get(
        git_url, token, "application/vnd.github+json"
    )
    if blob.status_code != 200:
        log.warning(
            "blob body %s status %s", sha[:10], blob.status_code
        )
        return None
    payload = blob.json()
    if payload.get("encoding") != "base64":
        return None
    raw = base64.b64decode(payload.get("content") or "")
    return raw.decode()


def _upstream_empty(series: str, sha: str, token: str) -> bool:
    try:
        response = _github_get(
            _contents_url(series, sha),
            token,
            "application/vnd.github+json",
        )
    except requests.RequestException as exc:
        log.warning("size check %s failed: %s", sha[:10], exc)
        return False
    if response.status_code == 404:
        return True
    if response.status_code != 200:
        return False
    return int(response.json().get("size") or 0) == 0


def fetch_one(series: str, sha: str, token: str) -> Fetch:
    dest = cache_dir(series) / f"{sha}.json"
    if dest.exists() and dest.stat().st_size > 0:
        log.debug("cache hit %s", sha[:10])
        return Fetch(dest, 0.0, dest.stat().st_size, "cache")
    url = _contents_url(series, sha)
    spent = 0.0
    for attempt in range(1, 6):
        t0 = time.perf_counter()
        try:
            log.info("downloading %s attempt %s", sha[:10], attempt)
            response = _github_get(
                url, token, "application/vnd.github.raw"
            )
            spent += time.perf_counter() - t0
            text = response.text
            if _too_large(response):
                large = _fetch_large_blob(series, sha, token)
                if large is None:
                    text = ""
                else:
                    text = large
                    response.status_code = 200
            if response.status_code == 200 and text.startswith("["):
                dest.write_text(text)
                log.info(
                    "saved %s (%s bytes, %.2fs)",
                    sha[:10],
                    dest.stat().st_size,
                    spent,
                )
                return Fetch(dest, spent, dest.stat().st_size, "download")
            if response.status_code == 200:
                log.info(
                    "skip %s: upstream body is not a JSON array "
                    "(%s bytes)",
                    sha[:10],
                    len(text.encode()),
                )
                return Fetch(None, spent, 0, "empty")
            if response.status_code == 404 or _upstream_empty(
                series, sha, token
            ):
                log.info("skip %s: upstream file is missing", sha[:10])
                return Fetch(None, spent, 0, "empty")
            log.warning(
                "bad response %s status %s",
                sha[:10],
                response.status_code,
            )
        except requests.RequestException as exc:
            spent += time.perf_counter() - t0
            log.warning("download error %s: %s", sha[:10], exc)
        delay = min(2**attempt, 30)
        log.info("retry %s in %ss", sha[:10], delay)
        time.sleep(delay)
        spent += delay
    log.error("gave up on %s", sha[:10])
    return Fetch(None, spent, 0, "failed")


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


def stitch(series: str, refresh: bool) -> None:
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
    token = github_token()
    log.info("%s commits to apply, oldest first", len(commits))
    failed = 0
    empty = 0
    downloads = 0
    cache_hits = 0
    download_s = 0.0
    download_bytes = 0
    before = 0
    acc: pd.DataFrame | None = None
    pending: list[pd.DataFrame] = []
    wall0 = time.perf_counter()
    for i, (sha, _) in enumerate(commits, 1):
        fetched = fetch_one(series, sha, token)
        if fetched.kind == "download":
            downloads += 1
            download_s += fetched.seconds
            download_bytes += fetched.nbytes
        elif fetched.kind == "cache":
            cache_hits += 1
        elif fetched.kind == "empty":
            empty += 1
        else:
            failed += 1
        path = fetched.path
        if path is not None:
            rows = json.loads(path.read_text())
            if series == "city":
                rows = [r for r in rows if r.get("city_code") == "BLR"]
            if rows:
                pending.append(pd.DataFrame(rows))
        if len(pending) >= 40 or (i == len(commits) and pending):
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
        if i % 25 == 0 or i == len(commits):
            rows_now = 0 if acc is None else len(acc)
            log.info(
                "progress %s/%s snapshots, failed %s, stitched rows %s",
                i,
                len(commits),
                failed,
                rows_now,
            )
    wall_s = time.perf_counter() - wall0
    log.info(
        "snapshots done, failed %s, empty %s, downloads %s, cache %s",
        failed,
        empty,
        downloads,
        cache_hits,
    )
    if failed:
        raise SystemExit(
            f"ny-stitch {series}: {failed} snapshots failed; "
            "CSV not written"
        )
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
            "empty_snapshots": empty,
            "timing": {
                "wall_s": round(wall_s, 3),
                "download_s": round(download_s, 3),
                "downloads": downloads,
                "cache_hits": cache_hits,
                "bytes_downloaded": download_bytes,
                "seconds_per_download": (
                    round(download_s / downloads, 3) if downloads else None
                ),
                "seconds_per_output_row": (
                    round(wall_s / len(df), 6) if len(df) else None
                ),
            },
            "output": str(out.relative_to(ROOT)),
            "script": "flowstate.ingest.namma_yatri",
        }
    )
    man_path = out.with_suffix(".coverage.json")
    man_path.write_text(json.dumps(manifest, indent=2))
    timing = manifest["timing"]
    log.info(
        "TIMING series=%s wall_s=%s download_s=%s downloads=%s "
        "cache_hits=%s empty=%s failed=%s bytes=%s "
        "s_per_download=%s rows=%s s_per_output_row=%s",
        series,
        timing["wall_s"],
        timing["download_s"],
        timing["downloads"],
        timing["cache_hits"],
        empty,
        failed,
        timing["bytes_downloaded"],
        timing["seconds_per_download"],
        len(df),
        timing["seconds_per_output_row"],
    )
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
    configure_logging()
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
    t0 = time.perf_counter()
    response = requests.get(NY_GZIP_URL, timeout=180)
    response.raise_for_status()
    dest.write_bytes(response.content)
    log.info(
        "TIMING series=city_day wall_s=%.3f bytes=%s",
        time.perf_counter() - t0,
        dest.stat().st_size,
    )
    stitch("city", False)
    stitch("ward", False)


if __name__ == "__main__":
    from flowstate.config import get_settings

    run(get_settings())

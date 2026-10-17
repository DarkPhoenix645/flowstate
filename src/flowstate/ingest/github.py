"""Authenticated GitHub file reads for ingest.

`gh auth login` supplies the token. Do not print it.
`raw.githubusercontent.com` resets mid-download. The contents API
does not. A file over 1 MB uses the Git blobs API.
"""

from __future__ import annotations

import base64
import logging
import subprocess
import time

import requests

log = logging.getLogger("ingest.github")

_API = "https://api.github.com"
_MIN_REMAINING = 100


def github_token() -> str:
    """Token from `gh auth login`. Not printed."""
    result = subprocess.run(
        ["gh", "auth", "token"],
        capture_output=True,
        text=True,
        check=False,
    )
    token = result.stdout.strip()
    if result.returncode != 0 or not token:
        raise SystemExit("ingest needs `gh auth login`")
    return token


def too_large(response: requests.Response) -> bool:
    if response.status_code != 403:
        return False
    text = response.text.lower()
    return "1 mb" in text or "too large" in text


def rate_limit_delay(response: requests.Response) -> float:
    """Seconds to wait before another GitHub request.

    A 1 MB contents response is not a rate limit.
    403 and 429 use Retry-After, otherwise the primary reset.
    A successful response waits only when remaining is below
    ``_MIN_REMAINING``.
    """
    if too_large(response):
        return 0.0
    retry_after = response.headers.get("Retry-After")
    limited = response.status_code in (403, 429)
    if limited and retry_after and str(retry_after).isdigit():
        return float(retry_after)
    remaining = response.headers.get("X-RateLimit-Remaining")
    low = (
        remaining is not None
        and str(remaining).isdigit()
        and int(remaining) < _MIN_REMAINING
    )
    if not limited and not low:
        return 0.0
    reset = response.headers.get("X-RateLimit-Reset")
    if reset and str(reset).isdigit():
        return max(1.0, int(reset) - time.time() + 1)
    return 60.0


def github_get(
    url: str, token: str, accept: str
) -> requests.Response:
    headers = {
        "Accept": accept,
        "Authorization": f"Bearer {token}",
        "User-Agent": "flowstate-ingest",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response: requests.Response | None = None
    for _ in range(6):
        response = requests.get(url, headers=headers, timeout=60)
        if too_large(response):
            return response
        delay = rate_limit_delay(response)
        if response.status_code in (403, 429):
            log.warning(
                "GitHub %s, sleeping %.0fs",
                response.status_code,
                delay,
            )
            time.sleep(delay)
            continue
        if delay:
            log.info(
                "GitHub remaining %s, sleeping %.0fs",
                response.headers.get("X-RateLimit-Remaining"),
                delay,
            )
            time.sleep(delay)
        return response
    assert response is not None
    return response


def fetch_repo_file(repo: str, path: str, *, ref: str = "main") -> bytes:
    """One file from a public repo, via the contents API."""
    token = github_token()
    url = f"{_API}/repos/{repo}/contents/{path}?ref={ref}"
    log.info("GET %s", url)
    last_error: Exception | None = None
    for attempt in range(1, 6):
        try:
            response = github_get(
                url, token, "application/vnd.github.raw"
            )
            if too_large(response):
                return _blob(repo, path, ref, token)
            if response.status_code == 200:
                return response.content
            raise SystemExit(
                f"GitHub {response.status_code} for {repo}/{path}"
            )
        except requests.RequestException as exc:
            last_error = exc
            delay = min(2**attempt, 30)
            log.warning(
                "GitHub %s/%s attempt %s: %s",
                repo,
                path,
                attempt,
                exc,
            )
            time.sleep(delay)
    raise SystemExit(
        f"GitHub fetch failed for {repo}/{path}: {last_error}"
    )


def _blob(repo: str, path: str, ref: str, token: str) -> bytes:
    meta_url = f"{_API}/repos/{repo}/contents/{path}?ref={ref}"
    meta = github_get(meta_url, token, "application/vnd.github+json")
    if meta.status_code != 200:
        raise SystemExit(
            f"GitHub blob meta {meta.status_code} for {repo}/{path}"
        )
    git_url = str(meta.json().get("git_url") or "")
    if not git_url:
        raise SystemExit(f"GitHub blob has no git_url for {repo}/{path}")
    blob = github_get(git_url, token, "application/vnd.github+json")
    if blob.status_code != 200:
        raise SystemExit(
            f"GitHub blob {blob.status_code} for {repo}/{path}"
        )
    payload = blob.json()
    if payload.get("encoding") != "base64":
        raise SystemExit(f"GitHub blob encoding for {repo}/{path}")
    return base64.b64decode(payload.get("content") or "")

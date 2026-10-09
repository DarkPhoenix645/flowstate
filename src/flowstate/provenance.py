"""Check the acquisition manifest before a downstream job uses the files.

Call require_valid() at the start of a training or ingest step. It fails
if the manifest is missing, a file hash does not match the manifest, or a
dataset is marked not valid. It does not fetch or repair anything.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

MANIFEST = Path("flowstate-datasets/provenance/manifest.json")


class ProvenanceError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_valid(dataset_ids: list[str] | None = None) -> None:
    if not MANIFEST.exists():
        raise ProvenanceError(
            f"Missing {MANIFEST}. Run make ingest DATASET=manifest."
        )
    manifest = json.loads(MANIFEST.read_text())
    wanted = set(dataset_ids) if dataset_ids else None
    problems: list[str] = []
    for dataset in manifest["datasets"]:
        name = dataset["dataset_id"]
        if wanted is not None and name not in wanted:
            continue
        files = list(dataset.get("files") or [])
        if dataset.get("file"):
            files.insert(0, dataset["file"])
        for item in files:
            rel = item.get("path")
            if not rel:
                problems.append(f"{name}: manifest entry has no path")
                continue
            path = Path(rel)
            if not path.exists():
                problems.append(f"{name}: missing {rel}")
                continue
            if _sha256(path) != item.get("sha256"):
                problems.append(
                    f"{name}: {rel} hash does not match the manifest. "
                    "Run make ingest DATASET=manifest."
                )
        if dataset.get("valid") is not True:
            problems.append(
                f"{name}: not valid. {dataset.get('invalid_reason')}"
            )
    if problems:
        raise ProvenanceError("\n".join(problems))

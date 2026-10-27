"""Check the acquisition manifest before a downstream job uses the files.

Call require_valid() at the start of a training or ingest step. It fails
if the manifest is missing, a file hash does not match the manifest, or a
dataset is marked not valid. It does not fetch or repair anything.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

_RAW_PREFIX = Path("data") / "raw"


def manifest_path(datasets_dir: Path) -> Path:
    """The acquisition manifest for one raw directory."""
    return datasets_dir / "provenance" / "manifest.json"


def recorded_path(path: Path, datasets_dir: Path) -> str:
    """Manifest path string. ``data/raw/...`` when the file is in that tree."""
    try:
        suffix = path.resolve().relative_to(datasets_dir.resolve())
    except ValueError:
        return path.as_posix()
    return (_RAW_PREFIX / suffix).as_posix()


def locate_recorded_path(rel: str, datasets_dir: Path) -> Path:
    """A manifest path. Raw files resolve under ``datasets_dir``."""
    path = Path(rel)
    if path.is_absolute():
        return path
    try:
        suffix = path.relative_to(_RAW_PREFIX)
    except ValueError:
        from flowstate.config import repo_root

        return repo_root() / path
    return datasets_dir / suffix


class ProvenanceError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_valid(
    dataset_ids: list[str] | None = None,
    datasets_dir: Path | None = None,
) -> None:
    if datasets_dir is None:
        from flowstate.config import get_settings

        datasets_dir = get_settings().datasets_dir
    assert datasets_dir is not None
    manifest_file = manifest_path(datasets_dir)
    if not manifest_file.exists():
        raise ProvenanceError(
            f"Missing {manifest_file}. Run make ingest DATASET=manifest."
        )
    manifest = json.loads(manifest_file.read_text())
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
            path = locate_recorded_path(rel, datasets_dir)
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

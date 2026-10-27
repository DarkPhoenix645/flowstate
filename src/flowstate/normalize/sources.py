"""Gate reads on the acquisition manifest.

require_valid() runs once per dataset id, before any source file is read.
"""

from __future__ import annotations

import json
from pathlib import Path

from flowstate.provenance import manifest_path, require_valid

_checked: set[str] = set()


def ensure_sources(
    dataset_ids: list[str], datasets_dir: Path | None = None
) -> None:
    pending = [name for name in dataset_ids if name not in _checked]
    if not pending:
        return
    require_valid(pending, datasets_dir)
    _checked.update(pending)


def source_hashes(
    dataset_ids: list[str], datasets_dir: Path | None = None
) -> dict[str, list[dict[str, str]]]:
    """Hashes already stored in the manifest. Does not re-read the files."""
    if datasets_dir is None:
        from flowstate.config import get_settings

        datasets_dir = get_settings().datasets_dir
    assert datasets_dir is not None
    manifest = json.loads(manifest_path(datasets_dir).read_text())
    wanted = set(dataset_ids)
    found: dict[str, list[dict[str, str]]] = {}
    for dataset in manifest["datasets"]:
        name = dataset["dataset_id"]
        if name not in wanted:
            continue
        files = list(dataset.get("files") or [])
        if dataset.get("file"):
            files.insert(0, dataset["file"])
        found[name] = [
            {"path": item["path"], "sha256": item["sha256"]}
            for item in files
            if item.get("path") and item.get("sha256")
        ]
    return found


def reset_source_cache() -> None:
    _checked.clear()

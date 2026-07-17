"""Manifest persistence and idempotence checks."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any


def read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as stream:
        value = json.load(stream)
    return value if isinstance(value, dict) else None


def is_complete(path: Path, sha256: str, version: str) -> bool:
    manifest = read_manifest(path)
    return bool(
        manifest
        and manifest.get("sha256") == sha256
        and manifest.get("pipeline_version") == version
        and manifest.get("download_status") == "success"
    )


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)


def write_manifest_atomic(path: Path, manifest: dict[str, Any]) -> None:
    """Publish a manifest only after its complete JSON body was written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        temporary = Path(stream.name)
    temporary.replace(path)

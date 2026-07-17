"""Resumable, hash-verified archive downloads."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests


class DownloadError(RuntimeError):
    """A remote archive could not be downloaded safely."""


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    source_url: str
    sha256: str
    size_bytes: int
    elapsed_seconds: float


def build_archive_url(template: str, archive_date: date, hour: int) -> str:
    if not 0 <= hour <= 23:
        raise ValueError("hour must be between 0 and 23")
    return template.format(
        year=archive_date.strftime("%Y"),
        month=archive_date.strftime("%m"),
        day=archive_date.strftime("%d"),
        hour=f"{hour:02d}",
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_archive(url: str, destination: Path, timeout_seconds: int) -> DownloadResult:
    """Download to ``.part`` and resume it only when the server honors ranges."""
    import time

    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size > 0:
        raise DownloadError(f"Refusing to replace existing file: {destination}")
    partial = destination.with_suffix(destination.suffix + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"Range": f"bytes={offset}-"} if offset else {}
    started = time.monotonic()
    try:
        response = requests.get(url, stream=True, timeout=timeout_seconds, headers=headers)
        if response.status_code == 416 and partial.exists():
            response.close()
        else:
            if response.status_code not in (200, 206):
                response.raise_for_status()
            if offset and response.status_code != 206:
                partial.unlink(missing_ok=True)
                return download_archive(url, destination, timeout_seconds)
            with partial.open("ab" if offset else "wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
    except requests.RequestException as error:
        raise DownloadError(f"Download failed for {url}: {error}") from error
    if not partial.exists() or partial.stat().st_size == 0:
        raise DownloadError(f"Downloaded file is empty: {url}")
    partial.replace(destination)
    return DownloadResult(
        destination,
        url,
        sha256_file(destination),
        destination.stat().st_size,
        time.monotonic() - started,
    )

"""Safe discovery of data files on macOS and external volumes."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

PARQUET_MAGIC = b"PAR1"
MIN_PARQUET_BYTES = 12


def is_auxiliary_file(path: Path) -> bool:
    return (
        path.name.startswith("._")
        or path.name == ".DS_Store"
        or path.suffix in {".partial", ".tmp"}
    )


def is_valid_parquet(path: Path) -> bool:
    """Perform a constant-size validation without parsing Parquet metadata."""
    if is_auxiliary_file(path) or not path.is_file() or path.stat().st_size < MIN_PARQUET_BYTES:
        return False
    try:
        with path.open("rb") as stream:
            if stream.read(4) != PARQUET_MAGIC:
                return False
            stream.seek(-4, 2)
            return stream.read(4) == PARQUET_MAGIC
    except OSError:
        return False


def discover_parquet(directory: Path, recursive: bool = False) -> Iterator[Path]:
    candidates = directory.rglob("*.parquet") if recursive else directory.glob("*.parquet")
    for path in sorted(candidates):
        if is_valid_parquet(path):
            yield path

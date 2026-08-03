"""Streaming Zstandard JSON Lines reader."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import zstandard as zstd


@dataclass(frozen=True)
class JsonLine:
    line_number: int
    payload: dict[str, Any] | None
    raw: str
    error: str | None = None


def iter_jsonl_zst(path: Path) -> Iterator[JsonLine]:
    """Yield parsed objects while retaining invalid JSON lines for rejection logs."""
    try:
        with (
            path.open("rb") as compressed,
            zstd.ZstdDecompressor().stream_reader(compressed) as reader,
        ):
            import io

            with io.TextIOWrapper(reader, encoding="utf-8") as text:
                for line_number, raw in enumerate(text, start=1):
                    raw = raw.rstrip("\r\n")
                    if not raw:
                        continue
                    try:
                        item = json.loads(raw)
                        if not isinstance(item, dict):
                            raise ValueError("JSON value is not an object")
                        yield JsonLine(line_number, item, raw)
                    except (json.JSONDecodeError, ValueError) as error:
                        yield JsonLine(line_number, None, raw, str(error))
    except zstd.ZstdError as error:
        raise ValueError(f"Invalid or truncated Zstandard archive: {path}") from error

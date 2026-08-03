from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from atlaspump.config import load_settings
from atlaspump.contracts import (
    canonical_observation_id,
    logical_event_id,
    provider_position,
    source_event_id,
)
from atlaspump.day_pipeline import collect_day, normalize_day
from atlaspump.manifests import read_manifest

DAY = date(2026, 4, 19)


def _archive(root: Path, hour: int, lines: list[str]) -> Path:
    path = root / "2026/04/19" / f"{hour:02d}.jsonl.zst"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(zstd.ZstdCompressor().compress(("\n".join(lines) + "\n").encode()))
    return path


def test_rfc_identity_and_positions_are_explicit() -> None:
    raw = '{"signature":"s","instructionIndex":2,"mint":"m"}'
    source = source_event_id("pumpapi", "00", "1", raw)
    assert source == source_event_id("pumpapi", "00", "1", raw)
    assert source != source_event_id("pumpapi", "00", "1", raw + " ")
    assert canonical_observation_id(source) != canonical_observation_id(source, 1)
    payload = {"signature": "s", "instructionIndex": 2, "mint": "m", "block": 7}
    assert logical_event_id(payload, "BUY", "m") is not None
    assert logical_event_id({"signature": "s", "mint": "m"}, "BUY", "m") is None
    assert provider_position(payload) == (None, None, "7")
    assert provider_position({"slot": 3, "blockHeight": 2, "block": 1}) == (3, 2, "1")


def test_collection_and_normalization_are_atomic_and_resumable(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _archive(input_root, 0, [json.dumps({"txType": "buy", "signature": "same", "instructionIndex": 1, "slot": 4, "block": 99, "mint": "m", "txSigner": "w"}), "{invalid"])
    _archive(input_root, 1, [json.dumps({"txType": "buy", "signature": "same", "instructionIndex": 1, "slot": 4, "mint": "m", "txSigner": "w"}), json.dumps({"txType": "other", "mint": "unknown"})])
    root = tmp_path / "output"
    collection_path = collect_day(root, input_root, DAY, [0, 1, 2], resume=False)
    collection = read_manifest(collection_path)
    assert collection is not None
    assert collection["coverage_status"] == "PARTIAL"
    assert collection["contract_status"] == "NOT_SATISFIED"
    assert collection["usability_status"] == "LIMITED"
    assert collection_path == collect_day(root, input_root, DAY, [0, 1, 2], resume=True)
    normalization_path = normalize_day(root, DAY, collection_path, load_settings().values)
    normalization = read_manifest(normalization_path)
    assert normalization is not None
    assert normalization["manifest_type"] == "NORMALIZATION"
    assert normalization["counters"]["invalid"] == 1
    assert normalization["counters"]["unknown"] == 1
    assert normalization["counters"]["logical_resolved"] == 1
    output = root / normalization["normalized_files"][0]["path"]
    assert output.read_bytes()[:4] == b"PAR1" and output.read_bytes()[-4:] == b"PAR1"
    table = pq.read_table(output)
    assert {"source_event_id", "canonical_observation_id", "logical_event_id", "blockchain_timestamp", "archive_timestamp", "slot", "block_height", "provider_block", "raw_reference"} <= set(table.column_names)
    assert table.num_rows == 3
    with pytest.raises(FileExistsError):
        normalize_day(root, DAY, collection_path, load_settings().values)


def test_collection_complete_and_collision_refusal(tmp_path: Path) -> None:
    input_root = tmp_path / "input"
    _archive(input_root, 0, [json.dumps({"txType": "buy", "signature": "a", "instructionIndex": 1})])
    root = tmp_path / "output"
    manifest_path = collect_day(root, input_root, DAY, [0])
    assert read_manifest(manifest_path)["coverage_status"] == "COMPLETE"  # type: ignore[index]
    raw = root / "raw/pumpapi/replay/date=2026-04-19/archives/00.jsonl.zst"
    raw.write_bytes(b"different")
    with pytest.raises(FileExistsError):
        collect_day(root, input_root, DAY, [0], resume=False)

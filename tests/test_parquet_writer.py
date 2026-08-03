from pathlib import Path

import pyarrow.parquet as pq

from atlaspump.config import load_settings
from atlaspump.decompressor import iter_jsonl_zst
from atlaspump.event_classifier import normalize_event
from atlaspump.parquet_writer import ParquetWriter


def test_reads_invalid_json_and_writes_parquet(archive: Path, tmp_path: Path) -> None:
    rows = list(iter_jsonl_zst(archive))
    assert len(rows) == 3
    assert rows[1].payload is None
    writer = ParquetWriter(tmp_path, batch_size=1)
    config = load_settings().values
    for record in rows:
        if record.payload:
            writer.add(normalize_event(record.payload, config))
    paths = writer.close()
    all_file = next(path for path in paths if path.name == "events_all.parquet")
    assert pq.read_table(all_file).num_rows == 2


def test_truncated_archive_raises(tmp_path: Path) -> None:
    path = tmp_path / "truncated.zst"
    path.write_bytes(b"not zstandard")
    import pytest

    with pytest.raises(ValueError):
        list(iter_jsonl_zst(path))

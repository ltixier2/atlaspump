from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from atlaspump.file_discovery import discover_parquet, is_valid_parquet


def test_discovers_only_valid_parquet_files(tmp_path: Path) -> None:
    valid = tmp_path / "with spaces.parquet"
    pq.write_table(pa.table({"value": [1]}), valid)
    (tmp_path / "._with spaces.parquet").write_bytes(b"PAR1not-a-parquetPAR1")
    (tmp_path / "empty.parquet").touch()
    (tmp_path / "fake.parquet").write_text("not parquet", encoding="utf-8")
    (tmp_path / "partial.parquet.partial").write_bytes(b"PAR1partialPAR1")
    assert list(discover_parquet(tmp_path)) == [valid]
    assert is_valid_parquet(valid)

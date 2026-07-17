from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from atlaspump.lifecycle_builder import EVENT_SCHEMA, build_token_lifecycle


def test_stable_schema_when_first_batch_is_all_null(tmp_path: Path) -> None:
    path = tmp_path / "events.parquet"
    writer = pq.ParquetWriter(path, EVENT_SCHEMA, compression="zstd")
    base = {field.name: None for field in EVENT_SCHEMA}
    base.update({"mint": "mint-a", "event_id": "a", "event_type": "BUY"})
    writer.write_table(pa.Table.from_pylist([base], schema=EVENT_SCHEMA))
    base.update({"event_id": "b", "pool": "pump", "block": 1, "sequence_index": 0})
    writer.write_table(pa.Table.from_pylist([base], schema=EVENT_SCHEMA))
    writer.close()
    table = pq.read_table(path)
    assert table.schema == EVENT_SCHEMA
    assert table.column("pool").to_pylist() == [None, "pump"]


def test_lifecycle_migration_and_censoring() -> None:
    rows = [
        {
            "token_mint": "m",
            "event_id": "1",
            "event_type": "CREATE_TOKEN",
            "blockchain_timestamp": 3,
            "block": 1,
            "signature": "a",
            "protocol_scope": "PUMPFUN_BONDING_CURVE",
            "wallet": "w",
        },
        {
            "token_mint": "m",
            "event_id": "2",
            "event_type": "MIGRATE",
            "blockchain_timestamp": 2,
            "block": 1,
            "signature": "b",
            "protocol_scope": "PUMPFUN_BONDING_CURVE",
            "wallet": "w",
        },
        {
            "token_mint": "m",
            "event_id": "2",
            "event_type": "BUY",
            "blockchain_timestamp": 4,
            "block": 2,
            "signature": "c",
            "protocol_scope": "PUMPSWAP",
            "wallet": "w",
        },
    ]
    lifecycle, _, anomalies = build_token_lifecycle(rows)
    assert lifecycle["migration_explicit"]
    assert lifecycle["right_censored"]
    assert any(item["anomaly_type"] == "DUPLICATE_EVENT" for item in anomalies)

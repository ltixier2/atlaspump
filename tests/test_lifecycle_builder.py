from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from atlaspump.lifecycle_builder import EVENT_SCHEMA, build_lifecycles, build_token_lifecycle


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


def test_builder_accepts_rfc003_positions_and_preserves_references(tmp_path: Path) -> None:
    source = tmp_path / "canonical.parquet"
    schema = pa.schema(
        [
            ("source_event_id", pa.string()),
            ("canonical_observation_id", pa.string()),
            ("logical_event_id", pa.string()),
            ("event_type", pa.string()),
            ("protocol_scope", pa.string()),
            ("signature", pa.string()),
            ("blockchain_timestamp", pa.int64()),
            ("slot", pa.int64()),
            ("block_height", pa.int64()),
            ("provider_block", pa.string()),
            ("raw_reference", pa.string()),
            ("token_mint", pa.string()),
            ("wallet", pa.string()),
            ("sol_amount", pa.string()),
            ("token_amount", pa.string()),
        ]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source_event_id": "source-1",
                    "canonical_observation_id": "canonical-1",
                    "logical_event_id": None,
                    "event_type": "CREATE_TOKEN",
                    "protocol_scope": "PUMPFUN",
                    "signature": "signature-1",
                    "blockchain_timestamp": 1,
                    "slot": None,
                    "block_height": None,
                    "provider_block": "provider-block",
                    "raw_reference": "raw/example.jsonl.zst#1",
                    "token_mint": "mint-1",
                    "wallet": "wallet-1",
                    "sol_amount": None,
                    "token_amount": None,
                }
            ],
            schema=schema,
        ),
        source,
    )
    output = tmp_path / "lifecycles"
    result = build_lifecycles(source, output)
    assert result["token_count"] == 1
    events = pq.read_table(output / "token_events.parquet").to_pylist()
    assert events[0]["canonical_observation_id"] == "canonical-1"
    assert events[0]["provider_block"] == "provider-block"
    assert events[0]["slot"] is None
    lifecycle = pq.read_table(output / "token_lifecycles.parquet").to_pylist()[0]
    assert lifecycle["coverage_status"] == "COMPLETE"
    assert lifecycle["censoring_status"] == "RIGHT"


def test_builder_refuses_to_overwrite_published_outputs(tmp_path: Path) -> None:
    output = tmp_path / "lifecycles"
    output.mkdir()
    (output / "token_events.parquet").touch()
    with pytest.raises(FileExistsError):
        build_lifecycles(tmp_path / "missing.parquet", output)

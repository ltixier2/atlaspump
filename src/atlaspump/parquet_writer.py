"""Bounded-memory Parquet output."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema(
    [
        (name, pa.string())
        for name in (
            "event_id",
            "event_type",
            "source",
            "signature",
            "token_mint",
            "wallet",
            "creator",
            "pool",
            "side",
            "sol_amount",
            "token_amount",
            "price",
            "market_cap_sol",
            "bonding_curve_progress",
            "raw_event_type",
            "schema_version",
            "raw_payload_json",
            "pool_created_by",
            "protocol_scope",
            "pool_id",
            "sol_in_pool",
            "tokens_in_pool",
            "v_sol_in_bonding_curve",
            "v_tokens_in_bonding_curve",
            "priority_fee",
            "token_program",
        )
    ]
    + [
        ("block", pa.int64()),
        ("blockchain_timestamp", pa.int64()),
        ("archive_timestamp", pa.int64()),
    ]
)


class ParquetWriter:
    def __init__(self, directory: Path, batch_size: int) -> None:
        self.directory = directory
        self.batch_size = batch_size
        self.buffers: dict[str, list[dict[str, Any]]] = {
            label: []
            for label in (
                "all",
                "create",
                "buy",
                "sell",
                "migration",
                "unknown",
                "pumpfun",
                "pumpswap",
            )
        }
        self.writers: dict[str, pq.ParquetWriter] = {}
        self.paths: dict[str, Path] = {}

    def add(self, row: dict[str, Any]) -> None:
        self._add_to("all", row)
        category = str(row["event_type"]).lower()
        if category in {"create", "buy", "sell", "migration", "unknown"}:
            self._add_to(category, row)
        if row["protocol_scope"] == "PUMPFUN_BONDING_CURVE":
            self._add_to("pumpfun", row)
        if row["protocol_scope"] == "PUMPSWAP":
            self._add_to("pumpswap", row)

    def _add_to(self, label: str, row: dict[str, Any]) -> None:
        buffer = self.buffers.setdefault(label, [])
        buffer.append(row)
        if len(buffer) >= self.batch_size:
            self._flush(label)

    def _flush(self, label: str) -> None:
        buffer = self.buffers.get(label, [])
        if not buffer:
            return
        path = self.directory / f"events_{label}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = self.writers.get(label)
        if writer is None:
            writer = pq.ParquetWriter(path, SCHEMA, compression="zstd")
            self.writers[label] = writer
            self.paths[label] = path
        writer.write_table(pa.Table.from_pylist(buffer, schema=SCHEMA))
        buffer.clear()

    def close(self) -> list[Path]:
        for label in list(self.buffers):
            self._flush(label)
            if label not in self.writers:
                path = self.directory / f"events_{label}.parquet"
                path.parent.mkdir(parents=True, exist_ok=True)
                writer = pq.ParquetWriter(path, SCHEMA, compression="zstd")
                writer.write_table(pa.Table.from_pylist([], schema=SCHEMA))
                writer.close()
                self.paths[label] = path
        for writer in self.writers.values():
            writer.close()
        return list(self.paths.values())


def parquet_size(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths)

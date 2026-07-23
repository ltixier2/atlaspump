"""Explicit, loss-aware adapters for token-summary EDA inputs."""

from __future__ import annotations

import pyarrow as pa

COMMON_VERSION = "token_summary_eda_v2_common"
RFC_VERSION = "token_summary_eda_v2_enriched_rfc"
COMMON_SCHEMA = pa.schema(
    [
        ("token_mint", pa.string()),
        ("logical_date", pa.string()),
        ("creation_timestamp_ms", pa.int64()),
        ("observation_duration_ms", pa.int64()),
        ("event_count", pa.int64()),
        ("unique_wallet_count", pa.int64()),
        ("migration_observed", pa.bool_()),
        ("events_1m", pa.int64()),
        ("source_backend", pa.string()),
        ("source_schema_version", pa.string()),
        ("has_detailed_windows", pa.bool_()),
        ("has_detailed_censoring", pa.bool_()),
        ("has_wallet_metrics", pa.bool_()),
    ]
)


def adapt_common(batch: pa.RecordBatch, date: str, backend: str) -> pa.RecordBatch:
    names = set(batch.schema.names)
    required = {
        "mint",
        "creation_timestamp",
        "observation_duration_ms",
        "event_count",
        "migrated",
        "events_1m",
    }
    missing = required - names
    if missing:
        raise ValueError(f"required source columns absent: {sorted(missing)}")
    n = batch.num_rows

    def col(name: str, typ: pa.DataType, default=None):
        return (
            batch.column(batch.schema.get_field_index(name)).cast(typ)
            if name in names
            else pa.array([default] * n, type=typ)
        )

    detailed = backend == "rfc_partitioned"
    return pa.record_batch(
        [
            col("mint", pa.string()),
            pa.array([date] * n, pa.string()),
            col("creation_timestamp", pa.int64()),
            col("observation_duration_ms", pa.int64()),
            col("event_count", pa.int64()),
            col("unique_wallet_count", pa.int64()),
            col("migrated", pa.bool_()),
            col("events_1m", pa.int64()),
            pa.array([backend] * n, pa.string()),
            pa.array([COMMON_VERSION] * n, pa.string()),
            pa.array([detailed] * n, pa.bool_()),
            pa.array([detailed] * n, pa.bool_()),
            pa.array([detailed] * n, pa.bool_()),
        ],
        schema=COMMON_SCHEMA,
    )

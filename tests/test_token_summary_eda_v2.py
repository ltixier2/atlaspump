from __future__ import annotations

import pytest
import pyarrow as pa

from atlaspump.token_summary_eda_v2 import COMMON_VERSION, adapt_common


def test_adapt_common_preserves_values_and_marks_legacy_availability() -> None:
    batch = pa.record_batch(
        [
            pa.array(["mint-a"]), pa.array([1704067200000], type=pa.int64()),
            pa.array([60000], type=pa.int64()), pa.array([4], type=pa.int64()),
            pa.array([True]), pa.array([2], type=pa.int64()),
        ],
        names=["mint", "creation_timestamp", "observation_duration_ms", "event_count", "migrated", "events_1m"],
    )
    result = adapt_common(batch, "2026-04-18", "legacy")
    row = result.to_pylist()[0]
    assert row["token_mint"] == "mint-a"
    assert row["logical_date"] == "2026-04-18"
    assert row["source_schema_version"] == COMMON_VERSION
    assert row["migration_observed"] is True
    assert row["unique_wallet_count"] is None
    assert row["has_detailed_windows"] is False


def test_adapt_common_rejects_missing_required_source_column() -> None:
    batch = pa.record_batch([pa.array(["mint-a"])], names=["mint"])
    with pytest.raises(ValueError, match="required source columns absent"):
        adapt_common(batch, "2026-04-18", "legacy")

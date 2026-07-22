from __future__ import annotations

import pytest

from atlaspump.features.engine import common_record, rfc_record, safe_ratio, stable_sample_mints
from atlaspump.features.registry import (
    FEATURES,
    horizon_features,
    validate_horizon_selection,
    validate_registry,
)


def test_registry_is_valid_and_unique() -> None:
    validate_registry()
    assert len({spec.feature_name for spec in FEATURES}) == len(FEATURES)


def test_horizons_reject_future_and_post_outcome_features() -> None:
    assert "events_10s" in horizon_features(10)
    assert "events_30s" not in horizon_features(10)
    assert "buy_count" not in horizon_features(3600)
    with pytest.raises(ValueError, match="future"):
        validate_horizon_selection(["events_30s"], 10)
    with pytest.raises(ValueError, match="post-outcome"):
        validate_horizon_selection(["buy_count"], 3600)
    with pytest.raises(ValueError, match="unregistered"):
        validate_horizon_selection(["migration"], 60)


def test_safe_ratio_preserves_unknown_and_zero_denominator() -> None:
    assert safe_ratio(1, 0) is None
    assert safe_ratio(None, 2) is None
    assert safe_ratio(3, 2) == 1.5


def test_stable_sample_is_deterministic_and_stratified() -> None:
    rows = [(f"m{i}", i % 2 == 0) for i in range(200)]
    assert stable_sample_mints(rows) == stable_sample_mints(list(reversed(rows)))
    assert len(stable_sample_mints(rows)) == 100


def test_common_record_derives_timezone_and_preserves_missing_timestamp() -> None:
    metadata = {"source_backend": "patched_rust_sqlite", "schema_version": "fixture"}
    record = common_record({"mint": "mint-a", "creation_timestamp": 1704067200000}, metadata)
    assert record["token_mint"] == "mint-a"
    assert record["creation_hour_utc"] == 0
    assert record["creation_hour_europe_paris"] == 1
    assert record["day_of_week"] == 0
    assert record["has_creation_timestamp"] is True
    assert record["is_legacy_source"] is True
    assert record["feature_completeness_ratio"] == 1.0
    missing = common_record({"mint": "mint-b"}, metadata)
    assert missing["creation_hour_utc"] is None
    assert missing["creation_hour_europe_paris"] is None
    assert missing["has_creation_timestamp"] is False
    assert missing["feature_completeness_ratio"] == 0.0


def test_rfc_record_materializes_observed_features_without_labels() -> None:
    row = {
        "mint": "mint-a", "creation_timestamp": 1704067200000,
        "events_10s": 2, "events_30s": 6, "events_1m": 12,
        "events_5m": 24, "events_15m": None, "events_60m": None,
        "buy_count": 3, "sell_count": 1, "transfer_count": 2,
        "event_count": 6, "unique_wallet_count": 3,
        "observation_duration_ms": 30000,
        "left_censored": False, "right_censored": True,
        "lifecycle_status": "CENSORED",
        "migration": 1,
    }
    record = rfc_record(row, {"source_backend": "fixture"})
    assert record["events_60s"] == 12
    assert record["event_velocity_0_10s"] == 0.2
    assert record["event_growth_10s_to_30s"] == 2.0
    assert record["total_transaction_count"] == 6
    assert record["buy_share"] == 0.5
    assert record["buy_sell_ratio"] == 3.0
    assert record["activity_duration_seconds"] == 30.0
    assert record["events_per_active_second"] == 0.2
    assert record["events_15m"] is None
    assert record["has_5m_window"] is True
    assert record["left_censored"] is False
    assert record["right_censored"] is True
    assert "migration" not in record

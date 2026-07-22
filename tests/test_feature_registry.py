from __future__ import annotations

import pytest

from atlaspump.features.engine import safe_ratio, stable_sample_mints
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

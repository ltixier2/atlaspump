from datetime import datetime, timedelta, timezone

import pytest

from atlaspump.shadow_inference import (
    FEATURES,
    TokenWindow,
    feature_contract,
    prediction_id,
    validate_features,
)


def test_window_rejects_future_and_is_immutable():
    created = datetime(2026, 4, 24, tzinfo=timezone.utc)
    window = TokenWindow("mint", created)
    assert window.add({"event_time": created + timedelta(seconds=5)}, created)
    assert not window.add({"event_time": created + timedelta(seconds=11)}, created)
    features = window.finalize(created + timedelta(seconds=10))
    assert features["events_10s"] == 1
    assert not window.add({"event_time": created + timedelta(seconds=6)}, created)
    validate_features(features)


def test_contract_and_id_are_stable():
    created = datetime(2026, 4, 24, tzinfo=timezone.utc)
    assert [item["name"] for item in feature_contract()["features"]] == FEATURES
    assert prediction_id("mint", created) == prediction_id("mint", created)
    with pytest.raises(ValueError):
        validate_features({"wrong": 1})

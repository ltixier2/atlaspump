from __future__ import annotations

import pytest

from atlaspump.nine_day_delta import concentration, require_dates, safe_ratio, stability_class


def test_delta_safe_ratio_preserves_unknown_and_zero_denominators() -> None:
    assert safe_ratio(1, 0) is None
    assert safe_ratio(None, 4) is None
    assert safe_ratio(3, 6) == 0.5
    assert safe_ratio(float("nan"), 4) is None
    assert safe_ratio(4, float("inf")) is None
    assert safe_ratio(4, float("-inf")) is None


def test_delta_manifest_dates_must_be_unique_complete_and_ordered() -> None:
    expected = ["2026-04-18", "2026-04-19"]
    require_dates(expected, expected)
    with pytest.raises(ValueError, match="duplicate"):
        require_dates(["2026-04-18", "2026-04-18"], expected)
    with pytest.raises(ValueError, match="coverage"):
        require_dates(list(reversed(expected)), expected)
    with pytest.raises(ValueError, match="coverage"):
        require_dates([], expected)


def test_stability_class_thresholds_are_descriptive() -> None:
    assert stability_class(100, 104) == "STABLE"
    assert stability_class(100, 120) == "STABLE_WITH_SMALL_CHANGE"
    assert stability_class(100, 130) == "MATERIALLY_CHANGED"
    assert stability_class(0, 10) == "NOT_COMPARABLE"
    assert stability_class(float("nan"), 10) == "NOT_COMPARABLE"
    assert stability_class(10, float("inf")) == "NOT_COMPARABLE"


def test_concentration_keeps_zero_event_tokens() -> None:
    result = concentration([0, 0, 1, 9])
    assert result["zero_event_tokens"] == 2
    assert result["top_1_share"] == 0.9
    assert result["gini"] is not None
    assert concentration([])["top_1_share"] is None


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_concentration_rejects_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        concentration([1, value])

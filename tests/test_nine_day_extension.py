from __future__ import annotations

import pytest

from atlaspump.nine_day_extension import safe_ratio, validate_iso_dates


def test_safe_ratio_preserves_zero_denominator() -> None:
    assert safe_ratio(3, 0) is None
    assert safe_ratio(3, 6) == 0.5


def test_nine_day_dates_must_be_unique_and_complete() -> None:
    expected = ["2026-04-18", "2026-04-19"]
    validate_iso_dates(expected, expected)
    with pytest.raises(ValueError, match="duplicate"):
        validate_iso_dates(["2026-04-18", "2026-04-18"], expected)
    with pytest.raises(ValueError, match="coverage"):
        validate_iso_dates(["2026-04-18"], expected)

from __future__ import annotations

from atlaspump.eda_missing_aggregates import (
    concentration,
    lorenz_points,
    numeric_summary,
    paris_bucket,
    rows_to_table,
    safe_ratio,
)


def test_numeric_summary_preserves_missing_values() -> None:
    summary = numeric_summary([None, 0, 10], 3)
    assert summary["n_available"] == 2
    assert summary["missing_rate"] == 1 / 3
    assert summary["mean"] == 5


def test_safe_ratio_rejects_zero_denominator() -> None:
    assert safe_ratio(4, 0) is None
    assert safe_ratio(4, 2) == 2


def test_paris_conversion_uses_summer_offset() -> None:
    utc_date, utc_hour, local_date, local_hour = paris_bucket(1776553200000)
    assert (utc_date, utc_hour) == ("2026-04-18", 23)
    assert (local_date, local_hour) == ("2026-04-19", 1)


def test_concentration_uses_ceil_and_keeps_zeros() -> None:
    result = concentration([0, 0, 1, 9])
    assert result["zero_event_tokens"] == 2
    assert result["top_1_count"] == 1
    assert result["top_1_share"] == 0.9
    assert result["gini"] is not None


def test_lorenz_is_reproducible_and_bounded() -> None:
    points = lorenz_points([0, 1, 9], point_count=5)
    assert points[0] == {"cumulative_token_fraction": 0.0, "cumulative_event_fraction": 0.0}
    assert points[-1] == {"cumulative_token_fraction": 1.0, "cumulative_event_fraction": 1.0}


def test_rows_to_table_keeps_fields_only_present_on_later_rows() -> None:
    table = rows_to_table(
        [{"metric": "x", "mean": 1.0}, {"metric": "comparison", "mean_ratio": 2.0}]
    )
    assert table.schema.names == ["mean", "mean_ratio", "metric"]
    assert table["mean_ratio"].to_pylist() == [None, 2.0]

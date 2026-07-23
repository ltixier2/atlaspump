"""Small deterministic helpers for the read-only nine-day EDA delta."""

from __future__ import annotations

import math
from collections.abc import Iterable


def safe_ratio(numerator: float | int | None, denominator: float | int | None) -> float | None:
    """Return ``None`` rather than divide by an unknown, zero, or non-finite value."""
    if numerator is None or denominator is None:
        return None
    numerator_value = float(numerator)
    denominator_value = float(denominator)
    if not math.isfinite(numerator_value) or not math.isfinite(denominator_value):
        return None
    if denominator_value == 0:
        return None
    return numerator_value / denominator_value


def require_dates(dates: Iterable[str], expected: Iterable[str]) -> None:
    """Reject missing, duplicated, or reordered logical dates."""
    actual = list(dates)
    wanted = list(expected)
    if len(actual) != len(set(actual)):
        raise ValueError("duplicate date in delta EDA manifest")
    if actual != wanted:
        raise ValueError(f"unexpected delta EDA date coverage: {actual}")


def stability_class(
    baseline: float | int | None,
    updated: float | int | None,
    *,
    small_change: float = 0.05,
    material_change: float = 0.25,
) -> str:
    """Classify a descriptive change without implying causality."""
    if baseline is None or updated is None:
        return "NOT_COMPARABLE"
    ratio = safe_ratio(abs(float(updated) - float(baseline)), abs(float(baseline)))
    if ratio is None:
        return "NOT_COMPARABLE"
    if ratio <= small_change:
        return "STABLE"
    if ratio <= material_change:
        return "STABLE_WITH_SMALL_CHANGE"
    return "MATERIALLY_CHANGED"


def concentration(values: Iterable[int | float]) -> dict[str, float | int | None]:
    """Calculate deterministic top shares and Gini, retaining zero-event tokens."""
    ordered = sorted(float(value) for value in values)
    if any(not math.isfinite(value) for value in ordered):
        raise ValueError("concentration requires finite values")
    count = len(ordered)
    total = sum(ordered)
    result: dict[str, float | int | None] = {
        "token_count": count,
        "total_events": int(total),
        "zero_event_tokens": sum(value == 0 for value in ordered),
        "top_rounding": "ceil",
        "top_1_share": None,
        "top_5_share": None,
        "top_10_share": None,
        "gini": None,
    }
    if not count or not total:
        return result
    for label, share in (("top_1_share", 0.01), ("top_5_share", 0.05), ("top_10_share", 0.10)):
        take = math.ceil(count * share)
        result[label] = sum(ordered[-take:]) / total
    weighted_sum = sum(
        (2 * index - count - 1) * value for index, value in enumerate(ordered, start=1)
    )
    result["gini"] = weighted_sum / (count * total)
    return result

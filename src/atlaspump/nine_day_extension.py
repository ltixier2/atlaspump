"""Small, deterministic validation helpers for the logical nine-day extension."""

from __future__ import annotations

from collections.abc import Iterable


def safe_ratio(numerator: int | float, denominator: int | float) -> float | None:
    """Return a ratio, preserving an undefined denominator as ``None``."""
    return None if denominator == 0 else float(numerator) / float(denominator)


def validate_iso_dates(dates: Iterable[str], expected: Iterable[str]) -> None:
    """Reject missing or duplicated dates in a logical manifest."""
    actual = list(dates)
    wanted = list(expected)
    if len(actual) != len(set(actual)):
        raise ValueError("duplicate date in nine-day manifest")
    if actual != wanted:
        raise ValueError(f"unexpected date coverage: {actual}")

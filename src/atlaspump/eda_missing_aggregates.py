"""Bounded, reproducible aggregates for the final EDA v2 run.

The helpers deliberately operate on one daily Parquet input at a time.  They
never turn absent RFC fields into zero and preserve the declared analytical
scope on every output row.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa

LOW_SAMPLE_THRESHOLD = 30
PERCENTILES = (0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99)


def rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build an Arrow table from heterogeneous summary rows without dropping fields."""
    if not rows:
        return pa.table({})
    names = sorted({name for row in rows for name in row})
    normalized = [{name: row.get(name) for name in names} for row in rows]
    return pa.Table.from_pylist(normalized)


def percentile(values: list[float], q: float) -> float | None:
    """Return a deterministic linear percentile, or None for no values."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def numeric_summary(
    values: list[float | int | None], n_total: int
) -> dict[str, float | int | None]:
    """Summarize a numeric field without treating None as zero."""
    valid = [float(value) for value in values if value is not None]
    n_available = len(valid)
    result: dict[str, float | int | None] = {
        "n_total": n_total,
        "n_available": n_available,
        "missing_rate": (n_total - n_available) / n_total if n_total else None,
        "mean": sum(valid) / n_available if n_available else None,
        "median": percentile(valid, 0.50),
        "q10": percentile(valid, 0.10),
        "q25": percentile(valid, 0.25),
        "q75": percentile(valid, 0.75),
        "q90": percentile(valid, 0.90),
        "q95": percentile(valid, 0.95),
        "q99": percentile(valid, 0.99),
        "min": min(valid) if valid else None,
        "max": max(valid) if valid else None,
    }
    return result


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    """Return a ratio only when its denominator is nonzero and known."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def paris_bucket(timestamp_ms: int) -> tuple[str, int, str, int]:
    """Map a UTC epoch-millisecond timestamp to the explicit Paris bucket."""
    utc_value = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    local_value = utc_value.astimezone(ZoneInfo("Europe/Paris"))
    return (
        utc_value.date().isoformat(),
        utc_value.hour,
        local_value.date().isoformat(),
        local_value.hour,
    )


def concentration(values: list[int | float]) -> dict[str, float | int | None]:
    """Compute top-share and Gini metrics including zero-event tokens."""
    ordered = sorted(float(value) for value in values)
    count = len(ordered)
    total = sum(ordered)
    result: dict[str, float | int | None] = {
        "token_count": count,
        "total_events": int(total),
        "zero_event_tokens": sum(value == 0 for value in ordered),
        "top_1_count": math.ceil(count * 0.01),
        "top_5_count": math.ceil(count * 0.05),
        "top_10_count": math.ceil(count * 0.10),
        "top_1_share": None,
        "top_5_share": None,
        "top_10_share": None,
        "gini": None,
    }
    if not total or not count:
        return result
    for label, fraction in (("top_1_share", 0.01), ("top_5_share", 0.05), ("top_10_share", 0.10)):
        take = math.ceil(count * fraction)
        result[label] = sum(ordered[-take:]) / total
    weighted = sum((2 * index - count - 1) * value for index, value in enumerate(ordered, start=1))
    result["gini"] = weighted / (count * total)
    return result


def lorenz_points(values: list[int | float], point_count: int = 101) -> list[dict[str, float]]:
    """Return fixed cumulative Lorenz points; zeros remain in the population."""
    ordered = sorted(float(value) for value in values)
    count = len(ordered)
    total = sum(ordered)
    points: list[dict[str, float]] = []
    for step in range(point_count):
        fraction = step / (point_count - 1)
        take = min(count, math.floor(count * fraction))
        event_share = sum(ordered[:take]) / total if total else 0.0
        points.append(
            {"cumulative_token_fraction": fraction, "cumulative_event_fraction": event_share}
        )
    return points

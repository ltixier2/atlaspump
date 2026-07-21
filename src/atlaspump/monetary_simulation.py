"""Pure, read-only monetary-simulation helpers for RFC-014 offline artifacts."""
from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timedelta

COST_SCENARIOS = {"GROSS": 0.0, "LOW_COST": 0.01, "REALISTIC": 0.03, "STRESSED": 0.08}


def valid_price(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def first_price_at_or_after(
    points: Iterable[tuple[datetime, float]], target: datetime, tolerance_seconds: int = 10
) -> tuple[datetime, float] | None:
    latest = target + timedelta(seconds=tolerance_seconds)
    for timestamp, price in points:
        if timestamp < target:
            continue
        if timestamp > latest:
            return None
        if valid_price(price):
            return timestamp, float(price)
    return None


def virtual_quantity(stake_eur: float, entry_price: float) -> float | None:
    return stake_eur / entry_price if stake_eur > 0 and valid_price(entry_price) else None


def valuation(stake_eur: float, entry_price: float, exit_price: float, cost_rate: float) -> dict[str, float]:
    if not (stake_eur > 0 and valid_price(entry_price) and valid_price(exit_price)):
        raise ValueError("invalid virtual valuation input")
    gross_exit = stake_eur * exit_price / entry_price
    net_exit = gross_exit * (1.0 - cost_rate)
    gross_pnl = gross_exit - stake_eur
    net_pnl = net_exit - stake_eur
    return {
        "gross_exit_value_eur": gross_exit,
        "net_exit_value_eur": net_exit,
        "gross_pnl_eur": gross_pnl,
        "net_pnl_eur": net_pnl,
        "gross_return": gross_pnl / stake_eur,
        "net_return": net_pnl / stake_eur,
    }


def top_prediction_ids(rows: list[dict[str, object]], score_name: str, fraction: float) -> set[str]:
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    ranked = sorted(rows, key=lambda row: (-float(row[score_name]), str(row["prediction_id"])))
    count = max(1, math.ceil(len(ranked) * fraction)) if ranked else 0
    return {str(row["prediction_id"]) for row in ranked[:count]}


def consensus_ids(score_sets: list[set[str]], minimum_models: int) -> set[str]:
    if minimum_models < 1 or minimum_models > len(score_sets):
        raise ValueError("invalid consensus size")
    counts: dict[str, int] = {}
    for score_set in score_sets:
        for prediction_id in score_set:
            counts[prediction_id] = counts.get(prediction_id, 0) + 1
    return {prediction_id for prediction_id, count in counts.items() if count >= minimum_models}


def maximum_drawdown(values: list[float]) -> tuple[float, float]:
    peak = 0.0
    cumulative = 0.0
    worst = 0.0
    for value in values:
        cumulative += value
        peak = max(peak, cumulative)
        worst = min(worst, cumulative - peak)
    return worst, (worst / peak if peak else 0.0)

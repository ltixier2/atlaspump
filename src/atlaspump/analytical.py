"""Pure analytical ranking selection; intentionally contains no finance fields."""

from __future__ import annotations

import math
from statistics import median
from typing import Any

FINANCIAL_FIELDS = {"price", "pnl", "capital", "fees", "slippage", "drawdown", "return"}


def select(
    rows: list[dict[str, Any]], score_field: str, mode: str, parameter: float
) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (-float(row[score_field]), str(row["token_mint"])))
    if mode == "TOP_K_DAILY":
        chosen = ordered[: int(parameter)]
    elif mode == "TOP_PERCENTILE_DAILY":
        chosen = ordered[: math.ceil(len(ordered) * parameter / 100)]
    elif mode == "SCORE_THRESHOLD":
        chosen = [row for row in ordered if float(row[score_field]) >= parameter]
    else:
        raise ValueError(f"unknown analytical selection mode: {mode}")
    selected = {row["token_mint"] for row in chosen}
    return [
        {
            **row,
            "score": float(row[score_field]),
            "rank": index + 1,
            "selected": row["token_mint"] in selected,
        }
        for index, row in enumerate(ordered)
    ]


def metrics(rows: list[dict[str, Any]]) -> dict[str, float | int | None]:
    available = len(rows)
    positives = sum(int(row["label_value"]) for row in rows)
    picked = [row for row in rows if row["selected"]]
    ps = sum(int(row["label_value"]) for row in picked)
    ns = len(picked) - ps
    precision = ps / len(picked) if picked else None
    recall = ps / positives if positives else None
    prevalence = positives / available if available else None
    return {
        "tokens_available": available,
        "tokens_selected": len(picked),
        "selection_rate": len(picked) / available if available else 0.0,
        "positives_available": positives,
        "positives_selected": ps,
        "negatives_selected": ns,
        "precision": precision,
        "recall": recall,
        "F1": 2 * precision * recall / (precision + recall) if precision and recall else 0.0,
        "lift": precision / prevalence if precision is not None and prevalence else None,
        "false_positive_count": ns,
        "false_negative_count": positives - ps,
        "average_selected_score": sum(r["score"] for r in picked) / len(picked) if picked else None,
        "median_selected_score": median([r["score"] for r in picked]) if picked else None,
        "minimum_selected_score": min((r["score"] for r in picked), default=None),
        "maximum_selected_score": max((r["score"] for r in picked), default=None),
        "prevalence": prevalence,
    }


def assert_analytical_schema(columns: set[str]) -> None:
    forbidden = {column for column in columns if column.lower() in FINANCIAL_FIELDS}
    if forbidden:
        raise ValueError(f"financial fields forbidden in analytical output: {sorted(forbidden)}")

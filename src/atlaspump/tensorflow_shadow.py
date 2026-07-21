"""Strict local inference helpers for the TensorFlow shadow path."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, cast

import numpy as np


def load_preprocessing(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text()))


def prepare_rows(rows: dict[str, Any], preprocessing: dict[str, Any]) -> np.ndarray:
    features = preprocessing["features"]
    missing = set(features) - set(rows)
    extra = set(rows) - set(features)
    if missing or extra:
        raise ValueError(f"feature mismatch missing={sorted(missing)} extra={sorted(extra)}")
    values = np.array([[rows[f] for f in features]], dtype=np.float32)
    normalized = (
        np.where(np.isfinite(values), values, np.array(preprocessing["medians"], dtype=np.float32))
        - np.array(preprocessing["means"], dtype=np.float32)
    ) / np.array(preprocessing["scales"], dtype=np.float32)
    return cast(np.ndarray[Any, Any], np.asarray(normalized, dtype=np.float32))


def prepare_batch(rows: list[dict[str, Any]], preprocessing: dict[str, Any]) -> np.ndarray:
    """Validate and normalize a small homogeneous batch in feature order."""

    if not rows:
        raise ValueError("cannot prepare an empty batch")
    return cast(
        np.ndarray[Any, Any],
        np.asarray(np.vstack([prepare_rows(row, preprocessing) for row in rows]), dtype=np.float32),
    )


def shadow_selected(score: float, policy: dict[str, Any]) -> bool:
    """Apply a frozen policy to an individual live score.

    A top-K/fraction rule has no stable meaning for one isolated live row.  Its
    validation cut-off is stored as ``min_selected_score`` and is the explicit
    threshold used by shadow inference.
    """

    if policy["type"] == "threshold":
        return score >= float(policy["value"])
    cutoff = policy.get("min_selected_score")
    if cutoff is None:
        raise ValueError("ranking policy lacks its frozen validation score cutoff")
    return score >= float(cutoff)


def predict_rows(
    model: Any, rows: dict[str, Any], preprocessing: dict[str, Any], policy: dict[str, Any]
) -> dict[str, Any]:
    start = time.perf_counter()
    score = float(model.predict(prepare_rows(rows, preprocessing), verbose=0)[0, 0])
    return {
        "score": score,
        "selected": shadow_selected(score, policy),
        "latency_ms": (time.perf_counter() - start) * 1000,
    }


def predict_batch(
    model: Any,
    rows: list[dict[str, Any]],
    preprocessing: dict[str, Any],
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    """Score a local shadow batch once, retaining strict feature validation."""

    started = time.perf_counter()
    scores = model.predict(prepare_batch(rows, preprocessing), verbose=0).reshape(-1)
    latency_ms = (time.perf_counter() - started) * 1000
    return [
        {
            "score": float(score),
            "selected": shadow_selected(float(score), policy),
            "latency_ms": latency_ms / len(scores),
        }
        for score in scores
    ]

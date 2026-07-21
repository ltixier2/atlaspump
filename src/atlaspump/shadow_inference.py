"""Read-only, local-only primitives for RFC-014 shadow inference.

This module deliberately has no network, wallet, transaction, signing, or
trading dependencies.  Model workers receive finalized feature rows only.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

FEATURES = [
    "creation_hour_utc",
    "creation_hour_europe_paris",
    "day_of_week",
    "is_weekend",
    "has_creation_timestamp",
    "is_legacy_source",
    "feature_completeness_ratio",
    "events_10s",
    "event_velocity_0_10s",
    "has_10s_window",
]
CONTRACT_VERSION = "shadow_features_10s_v1"
FORBIDDEN_COLUMNS = {
    "date",
    "token_mint",
    "label_value",
    "meta_split_role",
    "dataset_version",
    "dataset",
    "feature_horizon",
}


def prediction_id(token_mint: str, creation_time: datetime) -> str:
    material = f"{token_mint}|{creation_time.isoformat()}|{CONTRACT_VERSION}|rfc010e-rfc012"
    return hashlib.sha256(material.encode()).hexdigest()


def feature_contract() -> dict[str, Any]:
    definitions = {
        "creation_hour_utc": ("int", "UTC hour at CREATE", "hour", "creation event", 0),
        "creation_hour_europe_paris": (
            "int",
            "Europe/Paris hour at CREATE",
            "hour",
            "creation event",
            0,
        ),
        "day_of_week": ("int", "UTC weekday Monday=0", "day", "creation event", 0),
        "is_weekend": (
            "bool",
            "UTC weekday is Saturday/Sunday",
            "boolean",
            "creation event",
            False,
        ),
        "has_creation_timestamp": (
            "bool",
            "creation timestamp available",
            "boolean",
            "creation event",
            False,
        ),
        "is_legacy_source": (
            "bool",
            "historical source marker",
            "boolean",
            "source metadata",
            False,
        ),
        "feature_completeness_ratio": (
            "float",
            "fraction of required live inputs available",
            "ratio",
            "events through T+10s",
            0.0,
        ),
        "events_10s": (
            "int",
            "count of accepted events in [T,T+10s]",
            "events",
            "normalized events",
            0,
        ),
        "event_velocity_0_10s": ("float", "events_10s / 10", "events/s", "normalized events", 0.0),
        "has_10s_window": ("bool", "window completed", "boolean", "clock", False),
    }
    return {
        "version": CONTRACT_VERSION,
        "window_seconds": 10,
        "no_future_information": True,
        "features": [
            {
                "name": n,
                "type": t,
                "definition": d,
                "unit": u,
                "source": s,
                "default": default,
                "missing_rule": "use default and set missing indicator",
                "max_event_time": "creation_time + 10 seconds",
            }
            for n, (t, d, u, s, default) in definitions.items()
        ],
    }


@dataclass
class TokenWindow:
    token_mint: str
    creation_time: datetime
    events: list[dict[str, Any]] = field(default_factory=list)
    finalized: bool = False

    def add(self, event: dict[str, Any], ingestion_time: datetime) -> bool:
        if self.finalized:
            return False
        event_time = event["event_time"]
        if not self.creation_time <= event_time <= self.creation_time + timedelta(seconds=10):
            return False
        self.events.append(event | {"ingestion_time": ingestion_time})
        return True

    def finalize(self, inference_time: datetime) -> dict[str, Any]:
        if inference_time < self.creation_time + timedelta(seconds=10):
            raise ValueError("cannot finalize before T+10s")
        self.finalized = True
        utc = self.creation_time.astimezone(timezone.utc)
        paris = self.creation_time.astimezone(__import__("zoneinfo").ZoneInfo("Europe/Paris"))
        count = len(self.events)
        values = {
            "creation_hour_utc": utc.hour,
            "creation_hour_europe_paris": paris.hour,
            "day_of_week": utc.weekday(),
            "is_weekend": utc.weekday() >= 5,
            "has_creation_timestamp": True,
            "is_legacy_source": False,
            "feature_completeness_ratio": 1.0,
            "events_10s": count,
            "event_velocity_0_10s": count / 10.0,
            "has_10s_window": True,
        }
        return values


def validate_features(values: dict[str, Any]) -> None:
    if set(values) != set(FEATURES):
        raise ValueError(
            f"feature contract mismatch missing={sorted(set(FEATURES) - set(values))} extra={sorted(set(values) - set(FEATURES))}"
        )


def frozen_selected(score: float, policy: dict[str, Any]) -> bool:
    return score >= float(
        policy["value"] if policy["type"] == "threshold" else policy["min_selected_score"]
    )


def consensus(record: dict[str, Any]) -> dict[str, bool]:
    cb = bool(record["catboost_selected_policy"])
    xgb = bool(record["xgboost_selected_policy"])
    mlp = bool(record["tensorflow_selected_policy"])
    return {"consensus_catboost_mlp": cb and mlp, "consensus_all": cb and xgb and mlp}


def json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, default=str)

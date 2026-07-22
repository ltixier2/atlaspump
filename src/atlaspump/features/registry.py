"""Versioned feature registry and leakage guards for RFC-008.

The registry is deliberately stricter than a dataframe schema: a registered
column is not automatically eligible for a predictive horizon.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass

FEATURE_REGISTRY_VERSION = "rfc008-feature-registry-v1"
HORIZONS = (10, 30, 60, 300, 900, 3600)
SAFE_CLASSES = {"ONLINE_SAFE", "HORIZON_SAFE"}
FORBIDDEN_FEATURE_NAMES = {"migration", "migrated", "migration_timestamp", "time_to_migration_ms"}


@dataclass(frozen=True)
class FeatureSpec:
    feature_name: str
    feature_version: str
    family: str
    description: str
    source_columns: tuple[str, ...]
    source_schema_version: str
    availability_seconds: int | None
    window_start_seconds: int | None
    window_end_seconds: int | None
    dtype: str
    nullable: bool
    missing_policy: str
    censoring_policy: str
    scope: str
    replay_available: bool
    live_feasible: bool
    leakage_risk: str
    status: str
    warnings: tuple[str, ...] = ()


def _spec(
    name: str,
    family: str,
    source: tuple[str, ...],
    availability: int | None,
    dtype: str,
    scope: str,
    risk: str,
    *,
    description: str = "",
    nullable: bool = True,
    status: str = "READY",
    window: tuple[int | None, int | None] = (None, None),
    warnings: tuple[str, ...] = (),
) -> FeatureSpec:
    return FeatureSpec(
        name,
        "v1",
        family,
        description or name,
        source,
        "token-summary-v1",
        availability,
        window[0],
        window[1],
        dtype,
        nullable,
        "NULL_PRESERVED",
        "QUALITY_FLAG_ONLY",
        scope,
        True,
        risk in SAFE_CLASSES,
        risk,
        status,
        warnings,
    )


FEATURES: tuple[FeatureSpec, ...] = (
    _spec(
        "creation_hour_utc", "calendar", ("creation_timestamp",), 0, "int8", "common", "ONLINE_SAFE"
    ),
    _spec(
        "creation_hour_europe_paris",
        "calendar",
        ("creation_timestamp",),
        0,
        "int8",
        "common",
        "ONLINE_SAFE",
    ),
    _spec("day_of_week", "calendar", ("creation_timestamp",), 0, "int8", "common", "ONLINE_SAFE"),
    _spec("is_weekend", "calendar", ("creation_timestamp",), 0, "bool", "common", "ONLINE_SAFE"),
    _spec(
        "has_creation_timestamp",
        "quality",
        ("creation_timestamp",),
        0,
        "bool",
        "common",
        "ONLINE_SAFE",
    ),
    _spec("is_legacy_source", "quality", (), 0, "bool", "common", "ONLINE_SAFE"),
    _spec("feature_completeness_ratio", "quality", (), 0, "float64", "common", "ONLINE_SAFE"),
    _spec(
        "events_10s",
        "early_activity",
        ("events_10s",),
        10,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 10),
    ),
    _spec(
        "events_30s",
        "early_activity",
        ("events_30s",),
        30,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 30),
    ),
    _spec(
        "events_60s",
        "early_activity",
        ("events_1m",),
        60,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 60),
    ),
    _spec(
        "events_5m",
        "early_activity",
        ("events_5m",),
        300,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 300),
    ),
    _spec(
        "events_15m",
        "early_activity",
        ("events_15m",),
        900,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 900),
    ),
    _spec(
        "events_60m",
        "early_activity",
        ("events_60m",),
        3600,
        "int64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 3600),
    ),
    _spec(
        "event_velocity_0_10s",
        "early_activity",
        ("events_10s",),
        10,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 10),
    ),
    _spec(
        "event_velocity_0_30s",
        "early_activity",
        ("events_30s",),
        30,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 30),
    ),
    _spec(
        "event_velocity_0_60s",
        "early_activity",
        ("events_1m",),
        60,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 60),
    ),
    _spec(
        "event_velocity_0_5m",
        "early_activity",
        ("events_5m",),
        300,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(0, 300),
    ),
    _spec(
        "event_growth_10s_to_30s",
        "early_activity",
        ("events_10s", "events_30s"),
        30,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(10, 30),
    ),
    _spec(
        "event_growth_30s_to_60s",
        "early_activity",
        ("events_30s", "events_1m"),
        60,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(30, 60),
    ),
    _spec(
        "event_growth_60s_to_5m",
        "early_activity",
        ("events_1m", "events_5m"),
        300,
        "float64",
        "rfc_enriched",
        "HORIZON_SAFE",
        window=(60, 300),
    ),
    _spec(
        "buy_count",
        "transactional",
        ("buy_count",),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
        warnings=("Final lifecycle aggregate; excluded from predictive horizons.",),
    ),
    _spec(
        "sell_count",
        "transactional",
        ("sell_count",),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
        warnings=("Final lifecycle aggregate; excluded from predictive horizons.",),
    ),
    _spec(
        "transfer_count",
        "transactional",
        ("transfer_count",),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
        warnings=("Final lifecycle aggregate; excluded from predictive horizons.",),
    ),
    _spec(
        "total_transaction_count",
        "transactional",
        ("buy_count", "sell_count", "transfer_count"),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "buy_share",
        "transactional",
        ("buy_count", "sell_count", "transfer_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "sell_share",
        "transactional",
        ("buy_count", "sell_count", "transfer_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "transfer_share",
        "transactional",
        ("buy_count", "sell_count", "transfer_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "buy_sell_ratio",
        "transactional",
        ("buy_count", "sell_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "net_buy_count",
        "transactional",
        ("buy_count", "sell_count"),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "unique_wallets",
        "wallets",
        ("unique_wallet_count",),
        None,
        "int64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
        warnings=("Whole-lifecycle metric; not early safe.",),
    ),
    _spec(
        "wallets_per_event",
        "wallets",
        ("unique_wallet_count", "event_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "events_per_wallet",
        "wallets",
        ("unique_wallet_count", "event_count"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "activity_duration_seconds",
        "duration",
        ("observation_duration_ms",),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "events_per_active_second",
        "duration",
        ("event_count", "observation_duration_ms"),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "time_to_last_observed_event",
        "duration",
        ("observation_duration_ms",),
        None,
        "float64",
        "rfc_enriched",
        "POST_OUTCOME",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "left_censored",
        "lifecycle_quality",
        ("left_censored",),
        None,
        "bool",
        "rfc_enriched",
        "QUALITY_ONLY",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "right_censored",
        "lifecycle_quality",
        ("right_censored",),
        None,
        "bool",
        "rfc_enriched",
        "QUALITY_ONLY",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec(
        "lifecycle_status",
        "lifecycle_quality",
        ("lifecycle_status",),
        None,
        "string",
        "rfc_enriched",
        "QUALITY_ONLY",
        status="READY_WITH_LIMITATIONS",
    ),
    _spec("has_10s_window", "quality", ("events_10s",), 10, "bool", "rfc_enriched", "HORIZON_SAFE"),
    _spec("has_30s_window", "quality", ("events_30s",), 30, "bool", "rfc_enriched", "HORIZON_SAFE"),
    _spec("has_60s_window", "quality", ("events_1m",), 60, "bool", "rfc_enriched", "HORIZON_SAFE"),
    _spec("has_5m_window", "quality", ("events_5m",), 300, "bool", "rfc_enriched", "HORIZON_SAFE"),
    _spec(
        "has_wallet_metrics",
        "quality",
        ("unique_wallet_count",),
        None,
        "bool",
        "rfc_enriched",
        "QUALITY_ONLY",
    ),
    _spec(
        "wallet_dominant",
        "wallets",
        (),
        None,
        "float64",
        "rfc_enriched",
        "BLOCKED",
        status="NOT_COMPUTED",
        warnings=("Absent from token summaries; source-event reread required.",),
    ),
    _spec(
        "wallet_dominant_share",
        "wallets",
        (),
        None,
        "float64",
        "rfc_enriched",
        "BLOCKED",
        status="NOT_COMPUTED",
        warnings=("Absent from token summaries; source-event reread required.",),
    ),
)


def registry_by_name() -> dict[str, FeatureSpec]:
    return {spec.feature_name: spec for spec in FEATURES}


def registry_payload() -> dict[str, object]:
    return {
        "registry_version": FEATURE_REGISTRY_VERSION,
        "features": [asdict(spec) for spec in FEATURES],
    }


def registry_checksum() -> str:
    blob = json.dumps(registry_payload(), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


def horizon_features(horizon_seconds: int, scope: str = "rfc_enriched") -> tuple[str, ...]:
    if horizon_seconds not in HORIZONS:
        raise ValueError(f"unsupported horizon: {horizon_seconds}")
    return tuple(
        spec.feature_name
        for spec in FEATURES
        if spec.scope in {scope, "common"}
        and spec.status in {"READY", "READY_WITH_LIMITATIONS"}
        and spec.leakage_risk in SAFE_CLASSES
        and spec.availability_seconds is not None
        and spec.availability_seconds <= horizon_seconds
    )


def validate_registry(specs: Iterable[FeatureSpec] = FEATURES) -> None:
    seen: set[str] = set()
    for spec in specs:
        if spec.feature_name in seen or spec.feature_name in FORBIDDEN_FEATURE_NAMES:
            raise ValueError(f"invalid or duplicate feature: {spec.feature_name}")
        seen.add(spec.feature_name)
        if spec.leakage_risk in SAFE_CLASSES and spec.availability_seconds is None:
            raise ValueError(f"safe feature lacks availability: {spec.feature_name}")


def validate_horizon_selection(names: Iterable[str], horizon_seconds: int) -> None:
    by_name = registry_by_name()
    for name in names:
        spec = by_name.get(name)
        if spec is None:
            raise ValueError(f"unregistered feature: {name}")
        if spec.leakage_risk not in SAFE_CLASSES or spec.availability_seconds is None:
            raise ValueError(f"post-outcome or quality-only feature selected: {name}")
        if spec.availability_seconds > horizon_seconds:
            raise ValueError(f"future feature selected at horizon: {name}")


validate_registry()

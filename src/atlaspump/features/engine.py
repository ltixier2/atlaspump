"""Pure RFC-008 transformations; no labels are emitted into feature rows."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


def safe_ratio(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return float(numerator) / float(denominator)


def stable_sample_mints(rows: list[tuple[str, bool]], limit: int = 100) -> set[str]:
    """Select a deterministic, label-stratified validation sample only."""
    result: set[str] = set()
    for group in (False, True):
        candidates = [mint for mint, migrated in rows if migrated is group]
        take = min(len(candidates), limit // 2)
        result.update(
            sorted(candidates, key=lambda mint: hashlib.sha256(mint.encode()).digest())[:take]
        )
    return result


def _value(row: dict[str, object], name: str) -> int | float | None:
    value = row.get(name)
    return None if value is None else value  # type: ignore[return-value]


def common_record(row: dict[str, object], metadata: dict[str, str]) -> dict[str, object]:
    timestamp = _value(row, "creation_timestamp")
    utc_time = (
        None if timestamp is None else datetime.fromtimestamp(float(timestamp) / 1000, tz=timezone.utc)
    )
    paris = None if utc_time is None else utc_time.astimezone(ZoneInfo("Europe/Paris"))
    available = int(utc_time is not None)
    return {
        **metadata,
        "token_mint": row["mint"],
        "creation_hour_utc": None if utc_time is None else utc_time.hour,
        "creation_hour_europe_paris": None if paris is None else paris.hour,
        "day_of_week": None if utc_time is None else utc_time.weekday(),
        "is_weekend": None if utc_time is None else utc_time.weekday() >= 5,
        "has_creation_timestamp": utc_time is not None,
        "is_legacy_source": metadata["source_backend"] == "patched_rust_sqlite",
        "feature_completeness_ratio": float(available),
    }


def rfc_record(row: dict[str, object], metadata: dict[str, str]) -> dict[str, object]:
    base = common_record(row, metadata)
    events = {
        "10s": _value(row, "events_10s"),
        "30s": _value(row, "events_30s"),
        "60s": _value(row, "events_1m"),
        "5m": _value(row, "events_5m"),
        "15m": _value(row, "events_15m"),
        "60m": _value(row, "events_60m"),
    }
    buy, sell, transfer = (
        _value(row, "buy_count"),
        _value(row, "sell_count"),
        _value(row, "transfer_count"),
    )
    total = (
        None
        if any(value is None for value in (buy, sell, transfer))
        else int(buy) + int(sell) + int(transfer)
    )
    event_count, wallets, duration_ms = (
        _value(row, "event_count"),
        _value(row, "unique_wallet_count"),
        _value(row, "observation_duration_ms"),
    )
    duration = None if duration_ms is None else float(duration_ms) / 1000
    base.update(
        {
            "events_10s": events["10s"],
            "events_30s": events["30s"],
            "events_60s": events["60s"],
            "events_5m": events["5m"],
            "events_15m": events["15m"],
            "events_60m": events["60m"],
            "event_velocity_0_10s": safe_ratio(events["10s"], 10),
            "event_velocity_0_30s": safe_ratio(events["30s"], 30),
            "event_velocity_0_60s": safe_ratio(events["60s"], 60),
            "event_velocity_0_5m": safe_ratio(events["5m"], 300),
            "event_growth_10s_to_30s": safe_ratio(
                None
                if events["30s"] is None or events["10s"] is None
                else float(events["30s"]) - float(events["10s"]),
                events["10s"],
            ),
            "event_growth_30s_to_60s": safe_ratio(
                None
                if events["60s"] is None or events["30s"] is None
                else float(events["60s"]) - float(events["30s"]),
                events["30s"],
            ),
            "event_growth_60s_to_5m": safe_ratio(
                None
                if events["5m"] is None or events["60s"] is None
                else float(events["5m"]) - float(events["60s"]),
                events["60s"],
            ),
            "buy_count": buy,
            "sell_count": sell,
            "transfer_count": transfer,
            "total_transaction_count": total,
            "buy_share": safe_ratio(buy, total),
            "sell_share": safe_ratio(sell, total),
            "transfer_share": safe_ratio(transfer, total),
            "buy_sell_ratio": safe_ratio(buy, sell),
            "net_buy_count": None if buy is None or sell is None else int(buy) - int(sell),
            "unique_wallets": wallets,
            "wallets_per_event": safe_ratio(wallets, event_count),
            "events_per_wallet": safe_ratio(event_count, wallets),
            "activity_duration_seconds": duration,
            "events_per_active_second": safe_ratio(event_count, duration),
            "time_to_last_observed_event": duration,
            "left_censored": row.get("left_censored"),
            "right_censored": row.get("right_censored"),
            "lifecycle_status": row.get("lifecycle_status"),
            "has_10s_window": events["10s"] is not None,
            "has_30s_window": events["30s"] is not None,
            "has_60s_window": events["60s"] is not None,
            "has_5m_window": events["5m"] is not None,
            "has_wallet_metrics": wallets is not None,
        }
    )
    return base

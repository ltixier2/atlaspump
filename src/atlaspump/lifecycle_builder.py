"""Disk-backed reconstruction of chronological token lifecycles."""

from __future__ import annotations

import math
import os
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

VERSION = "0.1.0"
TOKEN_EVENT_COLUMNS = (
    "mint",
    "event_id",
    "source_event_id",
    "canonical_observation_id",
    "logical_event_id",
    "event_type",
    "protocol_scope",
    "pool",
    "pool_id",
    "signature",
    "slot",
    "block_height",
    "provider_block",
    "blockchain_timestamp",
    "archive_timestamp",
    "wallet",
    "creator",
    "pool_created_by",
    "sol_amount",
    "token_amount",
    "price",
    "market_cap_sol",
    "sol_in_pool",
    "tokens_in_pool",
    "v_sol_in_bonding_curve",
    "v_tokens_in_bonding_curve",
    "priority_fee",
    "sequence_index",
    "time_since_first_event_ms",
    "time_since_previous_event_ms",
    "raw_reference",
)
EVENT_SCHEMA = pa.schema([(name, pa.string()) for name in TOKEN_EVENT_COLUMNS])
for _name in (
    "slot",
    "block_height",
    "blockchain_timestamp",
    "archive_timestamp",
    "sequence_index",
    "time_since_first_event_ms",
    "time_since_previous_event_ms",
):
    EVENT_SCHEMA = EVENT_SCHEMA.set(
        EVENT_SCHEMA.get_field_index(_name), pa.field(_name, pa.int64())
    )

_LIFECYCLE_BOOLEANS = (
    "creation_event_received",
    "pumpfun_activity_observed",
    "migration_event_received",
    "pumpswap_activity_observed",
    "pool_creation_received",
    "liquidity_added",
    "liquidity_removed",
    "migration_explicit",
    "migration_inferred",
    "left_censored",
    "right_censored",
    "lifecycle_complete",
)
_LIFECYCLE_INTS = (
    "creation_block",
    "creation_timestamp",
    "first_observed_timestamp",
    "last_observed_timestamp",
    "observation_duration_ms",
    "event_count",
    "buy_count",
    "sell_count",
    "transfer_count",
    "create_count",
    "migration_count",
    "create_pool_count",
    "add_liquidity_count",
    "remove_liquidity_count",
    "unique_wallet_count",
    "unique_buyer_count",
    "unique_seller_count",
    "time_to_first_buy_ms",
    "time_to_first_sell_ms",
    "time_to_peak_ms",
    "time_to_migration_ms",
    "time_to_pumpswap_activity_ms",
    "time_between_creation_and_first_trade_ms",
    "liquidity_add_events",
    "liquidity_remove_events",
)
_LIFECYCLE_STRINGS = (
    "mint",
    "creator",
    "creation_signature",
    "first_pool",
    "final_pool",
    "lifecycle_status",
    "coverage_status",
    "contract_status",
    "usability_status",
    "censoring_status",
    "lifecycle_version",
)
LIFECYCLE_SCHEMA = pa.schema(
    [(name, pa.bool_()) for name in _LIFECYCLE_BOOLEANS]
    + [(name, pa.int64()) for name in _LIFECYCLE_INTS]
    + [(name, pa.string()) for name in _LIFECYCLE_STRINGS]
    + [
        (name, pa.float64())
        for name in (
            "migration_confidence",
            "buy_volume_sol",
            "sell_volume_sol",
            "total_volume_sol",
            "net_sol_flow",
            "buy_token_volume",
            "sell_token_volume",
            "first_price",
            "last_price",
            "min_price",
            "max_price",
            "first_market_cap_sol",
            "last_market_cap_sol",
            "min_market_cap_sol",
            "max_market_cap_sol",
            "max_return_from_first",
            "drawdown_from_peak_at_end",
            "first_v_sol_in_bonding_curve",
            "last_v_sol_in_bonding_curve",
            "max_v_sol_in_bonding_curve",
            "first_v_tokens_in_bonding_curve",
            "last_v_tokens_in_bonding_curve",
            "min_v_tokens_in_bonding_curve",
            "first_sol_in_pool",
            "last_sol_in_pool",
            "max_sol_in_pool",
            "first_tokens_in_pool",
            "last_tokens_in_pool",
        )
    ]
)
OUTCOME_SCHEMA = pa.schema(
    [(name, pa.string()) for name in ("mint",)]
    + [
        (name, pa.int64())
        for name in ("observation_duration_ms", "time_to_peak_ms", "unique_wallet_count")
    ]
    + [
        (name, pa.bool_())
        for name in (
            "migrated_explicitly",
            "pumpswap_observed",
            "reached_2x",
            "reached_5x",
            "reached_10x",
            "market_cap_above_100_sol",
            "market_cap_above_500_sol",
            "market_cap_above_1000_sol",
            "survived_5_minutes",
            "survived_30_minutes",
            "survived_60_minutes",
        )
    ]
    + [
        (name, pa.float64())
        for name in (
            "max_market_cap_sol",
            "max_return_from_first",
            "total_volume_sol",
            "final_market_cap_sol",
            "final_price",
            "ended_near_peak_ratio",
            "maximum_drawdown",
        )
    ]
)
ANOMALY_SCHEMA = pa.schema(
    [
        ("mint", pa.string()),
        ("anomaly_type", pa.string()),
        ("severity", pa.string()),
        ("event_id", pa.string()),
        ("signature", pa.string()),
        ("timestamp", pa.int64()),
        ("details", pa.string()),
    ]
)
OUTPUT_SCHEMAS = {
    "events": EVENT_SCHEMA,
    "lifecycles": LIFECYCLE_SCHEMA,
    "outcomes": OUTCOME_SCHEMA,
    "anomalies": ANOMALY_SCHEMA,
}


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _first(rows: list[dict[str, Any]], field: str) -> Any:
    return next((row[field] for row in rows if row.get(field) is not None), None)


def _last(rows: list[dict[str, Any]], field: str) -> Any:
    return next((row[field] for row in reversed(rows) if row.get(field) is not None), None)


def _finite_values(rows: list[dict[str, Any]], field: str) -> list[float]:
    return [
        number for row in rows if (number := _number(row.get(field))) is not None and number > 0
    ]


def build_token_lifecycle(
    rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build one lifecycle from rows sorted by canonical source positions."""
    rows.sort(
        key=lambda row: (
            row.get("blockchain_timestamp") or -1,
            row.get("slot") or -1,
            row.get("signature") or "",
            row.get("event_id") or "",
        )
    )
    mint = rows[0]["token_mint"]
    anomalies: list[dict[str, Any]] = []
    seen: set[str] = set()
    previous_time: int | None = None
    events: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        timestamp = row.get("blockchain_timestamp")
        event = {
            "mint": mint,
            **{key: row.get(key) for key in TOKEN_EVENT_COLUMNS if key != "mint"},
        }
        event["sequence_index"] = index
        event["time_since_first_event_ms"] = (
            timestamp - rows[0]["blockchain_timestamp"] if timestamp is not None else None
        )
        event["time_since_previous_event_ms"] = (
            timestamp - previous_time
            if timestamp is not None and previous_time is not None
            else None
        )
        events.append(event)
        if row["event_id"] in seen:
            anomalies.append(
                _anomaly(mint, "DUPLICATE_EVENT", "warning", row, "duplicate event_id")
            )
        seen.add(row["event_id"])
        if timestamp is None:
            anomalies.append(
                _anomaly(mint, "MISSING_TIMESTAMP", "warning", row, "missing blockchain_timestamp")
            )
        elif previous_time is not None and timestamp < previous_time:
            anomalies.append(
                _anomaly(
                    mint, "TIMESTAMP_REGRESSION", "warning", row, "timestamp regressed after sort"
                )
            )
        previous_time = timestamp if timestamp is not None else previous_time
        amount = _number(row.get("sol_amount"))
        if amount is not None and amount < 0:
            anomalies.append(
                _anomaly(mint, "NEGATIVE_AMOUNT", "warning", row, "negative sol_amount")
            )
        for field, label in (
            ("price", "NON_FINITE_PRICE"),
            ("market_cap_sol", "NON_FINITE_MARKET_CAP"),
        ):
            if row.get(field) is not None and _number(row[field]) is None:
                anomalies.append(_anomaly(mint, label, "warning", row, f"invalid {field}"))
    types = Counter(str(row.get("event_type")) for row in rows)
    creators = {row["creator"] for row in rows if row.get("creator")}
    if types["CREATE_TOKEN"] > 1:
        anomalies.append(
            _anomaly(
                mint, "MULTIPLE_CREATION_EVENTS", "warning", rows[0], "multiple CREATE_TOKEN events"
            )
        )
    if len(creators) > 1:
        anomalies.append(
            _anomaly(mint, "MULTIPLE_CREATORS", "warning", rows[0], "multiple creator values")
        )
    first_time, last_time = (
        rows[0].get("blockchain_timestamp"),
        rows[-1].get("blockchain_timestamp"),
    )
    first_pool, final_pool = _first(rows, "pool"), _last(rows, "pool")
    explicit = types["MIGRATE"] > 0
    pumpswap = any(row.get("protocol_scope") == "PUMPSWAP" for row in rows)
    pumpfun = any(
        row.get("protocol_scope") in {"PUMPFUN_BONDING_CURVE", "PUMPFUN"}
        for row in rows
    )
    inferred = not explicit and pumpfun and pumpswap
    confidence = 1.0 if explicit else 0.6 if inferred else 0.0
    status = (
        "ACTIVE_ON_PUMPSWAP"
        if pumpswap
        else "MIGRATED"
        if explicit
        else "ACTIVE_ON_BONDING_CURVE"
        if pumpfun
        else "CREATED_ONLY"
        if types["CREATE_TOKEN"]
        else "INCOMPLETE"
    )
    prices, caps = _finite_values(rows, "price"), _finite_values(rows, "market_cap_sol")
    buy_sol = sum(
        value
        for row in rows
        if row.get("event_type") == "BUY"
        if (value := _number(row.get("sol_amount"))) is not None
    )
    sell_sol = sum(
        value
        for row in rows
        if row.get("event_type") == "SELL"
        if (value := _number(row.get("sol_amount"))) is not None
    )
    lifecycle = {
        "mint": mint,
        "creator": _first(rows, "creator"),
        "creation_signature": _first_of_type(rows, "CREATE_TOKEN", "signature"),
        "creation_block": _first_of_type(rows, "CREATE_TOKEN", "slot"),
        "creation_timestamp": _first_of_type(rows, "CREATE_TOKEN", "blockchain_timestamp"),
        "first_observed_timestamp": first_time,
        "last_observed_timestamp": last_time,
        "observation_duration_ms": last_time - first_time
        if first_time is not None and last_time is not None
        else None,
        "first_pool": first_pool,
        "final_pool": final_pool,
        "creation_event_received": bool(types["CREATE_TOKEN"]),
        "pumpfun_activity_observed": pumpfun,
        "migration_event_received": explicit,
        "pumpswap_activity_observed": pumpswap,
        "pool_creation_received": bool(types["CREATE_POOL"]),
        "liquidity_added": bool(types["ADD_LIQUIDITY"]),
        "liquidity_removed": bool(types["REMOVE_LIQUIDITY"]),
        "lifecycle_status": status,
        "migration_explicit": explicit,
        "migration_inferred": inferred,
        "migration_confidence": confidence,
        "event_count": len(rows),
        "buy_count": types["BUY"],
        "sell_count": types["SELL"],
        "transfer_count": types["TRANSFER"],
        "create_count": types["CREATE_TOKEN"],
        "migration_count": types["MIGRATE"],
        "create_pool_count": types["CREATE_POOL"],
        "add_liquidity_count": types["ADD_LIQUIDITY"],
        "remove_liquidity_count": types["REMOVE_LIQUIDITY"],
        "unique_wallet_count": len({row["wallet"] for row in rows if row.get("wallet")}),
        "unique_buyer_count": len(
            {row["wallet"] for row in rows if row.get("event_type") == "BUY" and row.get("wallet")}
        ),
        "unique_seller_count": len(
            {row["wallet"] for row in rows if row.get("event_type") == "SELL" and row.get("wallet")}
        ),
        "buy_volume_sol": buy_sol or None,
        "sell_volume_sol": sell_sol or None,
        "total_volume_sol": (buy_sol + sell_sol) or None,
        "net_sol_flow": (buy_sol - sell_sol) if buy_sol or sell_sol else None,
        "buy_token_volume": _volume(rows, "BUY", "token_amount"),
        "sell_token_volume": _volume(rows, "SELL", "token_amount"),
        "first_price": prices[0] if prices else None,
        "last_price": prices[-1] if prices else None,
        "min_price": min(prices) if prices else None,
        "max_price": max(prices) if prices else None,
        "first_market_cap_sol": caps[0] if caps else None,
        "last_market_cap_sol": caps[-1] if caps else None,
        "min_market_cap_sol": min(caps) if caps else None,
        "max_market_cap_sol": max(caps) if caps else None,
        "max_return_from_first": max(prices) / prices[0] - 1 if prices else None,
        "drawdown_from_peak_at_end": 1 - prices[-1] / max(prices) if prices else None,
        "time_to_first_buy_ms": _time_to(rows, "BUY", first_time),
        "time_to_first_sell_ms": _time_to(rows, "SELL", first_time),
        "time_to_peak_ms": _peak_time(rows, first_time),
        "time_to_migration_ms": _time_to(rows, "MIGRATE", first_time),
        "time_to_pumpswap_activity_ms": _scope_time(rows, "PUMPSWAP", first_time),
        "time_between_creation_and_first_trade_ms": _first_trade_after_creation(rows),
        "first_v_sol_in_bonding_curve": _first(rows, "v_sol_in_bonding_curve"),
        "last_v_sol_in_bonding_curve": _last(rows, "v_sol_in_bonding_curve"),
        "max_v_sol_in_bonding_curve": _max_value(rows, "v_sol_in_bonding_curve"),
        "first_v_tokens_in_bonding_curve": _first(rows, "v_tokens_in_bonding_curve"),
        "last_v_tokens_in_bonding_curve": _last(rows, "v_tokens_in_bonding_curve"),
        "min_v_tokens_in_bonding_curve": _min_value(rows, "v_tokens_in_bonding_curve"),
        "first_sol_in_pool": _first(rows, "sol_in_pool"),
        "last_sol_in_pool": _last(rows, "sol_in_pool"),
        "max_sol_in_pool": _max_value(rows, "sol_in_pool"),
        "first_tokens_in_pool": _first(rows, "tokens_in_pool"),
        "last_tokens_in_pool": _last(rows, "tokens_in_pool"),
        "liquidity_add_events": types["ADD_LIQUIDITY"],
        "liquidity_remove_events": types["REMOVE_LIQUIDITY"],
        "left_censored": not bool(types["CREATE_TOKEN"]),
        "right_censored": status in {"ACTIVE_ON_BONDING_CURVE", "ACTIVE_ON_PUMPSWAP"},
        "lifecycle_complete": explicit and not anomalies,
        "coverage_status": "COMPLETE",
        "contract_status": "SATISFIED",
        "usability_status": "VALID",
        "censoring_status": (
            "RIGHT" if status in {"ACTIVE_ON_BONDING_CURVE", "ACTIVE_ON_PUMPSWAP"} else "NONE"
        ),
        "lifecycle_version": VERSION,
    }
    return lifecycle, events, anomalies


def _anomaly(
    mint: str, kind: str, severity: str, row: dict[str, Any], details: str
) -> dict[str, Any]:
    return {
        "mint": mint,
        "anomaly_type": kind,
        "severity": severity,
        "event_id": row.get("event_id"),
        "signature": row.get("signature"),
        "timestamp": row.get("blockchain_timestamp"),
        "details": details,
    }


def _first_of_type(rows: list[dict[str, Any]], event_type: str, field: str) -> Any:
    return next((row.get(field) for row in rows if row.get("event_type") == event_type), None)


def _volume(rows: list[dict[str, Any]], event_type: str, field: str) -> float | None:
    values = [_number(row.get(field)) for row in rows if row.get("event_type") == event_type]
    result = sum(value for value in values if value is not None)
    return result or None


def _time_to(rows: list[dict[str, Any]], event_type: str, start: int | None) -> int | None:
    value = _first_of_type(rows, event_type, "blockchain_timestamp")
    return value - start if value is not None and start is not None else None


def _scope_time(rows: list[dict[str, Any]], scope: str, start: int | None) -> int | None:
    value = next(
        (row.get("blockchain_timestamp") for row in rows if row.get("protocol_scope") == scope),
        None,
    )
    return value - start if value is not None and start is not None else None


def _peak_time(rows: list[dict[str, Any]], start: int | None) -> int | None:
    candidates = [
        (number, row.get("blockchain_timestamp"))
        for row in rows
        if (number := _number(row.get("price"))) and number > 0
    ]
    peak_timestamp = max(candidates)[1] if candidates else None
    return peak_timestamp - start if peak_timestamp is not None and start is not None else None


def _first_trade_after_creation(rows: list[dict[str, Any]]) -> int | None:
    creation = _first_of_type(rows, "CREATE_TOKEN", "blockchain_timestamp")
    trade = next(
        (
            row.get("blockchain_timestamp")
            for row in rows
            if row.get("event_type") in {"BUY", "SELL"}
        ),
        None,
    )
    return trade - creation if trade is not None and creation is not None else None


def _max_value(rows: list[dict[str, Any]], field: str) -> float | None:
    values = _finite_values(rows, field)
    return max(values) if values else None


def _min_value(rows: list[dict[str, Any]], field: str) -> float | None:
    values = _finite_values(rows, field)
    return min(values) if values else None


def build_lifecycles(
    input_path: Path, output_dir: Path, force: bool = False, mint: str | None = None
) -> dict[str, Any]:
    """Sort source events externally, then build one mint at a time."""
    started = time.monotonic()
    final_names = (
        "token_events.parquet",
        "token_lifecycles.parquet",
        "token_outcomes_preliminary.parquet",
        "lifecycle_anomalies.parquet",
    )
    if any((output_dir / name).exists() for name in final_names):
        raise FileExistsError(f"refusing to overwrite published lifecycle output: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = Path(os.environ.get("ATLAS_DATA_DIR", output_dir.parent)) / "tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    database_path = temp_dir / f"lifecycles-{uuid.uuid4().hex}.duckdb"
    connection = duckdb.connect(str(database_path))
    # External sorting is intentional: a full day must not reserve host memory.
    connection.execute("SET memory_limit = '2GB'")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute(f"SET temp_directory = '{temp_dir.as_posix()}'")
    source_columns = {field.name for field in pq.ParquetFile(input_path).schema_arrow}
    required = {"event_type", "protocol_scope", "blockchain_timestamp", "token_mint"}
    missing = sorted(required - source_columns)
    if missing:
        raise ValueError(f"input missing lifecycle columns: {', '.join(missing)}")

    def source(name: str, fallback: str = "NULL") -> str:
        return name if name in source_columns else fallback

    select = ", ".join(
        (
            "token_mint",
            f"{source('canonical_observation_id', source('event_id_legacy'))} AS event_id",
            f"{source('source_event_id')} AS source_event_id",
            f"{source('canonical_observation_id')} AS canonical_observation_id",
            f"{source('logical_event_id')} AS logical_event_id",
            source("event_type"),
            source("protocol_scope"),
            "NULL AS pool, NULL AS pool_id",
            source("signature"),
            f"{source('slot')} AS slot",
            f"{source('block_height')} AS block_height",
            f"{source('provider_block')} AS provider_block",
            source("blockchain_timestamp"),
            source("archive_timestamp"),
            source("wallet"),
            "NULL AS creator, NULL AS pool_created_by",
            source("sol_amount"),
            source("token_amount"),
            "NULL AS price, NULL AS market_cap_sol",
            "NULL AS sol_in_pool, NULL AS tokens_in_pool",
            "NULL AS v_sol_in_bonding_curve, NULL AS v_tokens_in_bonding_curve",
            "NULL AS priority_fee",
            source("raw_reference"),
        )
    )
    where = "token_mint IS NOT NULL"
    params: list[Any] = []
    if mint:
        where += " AND token_mint = ?"
        params.append(mint)
    query = (
        f"SELECT {select} FROM read_parquet(?) WHERE {where} "
        "ORDER BY token_mint, blockchain_timestamp, slot, signature, event_id"
    )
    reader = connection.execute(query, [str(input_path), *params]).to_arrow_reader(
        batch_size=10_000
    )
    writers: dict[str, pq.ParquetWriter] = {}
    paths = {
        "events": output_dir / "token_events.parquet",
        "lifecycles": output_dir / "token_lifecycles.parquet",
        "outcomes": output_dir / "token_outcomes_preliminary.parquet",
        "anomalies": output_dir / "lifecycle_anomalies.parquet",
    }
    partial_paths = {label: path.with_suffix(".parquet.partial") for label, path in paths.items()}
    current: list[dict[str, Any]] = []
    current_mint: str | None = None
    lifecycle_count = 0
    anomaly_count = 0
    pending: dict[str, list[dict[str, Any]]] = {label: [] for label in OUTPUT_SCHEMAS}

    def write_batch(label: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        schema = OUTPUT_SCHEMAS[label]
        prepared: list[dict[str, Any]] = []
        for row in rows:
            converted: dict[str, Any] = {}
            for field in schema:
                value = row.get(field.name)
                if value is None:
                    converted[field.name] = None
                elif pa.types.is_string(field.type):
                    converted[field.name] = str(value)
                elif pa.types.is_floating(field.type):
                    converted[field.name] = _number(value)
                elif pa.types.is_integer(field.type):
                    converted[field.name] = int(value)
                else:
                    converted[field.name] = bool(value)
            prepared.append(converted)
        table = pa.Table.from_pylist(prepared, schema=schema)
        if label not in writers:
            partial_paths[label].unlink(missing_ok=True)
            writers[label] = pq.ParquetWriter(partial_paths[label], schema, compression="zstd")
        writers[label].write_table(table)

    def write(label: str, rows: list[dict[str, Any]]) -> None:
        pending[label].extend(rows)
        while len(pending[label]) >= 10_000:
            write_batch(label, pending[label][:10_000])
            del pending[label][:10_000]

    def flush() -> None:
        nonlocal lifecycle_count, anomaly_count, current
        if not current:
            return
        lifecycle, token_events, anomalies = build_token_lifecycle(current)
        outcome = {
            key: lifecycle.get(key)
            for key in (
                "mint",
                "observation_duration_ms",
                "max_market_cap_sol",
                "max_return_from_first",
                "time_to_peak_ms",
                "total_volume_sol",
                "unique_wallet_count",
            )
        }
        outcome.update(
            {
                "migrated_explicitly": lifecycle["migration_explicit"],
                "pumpswap_observed": lifecycle["pumpswap_activity_observed"],
                "final_market_cap_sol": lifecycle["last_market_cap_sol"],
                "final_price": lifecycle["last_price"],
                "ended_near_peak_ratio": (
                    lifecycle["last_price"] / lifecycle["max_price"]
                    if lifecycle.get("last_price") and lifecycle.get("max_price")
                    else None
                ),
                "maximum_drawdown": lifecycle["drawdown_from_peak_at_end"],
            }
        )
        maximum = lifecycle.get("max_return_from_first")
        market_cap = lifecycle.get("max_market_cap_sol")
        duration = lifecycle.get("observation_duration_ms")
        outcome.update(
            {
                f"reached_{multiple}x": maximum >= multiple - 1 if maximum is not None else None
                for multiple in (2, 5, 10)
            }
        )
        outcome.update(
            {
                f"market_cap_above_{threshold}_sol": market_cap >= threshold
                if market_cap is not None
                else None
                for threshold in (100, 500, 1000)
            }
        )
        outcome.update(
            {
                f"survived_{minutes}_minutes": duration >= minutes * 60_000
                if duration is not None
                else None
                for minutes in (5, 30, 60)
            }
        )
        write("events", token_events)
        write("lifecycles", [lifecycle])
        write("outcomes", [outcome])
        write("anomalies", anomalies)
        lifecycle_count += 1
        anomaly_count += len(anomalies)
        current = []

    for batch in reader:
        for row in batch.to_pylist():
            row["token_mint"] = row.pop("token_mint")
            if current_mint is not None and row["token_mint"] != current_mint:
                flush()
            current_mint = row["token_mint"]
            current.append(row)
    flush()
    for label, rows in pending.items():
        write_batch(label, rows)
    for label, schema in OUTPUT_SCHEMAS.items():
        if label not in writers:
            partial_paths[label].unlink(missing_ok=True)
            writer = pq.ParquetWriter(partial_paths[label], schema, compression="zstd")
            writer.write_table(pa.Table.from_pylist([], schema=schema))
            writer.close()
    for writer in writers.values():
        writer.close()
    for label, final_path in paths.items():
        partial_paths[label].replace(final_path)
    connection.close()
    for path in (database_path, database_path.with_suffix(".duckdb.wal")):
        path.unlink(missing_ok=True)
    return {
        "token_count": lifecycle_count,
        "anomaly_count": anomaly_count,
        "duration_seconds": time.monotonic() - started,
        "output_files": [str(path) for path in paths.values() if path.exists()],
    }

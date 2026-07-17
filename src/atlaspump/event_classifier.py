"""Configurable classification and conservative normalization."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation
from typing import Any

EVENT_TYPES = (
    "CREATE",
    "BUY",
    "SELL",
    "TRANSFER",
    "MIGRATION",
    "SWAP",
    "SOL_TRANSFER",
    "SPL_TRANSFER",
    "UNKNOWN",
)


def classify_event(payload: dict[str, Any], config: dict[str, Any]) -> tuple[str, str | None]:
    classification = config["classification"]
    for field in classification["field_candidates"]:
        raw = payload.get(field)
        if raw is None:
            continue
        value = str(raw).lower()
        for category, tokens in classification["rules"].items():
            if any(token.lower() == value for token in tokens):
                return category, str(raw)
        return "UNKNOWN", str(raw)
    return "UNKNOWN", None


def _get_path(payload: dict[str, Any], paths: list[str]) -> Any:
    for path in paths:
        value: Any = payload
        for key in path.split("."):
            if not isinstance(value, dict) or key not in value:
                value = None
                break
            value = value[key]
        if value is not None:
            return value
    return None


def _decimal(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def make_event_id(payload: dict[str, Any], event_type: str, signature: Any) -> str:
    index = payload.get("instruction_index", payload.get("index", ""))
    if signature is not None:
        mint = payload.get("mint", "")
        signer = payload.get("txSigner", "")
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
        return f"{signature}:{index}:{event_type}:{mint}:{signer}:{digest}"
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_event(payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    event_type, raw_event_type = classify_event(payload, config)
    fields = config["normalization"]["fields"]
    values = {name: _get_path(payload, paths) for name, paths in fields.items()}
    signature = values["signature"]
    pool = _string(values["pool"])
    protocol_scope = (
        {"pump": "PUMPFUN_BONDING_CURVE", "pump-amm": "PUMPSWAP"}.get(pool, "OTHER")
        if pool is not None
        else "OTHER"
    )
    return {
        "event_id": make_event_id(payload, event_type, signature),
        "event_type": event_type,
        "source": None,
        "signature": str(signature) if signature is not None else None,
        "block": _integer(values["block"]),
        "blockchain_timestamp": _integer(values["blockchain_timestamp"]),
        "archive_timestamp": _integer(values["archive_timestamp"]),
        "token_mint": _string(values["token_mint"]),
        "wallet": _string(values["wallet"]),
        "creator": None,
        "pool": pool,
        "pool_created_by": _string(values["pool_created_by"]),
        "protocol_scope": protocol_scope,
        "side": event_type if event_type in ("BUY", "SELL") else None,
        "sol_amount": _decimal(values["sol_amount"]),
        "token_amount": _decimal(values["token_amount"]),
        "price": _decimal(values["price"]),
        "market_cap_sol": _decimal(values["market_cap_sol"]),
        "bonding_curve_progress": None,
        "pool_id": _string(values["pool_id"]),
        "sol_in_pool": _decimal(values["sol_in_pool"]),
        "tokens_in_pool": _decimal(values["tokens_in_pool"]),
        "v_sol_in_bonding_curve": _decimal(values["v_sol_in_bonding_curve"]),
        "v_tokens_in_bonding_curve": _decimal(values["v_tokens_in_bonding_curve"]),
        "priority_fee": _decimal(values["priority_fee"]),
        "token_program": _string(values["token_program"]),
        "raw_event_type": raw_event_type,
        "schema_version": "0.1",
        "raw_payload_json": json.dumps(payload, sort_keys=True, default=str),
    }


def _string(value: Any) -> str | None:
    return str(value) if value is not None else None


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) else None

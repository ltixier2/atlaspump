"""Versioned RFC-003 identities and canonical observation contract."""

from __future__ import annotations

import hashlib
import json
from typing import Any

ID_STRATEGY_VERSION = "rfc003-v1"
NORMALIZATION_VERSION = "rfc003-v1"
SCHEMA_VERSION = "rfc003-v1"


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def source_event_id(provider: str, partition: str, cursor: str, raw_payload: str) -> str:
    """Identify one exact provider observation, stably across identical replays."""
    return f"src:{ID_STRATEGY_VERSION}:{digest([provider, partition, cursor, hashlib.sha256(raw_payload.encode()).hexdigest()])}"


def canonical_observation_id(source_id: str, output_index: int = 0) -> str:
    return f"obs:{ID_STRATEGY_VERSION}:{digest([source_id, NORMALIZATION_VERSION, output_index])}"


def logical_event_id(payload: dict[str, Any], event_type: str, mint: str | None) -> str | None:
    """Return a cross-source identity only when the chain position is demonstrable."""
    signature = payload.get("signature")
    instruction = payload.get("instructionIndex", payload.get("instruction_index"))
    event = payload.get("innerInstructionIndex", payload.get("eventIndex", payload.get("event_index")))
    if signature is None or instruction is None:
        return None
    return f"logical:{ID_STRATEGY_VERSION}:{digest([str(signature), int(instruction), event, event_type, mint])}"


def provider_position(payload: dict[str, Any]) -> tuple[int | None, int | None, str | None]:
    """Keep Solana positions separate; a provider's ``block`` remains ambiguous."""
    slot = payload.get("slot")
    height = payload.get("blockHeight", payload.get("block_height"))
    block = payload.get("block")
    return (
        slot if isinstance(slot, int) else None,
        height if isinstance(height, int) else None,
        None if block is None else str(block),
    )

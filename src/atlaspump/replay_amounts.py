"""Conservative helpers for replay amount-contract recovery.

These functions deliberately distinguish proving a serialization from proving
an economic unit.  A decimal-looking provider value is not, by itself, a
validated SOL or token quantity.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def canonical_decimal(value: Any) -> str | None:
    """Return the exact Decimal rendering used by the normalizer, if valid."""
    if value is None:
        return None
    try:
        return str(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def identity_serialization_status(raw_value: Any, normalized: str | None) -> str:
    """Classify raw-to-string evidence without assigning a monetary unit."""
    raw_rendered = canonical_decimal(raw_value)
    if raw_rendered is None or normalized is None:
        return "SOURCE_NOT_FOUND"
    if raw_rendered == normalized:
        return "VERIFIED_EXACT"
    try:
        if Decimal(raw_rendered) == Decimal(normalized):
            return "VERIFIED_WITH_ROUNDING"
    except InvalidOperation:
        pass
    return "CONVERSION_MISMATCH"


def normalized_side(event_type: str | None) -> str:
    """Keep side semantics explicit; unknown events are never coerced."""
    return event_type if event_type in {"BUY", "SELL"} else "NON_TRADE"

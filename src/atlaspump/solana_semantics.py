"""Small, explicit Solana primitives used by RFC011C evidence collection."""

from __future__ import annotations

import base64
from decimal import Decimal
from typing import Any

MINT_ACCOUNT_SIZE = 82
MINT_DECIMALS_OFFSET = 44


def decode_spl_mint_decimals(encoded: str) -> int | None:
    """Decode the decimals byte of an SPL Mint account; reject wrong layouts."""
    try:
        value = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError):
        return None
    if len(value) != MINT_ACCOUNT_SIZE:
        return None
    return value[MINT_DECIMALS_OFFSET]


def ui_amount(raw_amount: str | int, decimals: int) -> Decimal:
    """Scale an atomic token amount only when decimals came from an authority."""
    if isinstance(decimals, bool) or not isinstance(decimals, int) or not 0 <= decimals <= 255:
        raise ValueError("invalid SPL token decimals")
    if isinstance(raw_amount, bool) or not isinstance(raw_amount, (int, str)):
        raise ValueError("invalid atomic token amount")
    if isinstance(raw_amount, str) and not raw_amount.isdecimal():
        raise ValueError("invalid atomic token amount")
    amount = int(raw_amount)
    if amount < 0:
        raise ValueError("invalid atomic token amount")
    return Decimal(amount) / (Decimal(10) ** decimals)


def token_balance_decimals(meta: dict[str, Any], mint: str) -> set[int]:
    """Collect transaction-authoritative decimals for a mint from token balances."""
    found: set[int] = set()
    for field in ("preTokenBalances", "postTokenBalances"):
        for balance in meta.get(field) or []:
            if balance.get("mint") != mint:
                continue
            amount = balance.get("uiTokenAmount") or {}
            decimals = amount.get("decimals")
            if isinstance(decimals, int) and not isinstance(decimals, bool) and 0 <= decimals <= 255:
                found.add(decimals)
    return found

import base64

import pytest

from atlaspump.solana_semantics import decode_spl_mint_decimals, token_balance_decimals, ui_amount


def test_decodes_spl_mint_decimals_at_canonical_offset() -> None:
    value = bytearray(82)
    value[44] = 6
    assert decode_spl_mint_decimals(base64.b64encode(value).decode()) == 6


def test_rejects_non_mint_layout() -> None:
    assert decode_spl_mint_decimals(base64.b64encode(b"short").decode()) is None


def test_scales_atomic_amount_only_with_explicit_decimals() -> None:
    assert ui_amount("10896711778733", 6).as_tuple().exponent == -6
    assert str(ui_amount("10896711778733", 6)) == "10896711.778733"
    assert str(ui_amount("1234567", 6)) == "1.234567"


@pytest.mark.parametrize("raw_amount", ["NaN", "Infinity", "-Infinity", "-1", "1.5", "not-a-number"])
def test_rejects_non_atomic_or_non_finite_amounts(raw_amount: str) -> None:
    with pytest.raises(ValueError, match="invalid atomic token amount"):
        ui_amount(raw_amount, 6)


def test_collects_and_surfaces_conflicting_transaction_decimals() -> None:
    meta = {
        "preTokenBalances": [
            {"mint": "mint", "uiTokenAmount": {"decimals": 0}},
            {"mint": "mint", "uiTokenAmount": {"decimals": 6}},
            {"mint": "mint", "uiTokenAmount": {"decimals": True}},
            {"mint": "mint", "uiTokenAmount": {"decimals": -1}},
            {"mint": "mint", "uiTokenAmount": {"decimals": 1.5}},
        ],
        "postTokenBalances": [
            {"mint": "mint", "uiTokenAmount": {"decimals": 9}},
            {"mint": "mint", "uiTokenAmount": {"decimals": 255}},
            {"mint": "mint", "uiTokenAmount": {"decimals": False}},
            {"mint": "mint", "uiTokenAmount": {"decimals": 256}},
            {"mint": "mint", "uiTokenAmount": {"decimals": "6"}},
        ],
    }
    assert token_balance_decimals(meta, "mint") == {0, 6, 9, 255}

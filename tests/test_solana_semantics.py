import base64

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


def test_collects_and_surfaces_conflicting_transaction_decimals() -> None:
    meta = {
        "preTokenBalances": [{"mint": "mint", "uiTokenAmount": {"decimals": 6}}],
        "postTokenBalances": [{"mint": "mint", "uiTokenAmount": {"decimals": 9}}],
    }
    assert token_balance_decimals(meta, "mint") == {6, 9}

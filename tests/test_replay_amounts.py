from atlaspump.replay_amounts import (
    canonical_decimal,
    identity_serialization_status,
    normalized_side,
)


def test_identity_serialization_proves_rendering_not_unit() -> None:
    assert canonical_decimal(0.329218105) == "0.329218105"
    assert identity_serialization_status(0.329218105, "0.329218105") == "VERIFIED_EXACT"


def test_decimal_equivalent_rendering_is_not_a_unit_conversion() -> None:
    assert identity_serialization_status("1.0", "1.00") == "VERIFIED_WITH_ROUNDING"


def test_invalid_or_missing_raw_value_is_not_verified() -> None:
    assert identity_serialization_status(None, "1") == "SOURCE_NOT_FOUND"
    assert canonical_decimal("not-a-number") is None
    assert canonical_decimal("NaN") is None
    assert canonical_decimal("Infinity") is None
    assert canonical_decimal("-Infinity") is None


def test_side_is_not_inferred_from_unknown_event() -> None:
    assert normalized_side("BUY") == "BUY"
    assert normalized_side("SELL") == "SELL"
    assert normalized_side("SWAP") == "NON_TRADE"

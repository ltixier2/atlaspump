from atlaspump.config import load_settings
from atlaspump.event_classifier import classify_event, make_event_id, normalize_event


def test_classifies_and_normalizes() -> None:
    config = load_settings().values
    row = normalize_event(
        {
            "txType": "buy",
            "signature": "abc",
            "block": 413933112,
            "timestamp": 1776474000212,
            "localTimestamp": 1776474000286,
            "mint": "mint-real",
            "txSigner": "wallet-real",
            "pool": "pump-amm",
            "poolCreatedBy": "pump",
            "solAmount": "1.25",
            "tokenAmount": "100",
            "vSolInBondingCurve": "4.2",
        },
        config,
    )
    assert classify_event({"txType": "BUY"}, config)[0] == "BUY"
    assert row["event_type"] == "BUY"
    assert row["sol_amount"] == "1.25"
    assert row["block"] == 413933112
    assert row["wallet"] == "wallet-real"
    assert row["token_mint"] == "mint-real"
    assert row["protocol_scope"] == "PUMPSWAP"
    assert row["v_sol_in_bonding_curve"] == "4.2"


def test_transfer_and_unmapped_real_tx_types() -> None:
    config = load_settings().values
    assert classify_event({"txType": "transfer"}, config) == ("TRANSFER", "transfer")
    assert classify_event({"txType": "create"}, config) == ("CREATE_TOKEN", "create")
    assert (
        normalize_event({"txType": "sell", "pool": "pump"}, config)["protocol_scope"]
        == "PUMPFUN_BONDING_CURVE"
    )


def test_event_id_is_deterministic_without_signature() -> None:
    payload = {"event_type": "odd", "value": 1}
    assert make_event_id(payload, "UNKNOWN", None) == make_event_id(payload, "UNKNOWN", None)


def test_event_id_distinguishes_payloads_with_same_signature_and_type() -> None:
    first = {"signature": "same", "txSigner": "a", "mint": "m", "txType": "buy"}
    second = {"signature": "same", "txSigner": "b", "mint": "m", "txType": "buy"}
    assert make_event_id(first, "BUY", "same") != make_event_id(second, "BUY", "same")

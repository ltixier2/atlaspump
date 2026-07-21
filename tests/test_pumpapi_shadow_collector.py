from datetime import UTC, datetime, timedelta

from atlaspump.pumpapi_shadow_collector import InspectionBook, PumpApiShadowFilter, stream_event


class Sink:
    def __init__(self):
        self.rows = []

    def emit(self, row):
        self.rows.append(row)


def payload(action="create", mint="mint", pool="pump", signature="sig"):
    return {"action": action, "mint": mint, "pool": pool, "signature": signature, "index": 0, "timestamp": 1}


def test_mapping_and_raw_amounts_remain_unproven():
    raw = payload("buy") | {"timestamp": 1_784_531_213_034, "txSigner": "observed-signer"}
    event = stream_event(raw, datetime(2026, 1, 1, tzinfo=UTC))
    assert event["event_type"] == "BUY"
    assert event["protocol_scope"] == "PUMPFUN_BONDING_CURVE"
    assert event["amount_semantics_status"] == "UNPROVEN"
    assert event["wallet"] == "observed-signer"
    assert event["blockchain_timestamp"] == 1_784_531_213
    assert event["event_time"].tzinfo is UTC
    assert int(event["event_time"].timestamp()) == event["blockchain_timestamp"]


def test_confirmed_historical_create_opens_tracking_and_is_deduplicated():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink)
    historical_create = {
        "txType": "create", "pool": "pump", "mint": "mint", "signature": "create-sig",
        "txSigner": "signer", "block": 415669956, "timestamp": 1_777_161_599_958,
        "initialBuy": 0.0, "tokensInPool": 1.0, "solInPool": 1.0,
    }
    collector.accept(historical_create, now)
    collector.accept(historical_create, now)
    collector.accept(payload("buy", signature="buy"), now + timedelta(seconds=5))
    assert [row["event_type"] for row in sink.rows] == ["CREATE_TOKEN", "BUY"]
    assert collector.metrics.duplicates == 1


def test_other_pool_and_missing_fields_are_not_emitted():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink)
    collector.accept(payload(pool="raydium"), now)
    collector.accept({"action": "buy"}, now)
    assert not [row for row in sink.rows if row.get("token_mint")]
    assert collector.metrics.invalid_messages == 1


def test_untracked_global_transfer_is_filtered_not_dead_lettered():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink)
    collector.accept({"action": "transfer", "signature": "transfer", "transfers": []}, now)
    assert collector.metrics.global_transfers_filtered == 1
    assert collector.metrics.invalid_messages == 0
    assert not sink.rows


def test_unobserved_migration_remains_unsupported():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink, grace_seconds=15)
    collector.accept(payload("migrate", signature="migration"), now + timedelta(seconds=20))
    assert collector.metrics.invalid_messages == 0
    assert collector.metrics.events_filtered_out == 1
    assert not sink.rows


def test_confirmed_pump_to_pumpswap_migration_maps_deterministically():
    raw = payload("migrate", mint="mint", pool="pump-amm", signature="migration") | {
        "txType": "migrate", "action": None, "poolCreatedBy": "pump", "txSigner": "signer",
        "timestamp": 1_776_556_942_305, "block": 414142703,
    }
    first = stream_event(raw, datetime(2026, 1, 1, tzinfo=UTC))
    second = stream_event(raw, datetime(2026, 1, 1, tzinfo=UTC))
    assert first["event_type"] == "MIGRATE"
    assert first["protocol_scope"] == "PUMPSWAP"
    assert first["event_id"] == second["event_id"]
    assert first["amount_semantics_status"] == "UNPROVEN"


def test_historical_action_only_pump_migration_maps_deterministically():
    """July-2026 replay payloads encode migration in ``action``, not txType."""
    raw = payload("migrate", mint="mint", pool="pump-amm", signature="migration-action") | {
        "poolCreatedBy": "pump", "txSigner": "signer",
        "timestamp": 1_784_332_496_176, "block": 420_000_000,
    }
    normalized = stream_event(raw, datetime(2026, 1, 1, tzinfo=UTC))
    assert normalized is not None
    assert normalized["event_type"] == "MIGRATE"
    assert normalized["token_mint"] == "mint"
    assert normalized["protocol_scope"] == "PUMPSWAP"


def test_migration_missing_mint_or_timestamp_is_refused():
    raw = payload("migrate", pool="pump-amm") | {"poolCreatedBy": "pump", "timestamp": 1_776_556_942_305}
    assert stream_event(raw | {"mint": None}, datetime(2026, 1, 1, tzinfo=UTC)) is None
    assert stream_event(raw | {"timestamp": None}, datetime(2026, 1, 1, tzinfo=UTC)) is None


def test_unconfirmed_create_pool_is_filtered_not_dead_lettered():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink)
    collector.accept(payload("createPool", pool="pump-amm"), now)
    assert collector.metrics.invalid_messages == 0
    assert collector.metrics.events_filtered_out == 1
    assert not sink.rows


def test_observed_claim_creator_fees_is_filtered_not_dead_lettered():
    sink, now = Sink(), datetime(2026, 1, 1, tzinfo=UTC)
    collector = PumpApiShadowFilter(sink)
    collector.accept(payload("claimCreatorFees", pool="pump"), now)
    assert collector.metrics.invalid_messages == 0
    assert collector.metrics.events_filtered_out == 1
    assert not sink.rows


def test_inspection_keeps_three_samples_per_action_pool_and_counts_create():
    book, now = InspectionBook(3), datetime(2026, 1, 1, tzinfo=UTC)
    for index in range(4):
        book.add(payload("create", mint=f"m{index}") | {"pool": "pump"}, now)
    assert book.target_creates() == 4
    assert len(book.samples["create/pump"]) == 3

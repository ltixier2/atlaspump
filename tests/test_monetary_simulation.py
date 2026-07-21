from datetime import UTC, datetime, timedelta

from atlaspump.monetary_simulation import (
    consensus_ids,
    first_price_at_or_after,
    maximum_drawdown,
    top_prediction_ids,
    valuation,
    virtual_quantity,
)


def test_virtual_stake_quantity_and_costs():
    assert virtual_quantity(1.0, 0.25) == 4.0
    result = valuation(1.0, 0.25, 0.5, 0.03)
    assert result["gross_pnl_eur"] == 1.0
    assert result["net_exit_value_eur"] == 1.94
    assert result["net_return"] == 0.94


def test_forward_price_never_uses_a_price_before_target():
    target = datetime(2026, 7, 19, tzinfo=UTC)
    points = [(target - timedelta(seconds=1), 1.0), (target + timedelta(seconds=5), 2.0)]
    assert first_price_at_or_after(points, target, 10) == (target + timedelta(seconds=5), 2.0)
    assert first_price_at_or_after(points, target, 2) is None


def test_top_rank_and_drawdown_are_deterministic():
    rows = [
        {"prediction_id": "b", "mlp_score": 0.9},
        {"prediction_id": "a", "mlp_score": 0.9},
        {"prediction_id": "c", "mlp_score": 0.1},
    ]
    assert top_prediction_ids(rows, "mlp_score", 0.34) == {"a", "b"}
    assert maximum_drawdown([1.0, -2.0, 0.5]) == (-2.0, -2.0)


def test_consensus_requires_the_requested_number_of_models():
    sets = [{"a", "b"}, {"a", "c"}, {"a", "b", "c"}]
    assert consensus_ids(sets, 2) == {"a", "b", "c"}
    assert consensus_ids(sets, 3) == {"a"}

import pytest

from atlaspump.analytical import assert_analytical_schema, metrics, select

ROWS = [
    {"token_mint": "b", "score_calibrated": 0.5, "score_raw": 0.2, "label_value": 0},
    {"token_mint": "a", "score_calibrated": 0.5, "score_raw": 0.9, "label_value": 1},
    {"token_mint": "c", "score_calibrated": 0.1, "score_raw": 0.1, "label_value": 0},
]


def test_top_k_breaks_ties_by_mint():
    assert select(ROWS, "score_calibrated", "TOP_K_DAILY", 1)[0]["token_mint"] == "a"


def test_threshold_and_percentile():
    assert sum(r["selected"] for r in select(ROWS, "score_raw", "SCORE_THRESHOLD", 0.2)) == 2
    assert sum(r["selected"] for r in select(ROWS, "score_raw", "TOP_PERCENTILE_DAILY", 20)) == 1


def test_metrics():
    assert metrics(select(ROWS, "score_raw", "TOP_K_DAILY", 1))["precision"] == 1


def test_financial_columns_rejected():
    with pytest.raises(ValueError):
        assert_analytical_schema({"token_mint", "pnl"})


def test_select_rejects_financial_input_field():
    with pytest.raises(ValueError, match="financial fields forbidden"):
        select([{"token_mint": "a", "score": 0.5, "label_value": 1, "pnl": 42}], "score", "TOP_K_DAILY", 1)

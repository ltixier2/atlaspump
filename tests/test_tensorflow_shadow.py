import numpy as np
import pytest

from atlaspump.tensorflow_shadow import prepare_batch, prepare_rows, shadow_selected


def test_feature_order_and_rejection():
    p = {"features": ["a", "b"], "medians": [0, 0], "means": [1, 2], "scales": [1, 2]}
    assert np.allclose(prepare_rows({"a": 2, "b": 6}, p), [[1, 2]])
    with pytest.raises(ValueError):
        prepare_rows({"a": 2}, p)


def test_ranking_policy_uses_its_frozen_validation_cutoff():
    assert shadow_selected(0.8, {"type": "top_fraction", "min_selected_score": 0.75})
    assert not shadow_selected(0.7, {"type": "top_fraction", "min_selected_score": 0.75})


def test_prepare_batch_keeps_feature_order():
    preprocessing = {"features": ["a", "b"], "medians": [0, 0], "means": [0, 0], "scales": [1, 1]}
    assert np.allclose(
        prepare_batch([{"a": 2, "b": 3}, {"a": 4, "b": 5}], preprocessing), [[2, 3], [4, 5]]
    )

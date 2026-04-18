import pandas as pd

from scoring.normalize import percentile_rank, percentile_rank_inverted


def test_strictly_increasing_sequence():
    s = pd.Series([10, 20, 30, 40])
    result = percentile_rank(s)
    assert result.tolist() == [0.25, 0.5, 0.75, 1.0]


def test_ties_get_equal_rank():
    s = pd.Series([10, 20, 20, 30])
    result = percentile_rank(s)
    assert result.iloc[1] == result.iloc[2]


def test_inverted_flips_order():
    s = pd.Series([10, 20, 30, 40])
    result = percentile_rank_inverted(s)
    assert result.tolist() == [0.75, 0.5, 0.25, 0.0]


def test_output_bounded_0_to_1():
    import numpy as np
    rng = np.random.default_rng(42)
    s = pd.Series(rng.random(500))
    r = percentile_rank(s)
    assert r.min() > 0
    assert r.max() <= 1

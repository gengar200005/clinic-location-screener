"""scripts/sensitivity_mc.py의 백분위 정규화가 파이프라인과 동일한지."""
import numpy as np
import pandas as pd

from scoring.normalize import percentile_rank
from scripts.sensitivity_mc import _pct_rank


def test_pct_rank_matches_pipeline_normalize():
    x = np.array([3.0, 1.0, 2.0, 2.0, 10.0])
    assert np.allclose(_pct_rank(x), percentile_rank(pd.Series(x)).to_numpy())

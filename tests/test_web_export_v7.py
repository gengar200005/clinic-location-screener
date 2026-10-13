"""publishers/web_export.py v7 표시 필드: 데이터 부족 항목·NaN 직렬화."""
import json

import numpy as np
import pandas as pd

from publishers.web_export import data_gaps, v7_block


def _row(**kw):
    base = {"t_transit": np.nan, "trend_flag": "ok", "trend_imputed": False,
            "survival_sample_flag": False, "n_stomach_ck_unknown": 0}
    base.update(kw)
    return pd.Series(base)


def test_gaps_lists_each_missing_item():
    gaps = data_gaps(_row(trend_flag="aggregated", survival_sample_flag=True, n_stomach_ck_unknown=2))
    assert any("대중교통" in g for g in gaps)
    assert any("권역 합산" in g for g in gaps)
    assert any("표본 부족" in g for g in gaps)
    assert any("2곳" in g for g in gaps)


def test_no_gaps_when_complete():
    assert data_gaps(_row(t_transit=45.0)) == []


def test_v7_block_is_json_safe_with_nan():
    blk = v7_block(_row(t_car_am=np.nan, trend_5069_3y=np.nan), None)
    json.dumps(blk)   # NaN 없이 직렬화
    assert blk["t_car_am"] is None and blk["sens"] is None

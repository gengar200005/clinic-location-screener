"""scoring/weighted_sum.py: 가중합 + 랭킹."""
import pandas as pd
import pytest

from config.constants import W_COMMUTE, W_COMPETITION, W_POPULATION
from scoring.weighted_sum import compute_final_scores, top_n


def test_score_in_unit_interval():
    """모든 정규화 항이 [0,1] → 가중합도 [0,1]."""
    df = pd.DataFrame({
        "adm_cd": list("ABCDE"),
        "adm_nm": [f"{x}동" for x in "ABCDE"],
        "c_raw": [1, 2, 3, 4, 5],
        "p_raw": [100, 200, 300, 400, 500],
        "t_raw": [10, 20, 30, 40, 50],
    })
    out = compute_final_scores(df)
    assert (out["score"] >= 0).all()
    assert (out["score"] <= 1).all()


def test_ranking_orders_by_score_desc():
    df = pd.DataFrame({
        "adm_cd": list("ABCDE"),
        "adm_nm": [f"{x}동" for x in "ABCDE"],
        "c_raw": [5, 4, 3, 2, 1],   # A 경쟁 강함 → 불리
        "p_raw": [500, 400, 300, 200, 100],  # A 인구 많음 → 유리
        "t_raw": [50, 40, 30, 20, 10],  # A 통근 김 → 불리
    })
    out = compute_final_scores(df)
    assert out["rank"].tolist() == [1, 2, 3, 4, 5]
    assert (out["score"].diff().dropna() <= 0).all()


def test_weights_applied_correctly():
    """동일한 raw 값일 때 score = sum of weights."""
    # 5개 동, raw 값 모두 동일 → c_norm/p_norm/t_norm 모두 0.6 (동점 평균 rank, n=5)
    df = pd.DataFrame({
        "adm_cd": list("ABCDE"),
        "adm_nm": [f"{x}동" for x in "ABCDE"],
        "c_raw": [1] * 5,
        "p_raw": [1] * 5,
        "t_raw": [1] * 5,
    })
    out = compute_final_scores(df)
    # rank(method='average', pct=True): 모두 동점 → (1+5)/2 / 5 = 0.6
    expected_norm = 0.6
    expected_score = (
        W_COMPETITION * (1 - expected_norm)  # inverted
        + W_POPULATION * expected_norm
        + W_COMMUTE * (1 - expected_norm)
    )
    assert all(abs(s - expected_score) < 1e-9 for s in out["score"])


def test_missing_columns_raises():
    df = pd.DataFrame({"adm_cd": ["A"], "c_raw": [1.0]})
    with pytest.raises(ValueError, match="필수 컬럼"):
        compute_final_scores(df)


def test_top_n_returns_correct_count():
    df = pd.DataFrame({
        "adm_cd": list(range(50)),
        "adm_nm": [f"{i}동" for i in range(50)],
        "c_raw": list(range(50)),
        "p_raw": list(range(50)),
        "t_raw": list(range(50)),
    })
    scored = compute_final_scores(df)
    assert len(top_n(scored, 30)) == 30
    assert len(top_n(scored, 5)) == 5


def test_p_norm_blends_trend_and_imputes_missing():
    from config.constants import W_P_TREND
    from scoring.weighted_sum import compute_final_scores
    df = pd.DataFrame({
        "adm_cd": list("abcd"), "adm_nm": list("abcd"),
        "c_raw": [1.0, 1.0, 1.0, 1.0], "p_raw": [10.0, 20.0, 30.0, 40.0], "t_raw": [30] * 4,
        "trend_5069_3y": [0.10, None, -0.05, 0.0],
    })
    out = compute_final_scores(df).set_index("adm_cd")
    assert out.loc["b", "trend_imputed"]
    expected = (1 - W_P_TREND) * out["p_size_norm"] + W_P_TREND * out["trend_norm"]
    assert (out["p_norm"] - expected).abs().max() < 1e-12

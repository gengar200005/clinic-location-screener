"""scoring/competition.py 단위 테스트.

API·파일 의존 없이 합성 DataFrame으로 공식 검증.
"""
import pandas as pd

from scoring.competition import (
    compute_competition_raw,
    count_clinics_per_dong,
)


def test_count_clinics_per_dong_basic():
    df = pd.DataFrame({
        "adm_cd": ["A", "A", "B"],
        "sido": ["S", "S", "S"],
        "sgg": ["X", "X", "Y"],
        "adm_nm": ["a동", "a동", "b동"],
        "yadmNm": ["c1", "c2", "c3"],
        "is_gi": [True, False, False],
    })
    out = count_clinics_per_dong(df).set_index("adm_cd")
    assert out.loc["A", "n_clinic"] == 2
    assert out.loc["A", "n_clinic_gi"] == 1
    assert out.loc["B", "n_clinic"] == 1


def test_competition_density_formula():
    """C_raw = 0.5 · density + 0.5 · radius."""
    n_by_dong = pd.DataFrame({
        "adm_cd": ["A"], "n_clinic": [10], "n_clinic_gi": [0],
    })
    within = pd.DataFrame({"adm_cd": ["A"], "n_within_radius": [4]})
    pop = pd.DataFrame({"adm_cd": ["A"], "population": [50_000]})

    out = compute_competition_raw(n_by_dong, within, population=pop)
    # density = 10 / (50000/10000) = 2.0
    # c_raw = 0.5 * 2.0 + 0.5 * 4 = 3.0
    assert abs(out["density_per_10k"].iloc[0] - 2.0) < 1e-9
    assert abs(out["c_raw"].iloc[0] - 3.0) < 1e-9


def test_competition_radius_only_mode():
    """population=None → 반경 항만 사용 (간이 모드)."""
    n_by_dong = pd.DataFrame({
        "adm_cd": ["A", "B"], "n_clinic": [5, 0], "n_clinic_gi": [0, 0],
    })
    within = pd.DataFrame({"adm_cd": ["A", "B"], "n_within_radius": [3, 0]})
    out = compute_competition_raw(n_by_dong, within, population=None)
    assert out.set_index("adm_cd").loc["A", "c_raw"] == 3.0
    assert out.set_index("adm_cd").loc["B", "c_raw"] == 0.0


def test_competition_zero_population_safe():
    """인구 0 동은 0으로 나누지 않고 density=0으로 처리."""
    n = pd.DataFrame({"adm_cd": ["A"], "n_clinic": [3], "n_clinic_gi": [0]})
    w = pd.DataFrame({"adm_cd": ["A"], "n_within_radius": [1]})
    p = pd.DataFrame({"adm_cd": ["A"], "population": [0]})
    out = compute_competition_raw(n, w, population=p)
    assert out["density_per_10k"].iloc[0] == 0
    assert out["c_raw"].iloc[0] == 0.5 * 1  # density 0 + radius 1

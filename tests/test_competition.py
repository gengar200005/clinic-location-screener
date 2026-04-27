"""scoring/competition.py 단위 테스트.

API·파일 의존 없이 합성 DataFrame으로 공식 검증.
"""
import pandas as pd

from scoring.competition import (
    _weighted_doctors,
    compute_competition_raw,
    compute_subcluster_max_doctors,
    count_clinics_per_dong,
    count_clinics_within_radius,
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


def test_competition_station_penalty():
    """역세권 페널티: c_raw = density·0.5 + radius·0.5 + station·0.2."""
    n_by_dong = pd.DataFrame({
        "adm_cd": ["A", "B"], "n_clinic": [10, 10], "n_clinic_gi": [0, 0],
    })
    within = pd.DataFrame({"adm_cd": ["A", "B"], "n_within_radius": [4, 4]})
    pop = pd.DataFrame({"adm_cd": ["A", "B"], "population": [50_000, 50_000]})
    # A: 역 500m 내 내과 의사 0명, B: 15명
    station = pd.DataFrame({
        "adm_cd": ["A", "B"],
        "n_doctors_station_500m_med": [0, 15],
    })
    out = compute_competition_raw(n_by_dong, within, population=pop, station_penalty=station).set_index("adm_cd")
    # density = 2.0, radius = 4 → base = 1.0 + 2.0 = 3.0
    # A: + 0.2 · 0 = 3.0 / B: + 0.2 · 15 = 6.0
    assert abs(out.loc["A", "c_raw"] - 3.0) < 1e-9
    assert abs(out.loc["B", "c_raw"] - 6.0) < 1e-9


def test_competition_station_penalty_none_means_zero():
    """station_penalty 미지정 시 페널티 0으로 처리 (캐시 없음 호환)."""
    n_by_dong = pd.DataFrame({"adm_cd": ["A"], "n_clinic": [10], "n_clinic_gi": [0]})
    within = pd.DataFrame({"adm_cd": ["A"], "n_within_radius": [4]})
    pop = pd.DataFrame({"adm_cd": ["A"], "population": [50_000]})
    out = compute_competition_raw(n_by_dong, within, population=pop, station_penalty=None)
    assert "n_doctors_station_500m_med" in out.columns
    assert out["n_doctors_station_500m_med"].iloc[0] == 0
    assert abs(out["c_raw"].iloc[0] - 3.0) < 1e-9


def test_compute_subcluster_max_doctors_basic():
    """동 내 가장 밀집된 500m disk 내과 의사 수.

    A동 centroid 기준:
      - C1, C2: 250m 이내 (서로 500m 안) → cluster 의사수 5+3=8
      - C3: 1.5km 안이지만 C1·C2와 600m 이상 떨어짐 → 단독 cluster 의사수 4
      → max = 8
    """
    import numpy as np
    # 좌표는 EPSG:5179 가정 (m 단위)
    admin = pd.DataFrame({
        "adm_cd": ["A"],
        "x_5179": [0.0],
        "y_5179": [0.0],
    })
    clinics = pd.DataFrame({
        "yadmNm": ["X 내과", "Y 내과", "Z 내과", "Q 정형"],
        "drTotCnt": [5, 3, 4, 10],
        "x_5179": [0.0, 200.0, 1000.0, 50.0],   # C1 origin, C2 200m, C3 1km away, Q 50m (비내과)
        "y_5179": [0.0,   0.0,    0.0,  0.0],
    })
    out = compute_subcluster_max_doctors(admin, clinics)
    assert out.loc[0, "adm_cd"] == "A"
    assert out.loc[0, "n_doctors_subcluster_max_med"] == 8
    # 비내과 Q는 제외돼야 함 — n_clinics_subcluster_max_med = 2 (C1+C2)
    assert out.loc[0, "n_clinics_subcluster_max_med"] == 2


def test_compute_subcluster_max_doctors_empty():
    """동 반경 안 내과 의원 0개면 둘 다 0."""
    admin = pd.DataFrame({"adm_cd": ["A"], "x_5179": [0.0], "y_5179": [0.0]})
    clinics = pd.DataFrame({
        "yadmNm": ["X 정형"],   # 비내과만
        "drTotCnt": [3],
        "x_5179": [100.0],
        "y_5179": [0.0],
    })
    out = compute_subcluster_max_doctors(admin, clinics)
    assert out.loc[0, "n_doctors_subcluster_max_med"] == 0
    assert out.loc[0, "n_clinics_subcluster_max_med"] == 0


def test_competition_subcluster_penalty_zero_default():
    """W_COMP_SUBCLUSTER 기본 0 — subcluster_penalty 머지하지만 c_raw 영향 0."""
    n_by_dong = pd.DataFrame({"adm_cd": ["A"], "n_clinic": [10], "n_clinic_gi": [0]})
    within = pd.DataFrame({"adm_cd": ["A"], "n_within_radius": [4]})
    pop = pd.DataFrame({"adm_cd": ["A"], "population": [50_000]})
    sub = pd.DataFrame({"adm_cd": ["A"], "n_doctors_subcluster_max_med": [99]})
    out = compute_competition_raw(n_by_dong, within, population=pop, subcluster_penalty=sub)
    assert out["n_doctors_subcluster_max_med"].iloc[0] == 99
    # W_COMP_SUBCLUSTER=0이라 99×0=0 → c_raw = 3.0 (density 1.0 + radius 2.0)
    assert abs(out["c_raw"].iloc[0] - 3.0) < 1e-9


# ─────────── GI 가중치 (W_GI_MULTIPLIER) 단위 테스트 — ADR-005 ───────────

def test_weighted_doctors_no_gi_column():
    """is_gi 컬럼 없으면 drTotCnt 그대로 (회귀 호환)."""
    df = pd.DataFrame({"drTotCnt": [3, 5, 2]})
    result = _weighted_doctors(df, gi_multiplier=2.0)
    assert list(result) == [3, 5, 2]


def test_weighted_doctors_multiplier_one_is_passthrough():
    """gi_multiplier=1.0이면 가중치 없이 dr 그대로."""
    df = pd.DataFrame({"drTotCnt": [3, 5, 2], "is_gi": [True, False, True]})
    result = _weighted_doctors(df, gi_multiplier=1.0)
    assert list(result) == [3, 5, 2]


def test_weighted_doctors_gi_amplified():
    """is_gi=True 의원의 dr만 W배. 일반 의원은 그대로.

    W=2.0, GI=[True,False,True], dr=[3,5,2] → [6, 5, 4].
    """
    df = pd.DataFrame({"drTotCnt": [3, 5, 2], "is_gi": [True, False, True]})
    result = _weighted_doctors(df, gi_multiplier=2.0)
    assert list(result) == [6.0, 5.0, 4.0]


def test_weighted_doctors_partial_multiplier():
    """W=1.5: GI 의원만 ×1.5."""
    df = pd.DataFrame({"drTotCnt": [4, 2], "is_gi": [True, False]})
    result = _weighted_doctors(df, gi_multiplier=1.5)
    assert list(result) == [6.0, 2.0]


def test_count_clinics_per_dong_with_gi_multiplier():
    """count_clinics_per_dong: gi_multiplier로 의사수 가중.

    GI 의사 2명, 일반 내과 의사 3명 → W=2.0이면 4+3=7.
    공식 검증: n_doctors_med_weighted = n_doctors_med + (W-1)×n_doctors_gi.
    """
    df = pd.DataFrame({
        "adm_cd": ["A", "A", "A", "A"],
        "sido": ["S"] * 4, "sgg": ["X"] * 4, "adm_nm": ["a"] * 4,
        "yadmNm": ["GI내과", "일반내과", "외과", "GI내과2"],
        "drTotCnt": [2, 3, 10, 1],
        "is_gi": [True, False, False, True],
    })
    out_unweighted = count_clinics_per_dong(
        df, internal_keyword="내과", sum_doctors=True, gi_multiplier=1.0
    )
    out_weighted = count_clinics_per_dong(
        df, internal_keyword="내과", sum_doctors=True, gi_multiplier=2.0
    )
    # 비가중: 2+3+1 = 6
    assert out_unweighted.loc[0, "n_doctors"] == 6
    # 가중 W=2: GI(2,1) ×2 + 일반(3) = 4+2+3 = 9
    assert out_weighted.loc[0, "n_doctors"] == 9
    # 식 검증: 6 + (2-1)×3 = 9
    n_doctors_gi = 2 + 1   # GI 의사 합
    expected = 6 + (2.0 - 1.0) * n_doctors_gi
    assert out_weighted.loc[0, "n_doctors"] == expected


def test_count_clinics_within_radius_with_gi_multiplier():
    """count_clinics_within_radius도 동일 가중 — 반경 안 의사 수에 적용."""
    admin = pd.DataFrame({
        "adm_cd": ["A"], "x_5179": [0.0], "y_5179": [0.0],
    })
    clinics = pd.DataFrame({
        "yadmNm": ["GI내과", "일반내과"],
        "drTotCnt": [4, 2],
        "is_gi": [True, False],
        "x_5179": [100.0, 200.0],   # 둘 다 1.5km 안
        "y_5179": [0.0, 0.0],
    })
    w1 = count_clinics_within_radius(
        clinics, admin, internal_keyword="내과", sum_doctors=True, gi_multiplier=1.0
    )
    w2 = count_clinics_within_radius(
        clinics, admin, internal_keyword="내과", sum_doctors=True, gi_multiplier=2.0
    )
    assert w1["n_doctors_within"].iloc[0] == 6   # 4+2
    assert w2["n_doctors_within"].iloc[0] == 10  # 8+2 (GI 4 → 8)


def test_count_clinics_within_radius_excludes_outside_radius():
    """반경 1.5km 밖 GI 의원은 가중치 적용해도 합산 X (경계 보장)."""
    admin = pd.DataFrame({"adm_cd": ["A"], "x_5179": [0.0], "y_5179": [0.0]})
    clinics = pd.DataFrame({
        "yadmNm": ["GI내과", "GI내과 멀리"],
        "drTotCnt": [3, 100],
        "is_gi": [True, True],
        "x_5179": [100.0, 5000.0],   # 두 번째는 5km 밖
        "y_5179": [0.0, 0.0],
    })
    w = count_clinics_within_radius(
        clinics, admin, internal_keyword="내과", sum_doctors=True, gi_multiplier=2.0
    )
    # 안에 있는 GI만: 3 ×2 = 6
    assert w["n_doctors_within"].iloc[0] == 6


def test_compute_subcluster_with_gi_multiplier():
    """compute_subcluster_max_doctors도 gi_multiplier 인식.

    같은 cluster (200m 안) GI 5명 + 일반 3명 = 8명 비가중,
    W=2.0이면 GI 5×2 + 3 = 13명.
    """
    admin = pd.DataFrame({"adm_cd": ["A"], "x_5179": [0.0], "y_5179": [0.0]})
    clinics = pd.DataFrame({
        "yadmNm": ["GI내과", "일반내과"],
        "drTotCnt": [5, 3],
        "is_gi": [True, False],
        "x_5179": [0.0, 100.0],
        "y_5179": [0.0, 0.0],
    })
    out_unw = compute_subcluster_max_doctors(admin, clinics, gi_multiplier=1.0)
    out_w = compute_subcluster_max_doctors(admin, clinics, gi_multiplier=2.0)
    assert out_unw.loc[0, "n_doctors_subcluster_max_med"] == 8
    assert out_w.loc[0, "n_doctors_subcluster_max_med"] == 13

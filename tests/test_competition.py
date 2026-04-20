"""scoring/competition.py 단위 테스트.

API·파일 의존 없이 합성 DataFrame으로 공식 검증.
"""
import pandas as pd

from scoring.competition import (
    compute_competition_raw,
    compute_subcluster_max_doctors,
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

"""scoring/population.py: merge_population + MIN_POPULATION 필터."""
import pandas as pd

from scoring.population import merge_population
from config.constants import MIN_POPULATION


def _dong(adm_cd, adm_cd10):
    return {"adm_cd": adm_cd, "adm_cd10": adm_cd10}


def test_p_raw_uses_pop_5069_when_available():
    """catchment 컬럼 없으면 동 단위 pop_5069 사용 (폴백)."""
    dong = pd.DataFrame([_dong("A", "1100000001"), _dong("B", "1100000002")])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [10000, 20000],
        "pop_5069": [4000, 9000],
        "ratio_5069": [0.4, 0.45],
    })
    merged = merge_population(dong, pop)
    assert merged.set_index("adm_cd").loc["A", "p_raw"] == 4000.0
    assert merged.set_index("adm_cd").loc["B", "p_raw"] == 9000.0


def test_p_raw_uses_catchment_when_available():
    """catchment_pop_*km + ratio_5069 있으면 catchment × ratio 사용."""
    dong = pd.DataFrame([
        {"adm_cd": "A", "adm_cd10": "1100000001", "catchment_pop_1_5km": 50000.0},
        {"adm_cd": "B", "adm_cd10": "1100000002", "catchment_pop_1_5km": 80000.0},
    ])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [10000, 20000],
        "pop_5069": [4000, 9000],
        "ratio_5069": [0.4, 0.45],
    })
    merged = merge_population(dong, pop).set_index("adm_cd")
    # A: 50000 * 0.4 = 20000 (동 pop_5069=4000 보다 훨씬 큼 = catchment 우위)
    assert merged.loc["A", "p_raw"] == 20000.0
    assert merged.loc["A", "catchment_pop_5069"] == 20000.0
    # B: 80000 * 0.45 = 36000
    assert merged.loc["B", "p_raw"] == 36000.0


def test_catchment_nan_falls_back_to_dong_pop_5069():
    """개별 동의 catchment가 NaN/0이면 동 단위 pop_5069로 폴백."""
    dong = pd.DataFrame([
        {"adm_cd": "A", "adm_cd10": "1100000001", "catchment_pop_1_5km": 50000.0},
        {"adm_cd": "B", "adm_cd10": "1100000002", "catchment_pop_1_5km": None},
    ])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [10000, 20000],
        "pop_5069": [4000, 9000],
        "ratio_5069": [0.4, 0.45],
    })
    merged = merge_population(dong, pop).set_index("adm_cd")
    assert merged.loc["A", "p_raw"] == 20000.0          # catchment 적용
    assert merged.loc["B", "p_raw"] == 9000.0           # pop_5069 폴백


def test_min_population_filter_excludes_small():
    """pop_total < MIN_POPULATION (500) 인 동은 제외."""
    dong = pd.DataFrame([
        _dong("A", "1100000001"),
        _dong("B", "1100000002"),
    ])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [MIN_POPULATION - 1, MIN_POPULATION + 1],
        "pop_5069": [100, 200],
        "ratio_5069": [0.5, 0.4],
    })
    merged = merge_population(dong, pop)
    assert "A" not in merged["adm_cd"].tolist()
    assert "B" in merged["adm_cd"].tolist()


def test_p_raw_falls_back_to_pop_total_when_no_age():
    """pop_5069 컬럼 없으면 P_raw = pop_total."""
    dong = pd.DataFrame([_dong("A", "1100000001")])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001"],
        "pop_total": [12345],
    })
    merged = merge_population(dong, pop)
    assert merged.set_index("adm_cd").loc["A", "p_raw"] == 12345.0


def test_crosswalk_codes_by_name_within_sido():
    """인천 개편: 경계는 옛 코드, KOSIS는 새 코드 → 같은 시도 내 동 이름으로 연결."""
    from scoring.population import crosswalk_codes
    dongs = pd.DataFrame({
        "adm_cd10": ["2811052000", "2814052500", "1111051500"],
        "adm_nm": ["인천광역시 중구 연안동", "인천광역시 동구 화수1·화평동", "서울특별시 종로구 청운효자동"],
    })
    pop = pd.DataFrame({
        "adm_cd10": ["2812552000", "2812557000", "1111051500", "1211075800"],
        "dong_nm": ["연안동", "화수1화평동", "청운효자동", "연안동"],  # 다른 시도 동명은 무시
    })
    out = crosswalk_codes(dongs, pop)
    assert out.tolist() == ["2812552000", "2812557000", "1111051500"]

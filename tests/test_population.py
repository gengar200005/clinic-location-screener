"""scoring/population.py: merge_population + MIN_POPULATION 필터."""
import pandas as pd

from scoring.population import merge_population
from config.constants import MIN_POPULATION


def _dong(adm_cd, adm_cd10):
    return {"adm_cd": adm_cd, "adm_cd10": adm_cd10}


def test_p_raw_uses_pop_40plus_when_available():
    dong = pd.DataFrame([_dong("A", "1100000001"), _dong("B", "1100000002")])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [10000, 20000],
        "pop_40plus": [4000, 9000],
        "ratio_40plus": [0.4, 0.45],
    })
    merged = merge_population(dong, pop)
    assert merged.set_index("adm_cd").loc["A", "p_raw"] == 4000.0
    assert merged.set_index("adm_cd").loc["B", "p_raw"] == 9000.0


def test_min_population_filter_excludes_small():
    """pop_total < MIN_POPULATION (500) 인 동은 제외."""
    dong = pd.DataFrame([
        _dong("A", "1100000001"),
        _dong("B", "1100000002"),
    ])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001", "1100000002"],
        "pop_total": [MIN_POPULATION - 1, MIN_POPULATION + 1],
        "pop_40plus": [100, 200],
        "ratio_40plus": [0.5, 0.4],
    })
    merged = merge_population(dong, pop)
    assert "A" not in merged["adm_cd"].tolist()
    assert "B" in merged["adm_cd"].tolist()


def test_p_raw_falls_back_to_pop_total_when_no_age():
    """pop_40plus 컬럼 없으면 P_raw = pop_total."""
    dong = pd.DataFrame([_dong("A", "1100000001")])
    pop = pd.DataFrame({
        "adm_cd10": ["1100000001"],
        "pop_total": [12345],
    })
    merged = merge_population(dong, pop)
    assert merged.set_index("adm_cd").loc["A", "p_raw"] == 12345.0

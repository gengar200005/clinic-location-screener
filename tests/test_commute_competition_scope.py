"""A rejected commute must not enter a candidate-only density calculation."""
import pandas as pd
import pytest
from scoring.commute import classify_commute
from scoring.competition import compute_competition_raw


def test_out_of_scope_zero_and_in_scope_invalid_population():
    base=pd.DataFrame({'adm_cd':['valid','zero'], 'n_clinic':[2,1],
                       't_car_am':[30,77], 't_car_pm':[40,66]})
    within=pd.DataFrame({'adm_cd':['valid','zero'],'n_within_radius':[3,1]})
    population=pd.DataFrame({'adm_cd':['valid','zero'],'population':[10000,0]})
    scoped=classify_commute(base,60)
    candidate=scoped[scoped.commute_ok]
    result=compute_competition_raw(candidate,within,population=population)
    expected=compute_competition_raw(base.iloc[:1],within.iloc[:1],population=population.iloc[:1])
    pd.testing.assert_series_equal(result.c_raw,expected.c_raw)
    assert scoped.loc[1,'commute_status']=='exceed_car'
    with pytest.raises(ValueError,match='finite positive population'):
        compute_competition_raw(base,within,population=population)


def test_pipeline_scopes_before_density_without_removing_guard():
    from pathlib import Path
    source=(Path(__file__).resolve().parents[1]/'scoring/pipeline.py').read_text(encoding='utf-8')
    assert source.index('base = base[base["commute_ok"]]') < source.index('comp = compute_competition_raw(')

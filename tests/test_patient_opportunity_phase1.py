import pandas as pd
import pytest
from scripts.patient_opportunity_phase1 import unique, demand_order, compare


def test_invalid_demand_does_not_turn_into_zero_rank_or_success():
    f=pd.DataFrame({'demand':[10,20,None,0,-1]})
    result=demand_order(f,'demand')
    assert list(result[:2])==[2,1]
    assert result[2:].isna().all()


def test_equal_demands_keep_equal_position():
    result=demand_order(pd.DataFrame({'demand':[20,20,10]}),'demand')
    assert result.tolist()==[1,1,3]


def test_ambiguous_identity_is_not_merged_or_summed():
    with pytest.raises(ValueError):unique(pd.DataFrame({'id':['a','a']}),['id'])
    with pytest.raises(ValueError):unique(pd.DataFrame({'id':['a',None]}),['id'])


def test_entering_and_departing_candidates_are_visible():
    assert compare(['a','b'],['b','c'])==dict(old=2,diagnostic=2,retained=1,entered=1,departed=1)

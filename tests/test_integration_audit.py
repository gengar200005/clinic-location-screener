import pandas as pd
import pytest
from scripts.audit_integration import membership_comparison, unique


def test_new_and_departed_candidates_survive_comparison():
    a=pd.DataFrame({'anchor_id':['a','b'],'rank':[1,2]})
    b=pd.DataFrame({'anchor_id':['b','c'],'rank':[1,2]})
    result=membership_comparison(a,b).set_index('anchor_id')
    assert set(result.index)=={'a','b','c'}
    assert pd.isna(result.loc['a','rank_new'])
    assert pd.isna(result.loc['c','rank_old'])


@pytest.mark.parametrize('values', [['a','a'],['a',None]])
def test_ambiguous_identity_is_not_aggregated(values):
    with pytest.raises(ValueError,match='Invalid identity'):
        unique(pd.DataFrame({'anchor_id':values}),['anchor_id'])

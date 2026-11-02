import pandas as pd
import pytest
from scripts.field_visit_phase2 import validate_selection


def cohort():
    return pd.DataFrame({'anchor_id':['a','b','c','d'],'visit_status':['우선 임장']*4,'personal_scope':[None]*4})


def test_personal_exclusion_never_promoted():
    f=cohort();f.loc[0,'personal_scope']='정릉 개인 제외'
    with pytest.raises(ValueError):validate_selection(f)


def test_duplicate_or_too_many_visits_rejected():
    f=cohort();f.loc[0,'anchor_id']='b'
    with pytest.raises(ValueError):validate_selection(f)
    with pytest.raises(ValueError):validate_selection(pd.concat([cohort(),cohort()]))


def test_requested_visit_scope_accepts_four():
    validate_selection(cohort())

import json
import unittest
from pathlib import Path
import pandas as pd
from scripts.build_current_recommendations import trend,excluded,visit_state,score_frame,stable_order,include_in_current_view,AS_OF,ROOT


def history(amount=130,count=130):
    qs=['20243','20244','20251','20252','20253','20254','20261','20262']
    return [dict(quarter=q,amount={'state':'observed','value':str(100 if i<4 else amount)},
            transactions={'state':'observed','value':str(100 if i<4 else count)},shop_change='0') for i,q in enumerate(qs)]


class CurrentRecommendationTests(unittest.TestCase):
    def test_four_quarter_direction_changes_explanation_and_priority(self):
        good=trend(history());bad=trend(history(130,70));decline=trend(history(70,70))
        self.assertEqual(good['status'],'persistent_growth');self.assertEqual(good['positive_pairs'],4)
        self.assertEqual(bad['status'],'persistent_decline')
        self.assertNotIn('환자 수요 증가',bad['explanation']);self.assertNotEqual(good['explanation'],bad['explanation'])
        self.assertEqual(decline['transactions_yoy'],-30)
        frame=pd.DataFrame({'anchor_id':['a','b'],'commute_lane':[0,0],'structural':[70.,70.]})
        self.assertEqual(stable_order(frame,pd.Series([75.,65.])).anchor_id.tolist(),['a','b'])
        self.assertEqual(stable_order(frame,pd.Series([65.,75.])).anchor_id.tolist(),['b','a'])

    def test_missing_period_and_zero_baseline_are_not_low_demand(self):
        for h in [history()[:-1],history()[:2]+history()[3:]]:
            self.assertEqual(trend(h)['status'],'unlinked_or_insufficient')
            self.assertIsNone(trend(h)['transactions_yoy'])
        h=history();h[0]['transactions']['value']='0'
        self.assertEqual(trend(h)['status'],'unlinked_or_insufficient')
        h=history();h[0]['transactions']['value']='*'
        self.assertEqual(trend(h)['status'],'unlinked_or_insufficient')
        with self.assertRaises(ValueError):trend(history()+[history()[0]])

    def test_one_quarter_spike_is_not_persistent_growth(self):
        h=history(90,90);h[-1]['amount']['value']='99999';h[-1]['transactions']['value']='99999'
        self.assertNotEqual(trend(h)['status'],'persistent_growth')
        self.assertEqual(trend(history(130,130))['status'],trend(history(135,135))['status'])

    def test_dense_supply_is_comparable_and_stable_order_is_deterministic(self):
        f=pd.DataFrame({'anchor_id':['b','a','c'],'commute_lane':[0,0,0],'structural':[70.,70.,60.],
          'demand_1000':[1000.,10000.,100000.],'internal_1000':[0.,5.,100.],
          'endo_1000':[0.,3.,150.],'hospital_1000':[0.,0.,3.],'gastric_1000':[0.,2.,50.]})
        scores=score_frame(f,.5,2,1000)
        self.assertEqual(len(scores),3);self.assertTrue(scores.notna().all())
        for _ in range(3):self.assertEqual(stable_order(f,pd.Series([70.,70.,60.])).anchor_id.tolist(),['a','b','c'])

    def test_space_verification_changes_visit_state_without_promoting_bad_building_to_bad_area(self):
        p={'id':'p','commute_lane':0,'comparable':True,'exclusion':None}
        self.assertEqual(visit_state(p,[]),'매물·공간 사전확인')
        b=dict(candidate_id='p',building_id='building-1',floor=3,same_location_verified=True,vacant=True,exclusive_pyeong=98,layout_verified=True,elevator_verified=True,checked_on=AS_OF,source='검증된 도면·현 공실 확인')
        self.assertEqual(visit_state(p,[b]),'현장 확인 가능')
        self.assertTrue(include_in_current_view(dict(p,visit_state=visit_state(p,[b]))))
        self.assertFalse(include_in_current_view(dict(p,visit_state=visit_state(p,[]))))
        for k,v in [('floor',None),('vacant',False),('exclusive_pyeong',70),('source',None),('checked_on','2025-01-01')]:
            self.assertEqual(visit_state(p,[dict(b,**{k:v})]),'매물·공간 사전확인')
        self.assertEqual(visit_state(dict(p,commute_lane=2),[]),'추가 분석 필요')
        self.assertEqual(visit_state(dict(p,manual_revisit=True),[]),'새 정보 확보 후 재방문')

    def test_personal_exclusions_and_whole_cohort_actual_output(self):
        self.assertIsNotNone(excluded('서울특별시 성북구 정릉1동'))
        self.assertIsNotNone(excluded('경기도 화성시 동탄구'))
        self.assertIsNotNone(excluded('인천광역시 부평구 부개동'))
        self.assertIsNone(excluded('경기도 수원시 팔달구 화서동'))
        d=json.loads((ROOT/'web/data/current-recommendations.json').read_text(encoding='utf8'))
        self.assertTrue(all(x['registry_coordinates']>=x['comparable_common'] for x in d['coverage']))
        self.assertEqual(len({p['id'] for p in d['candidates']}),len(d['candidates']))
        top=[p for p in d['candidates'] if p.get('research_order') and p['research_order']<=5 and not p['exclusion']]
        self.assertFalse(any('정릉' in p['area'] for p in top))
        self.assertTrue(any(p['region']=='경기' and p['research_order']==1 for p in top))


if __name__=='__main__':unittest.main()

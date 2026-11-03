import copy
import json
import unittest
from datetime import date, timedelta
import numpy as np
import pandas as pd
from scripts.build_current_recommendations import ROOT, structural_score, stable_order
from scripts.recommendation_density import reference_weights, percentile
from scripts.recommendation_policy import parse_rules, as_of_date, date_state, premises_review


def rules():
    return parse_rules(json.loads((ROOT/'config/current_recommendations.json').read_text(encoding='utf8')))


def fixture():
    f=pd.DataFrame(dict(anchor_id=['commercial_a','commercial_b','commercial_c','commercial_d'],
        building_key=['a','b','c','d'],building_identity_state=['reported_building_id']*4,
        x=[100.,110.,400.,700.],y=[100.]*4,commute_lane=[0]*4,admin_code=['A','A','B','C']))
    for r in [1000,1500]:
        f[f'demand_{r}']=[200.,210.,1000.,800.]
        for k in ['internal','endo','hospital','gastric']: f[f'{k}_{r}']=[1.,2.,2.,8.]
    return f


class ConfigurationTests(unittest.TestCase):
    def test_default_legacy_math_unchanged(self):
        f=fixture();r=rules();r['reference_distribution']['mode']='legacy_coordinates'
        p=f.demand_1000
        expected=.5*p.rank(pct=True)*100+.5*pd.concat([(p/(1+f.internal_1000)).rank(pct=True)*100,
            (p/(1+f.endo_1000+2*f.hospital_1000)).rank(pct=True)*100,(p/(1+f.gastric_1000)).rank(pct=True)*100],axis=1).median(axis=1)
        np.testing.assert_allclose(structural_score(f,r),expected)

    def test_changed_settings_change_base_and_alternatives(self):
        f=fixture();r=rules();base=structural_score(f,r)
        for key,value in [('demand_weight',.9),('hospital_burden',20)]:
            changed=copy.deepcopy(r);changed[key]=value
            # Hospital only affects the endoscopy axis; make it the median component.
            if key=='hospital_burden':
                f.loc[0,'hospital_1000']=10;f.loc[0,'hospital_1500']=10
                f.loc[1,['endo_1000','endo_1500']]=0
            expected=structural_score(f,changed,weight=changed['demand_weight'],hospital=changed['hospital_burden'])
            np.testing.assert_allclose(structural_score(f,changed),expected)
        changed=copy.deepcopy(r);changed['radii_m']=[1000]
        f.loc[0,'demand_1500']=99999
        np.testing.assert_allclose(structural_score(f,changed),structural_score(f,changed,radius=1000))
        self.assertFalse(np.allclose(base,structural_score(f,dict(r,demand_weight=.9))))
        rng=np.random.default_rng(19)
        varied=pd.DataFrame(dict(anchor_id=[str(i) for i in range(20)],x=np.arange(20)*250.,y=np.zeros(20)))
        for rad in [1000,1500]:
            varied[f'demand_{rad}']=rng.uniform(10,10000,20)
            for kind in ['internal','endo','hospital','gastric']: varied[f'{kind}_{rad}']=rng.integers(0,10,20).astype(float)
        self.assertFalse(np.allclose(structural_score(varied,r),structural_score(varied,dict(r,hospital_burden=20))))
        self.assertFalse(np.allclose(structural_score(varied,r),structural_score(varied,dict(r,radii_m=[1000]))))
        ordered=f.assign(structural=[71,70,69,68]);scores=pd.Series([71,74,69,68])
        self.assertNotEqual(stable_order(ordered,scores,5).anchor_id.tolist(),stable_order(ordered,scores,1).anchor_id.tolist())

    def test_bad_settings_raise(self):
        for k,v in [('demand_weight',-1),('demand_weight',float('nan')),('hospital_burden',0),
                    ('priority_band_width',0),('radii_m',[500]),('radii_m',[1000,1000]),
                    ('seoul_shortlist',0),('alternative_demand_weights',[2]),('hospital_burdens',[False])]:
            with self.subTest(k=k,v=v), self.assertRaises(ValueError):parse_rules(dict(rules(),**{k:v}))

    def test_as_of_is_explicit_or_korean_today(self):
        self.assertEqual(as_of_date('2026-11-03'),'2026-11-03')
        self.assertEqual(len(as_of_date()),10)
        for v in ['2026-2-03','2026-02-30','oops']:
            with self.assertRaises(ValueError):as_of_date(v)


class BuildingTests(unittest.TestCase):
    def setUp(self):
        self.p=dict(id='p',lat=37.5,lon=127.,comparable=True,commute_lane=0,exclusion=None)
        self.b=dict(candidate_id='p',building_id='b',floor=3,vacant=True,exclusive_pyeong=98,
                    layout_prechecked=True,elevator_prechecked=True,same_location_verified=True,
                    checked_on='2026-11-03',source='고정 도면·전화 확인 사례')
    def review(self,**changes):return premises_review(self.p,[dict(self.b,**changes)],rules(),'2026-11-03')

    def test_dates_today_previous_boundary_expired_future_invalid_missing(self):
        for age,state in [(0,'fresh'),(1,'fresh'),(14,'fresh'),(15,'expired'),(-1,'future')]:
            day=(date(2026,11,3)-timedelta(days=age)).isoformat()
            out=self.review(vacancy_checked_on=day)
            self.assertEqual(out['records'][0]['dates']['vacant']['state'],state)
            self.assertEqual(out['state']=='현장 확인 가능',state=='fresh')
        for value,state in [(None,'missing'),('bad','invalid')]:
            out=self.review(checked_on=value)
            self.assertEqual(out['records'][0]['dates']['vacant']['state'],state)
            self.assertNotEqual(out['state'],'현장 확인 가능')

    def test_item_specific_ttl(self):
        out=self.review(area_checked_on='2026-08-01',vacancy_checked_on='2026-11-02')
        self.assertEqual(out['state'],'현장 확인 가능')
        out=self.review(vacancy_checked_on='2026-08-01')
        self.assertNotEqual(out['state'],'현장 확인 가능')

    def test_area_thresholds_not_whole_area_rejection(self):
        for area,status,ready in [(94,'below_target',False),(95,'within_target',True),(98,'within_target',True),(100,'within_target',True),(105,'above_target',False)]:
            out=self.review(exclusive_pyeong=area)
            self.assertEqual(out['records'][0]['area_state'],status)
            self.assertEqual(out['state']=='현장 확인 가능',ready)
            self.assertIsNone(self.p['exclusion'])
            self.assertEqual(out['opening_suitability'],'판단 보류')

    def test_vacancy_and_parking_not_assumed(self):
        for vacant,ready in [(True,True),(None,False),(False,False)]:
            self.assertEqual(self.review(vacant=vacant)['state']=='현장 확인 가능',ready)
        for parking,status in [(True,'confirmed'),(None,'unknown_or_stale'),(False,'constraint')]:
            out=self.review(parking=parking)
            self.assertEqual(out['records'][0]['parking'],status)
            self.assertEqual(out['state'],'현장 확인 가능')
            self.assertTrue(out['records'][0]['pending'])

    def test_previsit_is_not_final_layout_or_opening_approval(self):
        out=self.review();self.assertEqual(out['state'],'현장 확인 가능')
        for key in ['layout_prechecked','elevator_prechecked']:
            self.assertNotEqual(self.review(**{key:False})['state'],'현장 확인 가능')
        self.assertIn('현장',out['records'][0]['pending'][0])

    def test_different_address_requires_reassessment(self):
        self.assertTrue(self.review(same_location_verified=False)['records'][0]['location_reassessment_required'])
        self.assertNotEqual(self.review(lon=128.)['state'],'현장 확인 가능')

    def test_historical_as_of_cannot_use_future_verification(self):
        out=premises_review(self.p,[self.b],rules(),'2026-11-02')
        self.assertNotEqual(out['state'],'현장 확인 가능')
        self.assertEqual(out['records'][0]['dates']['area']['state'],'future')
        self.assertIsNone(out['records'][0]['exclusive_pyeong'])

    def test_new_occupied_record_overrides_old_vacancy_and_future_does_not(self):
        older=dict(self.b,checked_on='2026-11-02')
        newer=dict(self.b,vacant=False,checked_on='2026-11-03')
        out=premises_review(self.p,[older,newer],rules(),'2026-11-03')
        self.assertNotEqual(out['state'],'현장 확인 가능')
        self.assertEqual(premises_review(self.p,[older,newer],rules(),'2026-11-02')['state'],'현장 확인 가능')


class DensityTests(unittest.TestCase):
    def test_duplicate_other_id_and_near_duplicate_same_cell(self):
        f=fixture();r=rules();base=structural_score(f,r)
        for n in [1,10,100]:
            dup=pd.concat([f.iloc[[0]].assign(anchor_id=f'alias{i}',x=249.9,y=100.) for i in range(n)])
            augmented=pd.concat([f,dup],ignore_index=True)
            np.testing.assert_allclose(base,structural_score(augmented,r).iloc[:len(f)])
        # No trusted identity: near records with same complete evidence in same cell.
        f['building_identity_state']='unresolved'
        duplicate=f.iloc[[0]].assign(anchor_id='alias',x=101.)
        np.testing.assert_allclose(structural_score(f,r),structural_score(pd.concat([f,duplicate],ignore_index=True),r).iloc[:len(f)])

    def test_new_evidence_and_location_are_preserved_and_can_change_result(self):
        f=fixture();r=rules();base=structural_score(f,r)
        added=f.iloc[[0]].copy();added['anchor_id']='new';added['building_key']='new';added['x']=1200.
        for rad in [1000,1500]:added[f'demand_{rad}']=99999.
        augmented=pd.concat([f,added],ignore_index=True)
        scores=structural_score(augmented,r)
        self.assertEqual(len(scores),5)
        self.assertFalse(np.allclose(base,scores.iloc[:4]))
        self.assertEqual(scores.idxmax(),4)

    def test_reference_mass_one_per_cell_and_row_shuffle_reproducibility(self):
        f=fixture();r=rules();spec=r['reference_distribution']
        w=reference_weights(f,spec)
        self.assertAlmostEqual(w.sum(),3.)
        scores=structural_score(f,r);shuffled=f.sample(frac=1,random_state=43)
        np.testing.assert_allclose(scores,structural_score(shuffled,r).reindex(f.index))

    def test_midpoint_definition_is_not_legacy_rank(self):
        p=pd.Series([1.,2.,3.]);w=pd.Series([1.,1.,1.])
        np.testing.assert_allclose(percentile(p)-percentile(p,w),[50/3]*3)

    def test_unknown_identity_grid_boundary_limit_is_exposed(self):
        f=fixture();f['building_identity_state']='unresolved';f.loc[0,'x']=249.9
        alias=f.iloc[[0]].assign(anchor_id='alias',x=250.1)
        # Such an unidentifiable cross-boundary copy adds a cell: NOT claimed invariant.
        self.assertFalse(np.allclose(structural_score(f,rules()),structural_score(pd.concat([f,alias],ignore_index=True),rules()).iloc[:len(f)]))


if __name__=='__main__':unittest.main()


def test_scenario_order_uses_alternative_structure_in_same_band():
    from scripts.build_current_recommendations import scenario_order, stable_order
    f=pd.DataFrame(dict(anchor_id=['a','b','c'],admin_code=['x','x','y'],commute_lane=[0,0,0],structural=[74.,72.,71.]))
    alternative=pd.Series([71.,73.,74.])
    expected=stable_order(f.assign(structural=alternative),alternative,5).anchor_id.tolist()
    assert expected==['c','b','a']
    assert scenario_order(f,alternative,0,5).anchor_id.tolist()==expected
    shuffled=f.sample(frac=1,random_state=3)
    assert scenario_order(shuffled,alternative,0,5).anchor_id.tolist()==expected


class EvidenceConflictTests(unittest.TestCase):
    setUp=BuildingTests.setUp
    def test_latest_vacancy_date_and_floor_type(self):
        a=dict(self.b,vacancy_checked_on='2026-11-02')
        b=dict(self.b,checked_on='2026-11-02',vacancy_checked_on='2026-11-03',vacant=False,floor='3',source='최근 비공실')
        out=premises_review(self.p,[a,b],rules(),'2026-11-03')
        self.assertEqual(out,premises_review(self.p,[b,a],rules(),'2026-11-03'))
        self.assertEqual(len(out['records']),1)
        self.assertIs(out['records'][0]['vacant'],False)
        self.assertNotEqual(out['state'],'현장 확인 가능')

    def test_same_date_conflicts(self):
        for key,value,field in [('exclusive_pyeong',80,'area'),('layout_prechecked',False,'layout'),('elevator_prechecked',False,'elevator')]:
            with self.subTest(field=field):
                out=premises_review(self.p,[self.b,dict(self.b,**{key:value})],rules(),'2026-11-03')
                self.assertEqual(out['records'][0]['dates'][field]['state'],'conflict')
                self.assertNotEqual(out['state'],'현장 확인 가능')

    def test_separate_units_and_basement(self):
        for key,value in [('floor',-3),('building_id','other'),('unit','302')]:
            a=dict(self.b,unit='301')
            b=dict(a,**{key:value},vacant=False)
            out=premises_review(self.p,[a,b],rules(),'2026-11-03')
            self.assertEqual(len(out['records']),2)
            self.assertEqual(out['state'],'현장 확인 가능')

    def test_absent_unknown_false_differ(self):
        a=dict(self.b,checked_on='2026-11-02')
        b={k:self.b[k] for k in ('candidate_id','building_id','floor','source','checked_on')}
        self.assertEqual(premises_review(self.p,[a,b],rules(),'2026-11-03')['state'],'현장 확인 가능')
        for value in (None,False):
            out=premises_review(self.p,[a,dict(b,vacant=value)],rules(),'2026-11-03')
            self.assertIs(out['records'][0]['vacant'],value)
            self.assertNotEqual(out['state'],'현장 확인 가능')

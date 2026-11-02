"""Fixed local fixtures only: never calls public APIs."""
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from scripts import market_demand as m


def row(q='20262',amount='150',count='12',**extra):
    return {'행정동_코드':'11590530','행정동_코드_명':'상도1동','기준_년분기_코드':q,
            '서비스_업종_코드':'CS200006','당월_매출_금액':amount,'당월_매출_건수':count,**extra}


class MarketDemandTests(unittest.TestCase):
    def test_observation_states_and_decimal_precision(self):
        self.assertEqual(m.cell('0'),{'state':'observed','value':'0'})
        self.assertEqual(m.cell('*')['state'],'suppressed')
        for x in [None,'','-','NULL']:self.assertEqual(m.cell(x)['state'],'missing')
        for x in ['NaN','Infinity','-1','bad']:self.assertEqual(m.cell(x)['state'],'invalid')
        self.assertEqual(m.cell('9007199254740993.001')['value'],'9007199254740993.001')

    def test_zero_denominator_and_no_fabricated_growth(self):
        for fn in [m.ratio,m.change]:
            for x in [0,None,'*','NaN']:self.assertIsNone(fn(100,x))
        self.assertEqual(m.change(0,100),'-100.00')
        self.assertEqual(m.ratio('150','12'),'12.50')
        self.assertEqual(m.unit_change(150,12,100,10),'25.00')
        self.assertIsNone(m.unit_change(150,0,100,10))

    def test_keys_industry_period_and_duplicates(self):
        with self.assertRaises(ValueError):m.index_rows([row(),row()])
        for key,val in [('行','ignored'),('행정동_코드','1159053000'),('기준_년분기_코드','20265')]:
            if key=='行':continue
            r=row();r[key]=val
            with self.assertRaises(ValueError):m.index_rows([r])
        r=row();r['서비스_업종_코드']='CS200001'
        self.assertEqual(m.index_rows([r],'CS200006'),{})

    def test_exact_yoy_scope_and_unrounded_unit_growth(self):
        rows=[row(),row('20252','100','10'),row('20261','999','999')]
        n={'records':[{'code':'11590530','quarter':'20262','amount':150,'yoy':999,'shops':None}]}
        out=m.medical_history(m.index_rows(rows),{},n,'11590530','상도1동')[0]
        self.assertEqual(out['amount_yoy'],'50.00')  # not stored yoy or prior quarter
        self.assertEqual(out['transactions_yoy'],'20.00')
        self.assertEqual(out['per_transaction_yoy'],'25.00')
        missing=m.medical_history(m.index_rows([row(),row('20261')]),{},n,'11590530','상도1동')[0]
        self.assertIsNone(missing['amount_yoy'])
        self.assertIsNone(missing['transactions_yoy'])
        with self.assertRaises(ValueError):m.medical_history(m.index_rows(rows),{},n,'11590530','다른동')
        with self.assertRaises(ValueError):m.medical_history(m.index_rows([row(amount='151')]),{},n,'11590530','상도1동')

    def test_annual_cadence_conflict_not_quarter_growth(self):
        idx=m.index_rows([row('20262',x='20'),row('20254',x='20')])
        a=m.annual_context(idx,'11590530','20262','x')
        self.assertEqual(a['documented_update_quarter'],'20254')
        self.assertTrue(a['matches_update_quarter'])
        self.assertIsNone(a['quarterly_growth']);self.assertIsNone(a['actual_observation_date'])
        idx[('11590530','20262')]['x']='21'
        self.assertFalse(m.annual_context(idx,'11590530','20262','x')['matches_update_quarter'])

    def test_marginal_age_is_not_age_time_cross(self):
        r={'연령대_60_이상_유동인구_수':'30','시간대_06_11_유동인구_수':'100'}
        self.assertEqual(list(m.distributions(r,r'^연령대_')),['연령대_60_이상_유동인구_수'])
        self.assertNotIn('50~69',str(m.distributions(r,r'^연령대_')))

    def test_living_fixture_does_not_double_count_or_invent_means(self):
        with tempfile.TemporaryDirectory() as td:
            cache=Path(td)
            (cache/'grid_centers.json').write_text(json.dumps([[37.5,127,'A',0,0]]))
            raw='일자,시간,행정동코드,250M격자,생활인구합계,남자 50~54세\n20261029,09,11110515,A,10,*\n20261029,09,11110530,A,20,5\n'
            with zipfile.ZipFile(cache/'250_LOCAL_RESD_20261029.zip','w') as z:z.writestr('sample.csv',raw.encode('cp949'))
            pilots=[{'id':'p','lat':37.5,'lon':127}]
            with patch.object(m,'CACHE',cache):a=m.living_audit(pilots)
            self.assertEqual(a['extra_admin_rows_at_same_grid_hour'],1)
            self.assertEqual(a['raw_rows'],2);self.assertEqual(a['cell_states']['suppressed'],1)
            self.assertTrue(a['age_time_cross_columns'])
            for k in ['totals','means','unique_visitors']:self.assertIsNone(a[k])
            with zipfile.ZipFile(cache/'250_LOCAL_RESD_20261029.zip','w') as z:z.writestr('sample.csv',(raw+raw.splitlines()[-1]+'\n').encode('cp949'))
            with patch.object(m,'CACHE',cache),self.assertRaises(ValueError):m.living_audit(pilots)

    def test_unavailable_living_does_not_block_other_modules(self):
        with tempfile.TemporaryDirectory() as td,patch.object(m,'CACHE',Path(td)):
            pilots=[{'id':'p'}];a=m.living_audit(pilots)
        self.assertEqual(a['status'],'unavailable');self.assertIsNone(pilots[0]['living_population']['grid_counts']['1000'])


if __name__=='__main__':unittest.main()

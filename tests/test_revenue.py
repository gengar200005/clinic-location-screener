import unittest
from scripts.build_revenue import build, growth, previous


class RevenueTests(unittest.TestCase):
    def test_quarter_boundaries(self):
        self.assertEqual(previous('20261'), '20254')
        self.assertEqual(previous('20262', True), '20252')

    def test_missing_and_zero_baseline_are_not_declines(self):
        self.assertIsNone(growth(None, 100))
        self.assertIsNone(growth(100, 0))
        self.assertEqual(growth(0, 100), -100)

    def test_missing_sales_and_code_systems(self):
        row = {'행정동_코드_명':'상암동','당월_매출_금액':'100', '주중_매출_금액':'70',
               '주말_매출_금액':'30','여성_매출_금액':'50','연령대_50_매출_금액':'20','연령대_60_이상_매출_금액':'30'}
        shops = {('20262','11440740'):{'행정동_코드_명':'상암동','전체_점포_수':'10'},
                 ('20262','11110000'):{'행정동_코드_명':'없는동','전체_점포_수':'2'}}
        result = build({('20262','11440740'):row}, shops,
                       [{'region':'서울','adm_nm':'서울특별시 마포구 상암동','adm_cd':'SGIS_OTHER'}])
        self.assertEqual(result['matches'][0]['code'],'11440740')
        missing = next(r for r in result['records'] if r['name']=='없는동')
        self.assertIsNone(missing['amount'])
        self.assertIsNone(missing['qoq'])
        self.assertEqual(missing['status'],'missing_sales')

    def test_inconsistent_totals_fail(self):
        with self.assertRaises(ValueError):
            build({('20262','x'):{'당월_매출_금액':'100','주중_매출_금액':'100','주말_매출_금액':'100'}},{},[])


if __name__ == '__main__':
    unittest.main()

import unittest
from scripts.build_field_followup import build, decomposition, numeric


class FieldFollowupTests(unittest.TestCase):
    def test_decomposition_identity_and_null(self):
        d=decomposition({'당월_매출_금액':'150','당월_매출_건수':'12'},
                        {'당월_매출_금액':'100','당월_매출_건수':'10'})
        self.assertEqual(d['transactions_yoy'],20)
        self.assertAlmostEqual((1+d['transactions_yoy']/100)*(1+d['per_transaction_yoy']/100)-1,.5,places=3)
        self.assertIsNone(decomposition({'당월_매출_금액':'0','당월_매출_건수':'0'}, {})['per_transaction'])
        self.assertEqual(numeric('0'),0)
        self.assertIsNone(numeric(''))
        with self.assertRaises(ValueError): numeric('nan')

    def test_building_boundary_and_unverified_premises(self):
        d=build()
        self.assertEqual(len(d['buildings']),2)
        for b in d['buildings']:
            self.assertEqual(b['revenue_code'],'11380640')
            self.assertIsNone(b['exclusive_area_sqm'])
            self.assertIsNone(b['rent_month_krw'])
        self.assertEqual(len(d['areas']),12)
        codes={a['code'] for a in d['areas']}
        self.assertTrue({'11380640','11380650'}<=codes)

    def test_new_provider_not_overwritten_as_old_equipment(self):
        d=build()
        p=next(p for p in d['competitors'] if p['name']=='바른플러스내과의원')
        self.assertFalse(p['equipment_2025'])
        self.assertTrue(p['source'].startswith('https://'))
        self.assertLess(p['straight_distance_m'],150)
        k=next(a for a in d['areas'] if a['code']=='11500593')
        self.assertGreater(k['amount_yoy'],40)
        self.assertLess(k['transactions_yoy'],15)
        missing=next(a for a in d['areas'] if not a['code'])
        self.assertIsNone(missing['transactions_yoy'])


if __name__=='__main__': unittest.main()

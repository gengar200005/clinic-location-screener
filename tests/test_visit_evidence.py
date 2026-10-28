import unittest
from scripts.build_visit_evidence import build, summarize, growth


class VisitEvidenceTests(unittest.TestCase):
    def test_missing_is_not_zero(self):
        self.assertIsNone(growth(None, 100))
        self.assertIsNone(growth(100, 0))
        self.assertEqual(summarize([], '20262')['status'], 'unmatched')

    def test_rolling_year_boundary_and_missing_period(self):
        records=[{'quarter':q,'amount':v} for q,v in [('20243',100),('20244',100),('20251',100),('20252',100),('20253',200),('20254',200),('20261',200),('20262',200)]]
        result=summarize(records,'20262')
        self.assertEqual(result['four_quarter_yoy'],100)
        self.assertEqual(result['yoy'],100)
        self.assertIsNone(summarize(records[1:],'20262')['four_quarter_yoy'])

    def test_duplicate_rejected(self):
        with self.assertRaises(ValueError):
            summarize([{'quarter':'20262','amount':1}]*2,'20262')

    def test_actual_outputs_preserve_scope_and_ranks(self):
        data=build(); rows=data['candidates']
        self.assertEqual(len(rows),11)
        self.assertEqual([int(x['visit_order']) for x in rows[:10]],list(range(1,11)))
        self.assertEqual(rows[-1]['revenue_code'],'11380650')
        self.assertEqual(rows[-1]['v3_joint_rank'],'')
        self.assertEqual(rows[7]['revenue']['status'],'unmatched')
        self.assertNotIn('source',rows[0])

if __name__=='__main__': unittest.main()

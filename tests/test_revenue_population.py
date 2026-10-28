import json
import unittest
from pathlib import Path
from scripts.build_revenue import build


class RevenuePopulationTests(unittest.TestCase):
    def test_population_requires_same_period_code_name_and_positive_denominator(self):
        row = {'행정동_코드_명':'상암동','당월_매출_금액':'100',
               '주중_매출_금액':'70','주말_매출_금액':'30','여성_매출_금액':'50',
               '연령대_50_매출_금액':'20','연령대_60_이상_매출_금액':'30'}
        sales = {('20262','11440740'):row}
        pop = {'행정동_코드_명':'상암동','총_상주인구_수':'10'}
        def result(key, value):
            return build(sales, {}, [], {key:value})['records'][0]['per_resident']
        self.assertEqual(result(('20262','11440740'), pop), 10)
        self.assertIsNone(result(('20261','11440740'), pop))
        self.assertIsNone(result(('20262','SGIS_ID'), pop))
        self.assertIsNone(result(('20262','11440740'), {**pop,'행정동_코드_명':'다른동'}))
        self.assertIsNone(result(('20262','11440740'), {**pop,'총_상주인구_수':'0'}))

    def test_historical_yoy_and_current_population(self):
        path=Path(__file__).resolve().parents[1]/'web/data/revenue.json'
        data=json.loads(path.read_text(encoding='utf-8'))
        by_key={(r['quarter'],r['code']):r for r in data['records']}
        a=by_key[('20252','11440740')]; b=by_key[('20242','11440740')]
        self.assertEqual(a['yoy'],round((a['amount']/b['amount']-1)*100,2))
        latest=by_key[('20262','11440740')]
        self.assertEqual(latest['per_resident'],round(latest['amount']/latest['population'],2))
        c=by_key[('20232','11440740')]
        self.assertEqual(b['yoy'],round((b['amount']/c['amount']-1)*100,2))
        self.assertIsNone(by_key[('20212','11440740')]['yoy'])
        first=by_key[('20212','11440740')]
        self.assertGreater(first['population'],0)
        self.assertEqual(first['per_resident'],round(first['amount']/first['population'],2))

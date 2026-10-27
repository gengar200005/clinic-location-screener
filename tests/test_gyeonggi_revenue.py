import json
import unittest
from pathlib import Path

from scripts.fetch_gyeonggi_revenue import inspect_download
from scripts.build_gyeonggi_revenue_scope import build, snapshot

ROOT = Path(__file__).resolve().parents[1]


class GyeonggiRevenueTests(unittest.TestCase):
    def test_html_success_is_not_data(self):
        for body in (b'<script>alert("error")</script>', b'<!DOCTYPE html><html>broken', b'\xef\xbb\xbf<html>'):
            self.assertEqual(inspect_download(200, body)['status'], 'html_instead_of_csv')

    def test_failed_empty_and_malformed_downloads(self):
        self.assertEqual(inspect_download(500, b'Server Error')['status'], 'http_error')
        for body in (b'', b'a,b,c,d\n', b'a,b,c,d\n1,2\n', b'a,a,c,d\n1,2,3,4'):
            self.assertNotEqual(inspect_download(200, body)['status'], 'downloaded_unverified')

    def test_csv_acquisition_does_not_mean_verified(self):
        for encoding in ('utf-8-sig', 'cp949'):
            body = '기준년월,행정동코드,중분류업종코드,매출금액\n202501,12345678,X1,100\n'.encode(encoding)
            result = inspect_download(200, body)
            self.assertEqual(result['status'], 'downloaded_unverified')
            self.assertEqual(result['row_count'], 1)

    def test_scope_preserves_bucheon_and_excludes_unranked(self):
        rows = build([dict(adm_cd='SGIS1', adm_nm='경기도 부천시 중동', rank=2),
                      dict(adm_cd='SGIS2', adm_nm='경기도 고양시 일산1동', rank=1),
                      dict(adm_cd='X', adm_nm='경기도 수원시 X동', rank=None),
                      dict(adm_cd='Y', adm_nm='서울특별시 X동', rank=3)])
        self.assertEqual([r['anchor_id'] for r in rows], ['SGIS2', 'SGIS1'])
        self.assertTrue(all(r['revenue_code'] is None and r['yoy'] is None for r in rows))

    def test_duplicate_candidates_fail(self):
        row = dict(adm_cd='x', adm_nm='경기도 X동', rank=1)
        with self.assertRaises(ValueError):
            build([row, row])

    def test_committed_snapshot_is_reproducible_and_has_no_invented_sales(self):
        def read(path):
            return json.loads((ROOT/path).read_text(encoding='utf-8'))
        folder = 'data/manual/revenue/gyeonggi/'
        result = snapshot(read('web/data/research.json'), read(folder+'verification.json'), read(folder+'acquisition.json'))
        self.assertEqual(result, read('web/data/gyeonggi-revenue-scope.json'))
        self.assertEqual(len(result['candidates']), 208)
        self.assertEqual(result['coverage']['verified_trends'], 0)
        self.assertTrue(all(r['revenue_code'] is None and r['periods'] == [] and r['mom'] is None and r['yoy'] is None for r in result['candidates']))


if __name__ == '__main__':
    unittest.main()

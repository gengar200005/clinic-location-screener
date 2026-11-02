import tempfile
import unittest
import zipfile
from pathlib import Path
from scripts.build_gyeonggi_inventory import source_rows


class ZipSourceTests(unittest.TestCase):
    def test_english_schema_preserves_decimal_and_codes(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'sample.zip'
            with zipfile.ZipFile(p,'w') as z:
                z.writestr('data.csv','std_ym,admdong_cd,mdclass_indutype_cd,sales_amt\r\n202010,4111156000,J0,123456.78\r\n\r\n')
            rows=list(source_rows(p))
            self.assertEqual(rows,[{'기준년월':'202010','행정동코드':'4111156000','중분류업종코드':'J0','매출금액':'123456.78'}])

    def test_ambiguous_archive_and_malformed_rows_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            p=Path(folder)/'sample.zip'
            with zipfile.ZipFile(p,'w') as z:
                z.writestr('a.csv','a,b');z.writestr('b.csv','a,b')
            with self.assertRaises(ValueError):list(source_rows(p))
            with zipfile.ZipFile(p,'w') as z:
                z.writestr('a.csv','std_ym,admdong_cd,mdclass_indutype_cd,sales_amt\n202010,4111156000,J0,1,2')
            with self.assertRaises(ValueError):list(source_rows(p))

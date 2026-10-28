"""Download the public CSV export using its published form (no API key).

Only CS200006 rows are retained. Build/validate separately before publishing.
"""
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


class ExportForm(HTMLParser):
    def __init__(self):
        super().__init__()
        self.active = False
        self.fields = {}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == 'form':
            self.active = attrs.get('id') == 'frmSheet'
        if self.active and tag == 'input' and attrs.get('name'):
            self.fields[attrs['name']] = attrs.get('value', '')

    def handle_endtag(self, tag):
        if tag == 'form':
            self.active = False


def main():
    target = ROOT/'data/manual/revenue'
    target.mkdir(parents=True, exist_ok=True)
    staged, manifest = {}, {'retrieved_at': datetime.now(timezone.utc).date().isoformat(), 'files': {}}
    for kind, dataset in [('sales','OA-22175'), ('shops','OA-22172')]:
        params = urlencode(dict(infId=dataset, srvType='S', serviceKind='1')).encode()
        req = Request(f'https://data.seoul.go.kr/dataList/sheetView.do?infId={dataset}&srvType=S', data=params)
        with urlopen(req, timeout=60) as r:
            page = r.read().decode('utf-8')
        form = ExportForm(); form.feed(page)
        if form.fields.get('infId') != dataset or 'strWhere' not in form.fields:
            raise ValueError('Public export form changed; review before fetching')
        req = Request('https://datafile.seoul.go.kr/bigfile/iot/sheet/csv/download.do', data=urlencode(form.fields).encode())
        with urlopen(req, timeout=120) as r:
            raw = r.read()
        reader = csv.DictReader(io.StringIO(raw.decode('cp949')))
        if '서비스_업종_코드' not in (reader.fieldnames or []):
            raise ValueError('Unexpected CSV response')
        rows = [row for row in reader if row['서비스_업종_코드'] == 'CS200006']
        if not rows:
            raise ValueError('Empty clinic export')
        output = io.StringIO(newline=''); writer = csv.DictWriter(output, reader.fieldnames)
        writer.writeheader(); writer.writerows(rows); staged[kind] = output.getvalue()
        manifest['files'][kind] = dict(dataset=dataset, raw_sha256=hashlib.sha256(raw).hexdigest(),
                                       retained_rows=len(rows), quarters=sorted({r['기준_년분기_코드'] for r in rows}))
    # Do not touch existing files until both responses have parsed successfully.
    for kind, value in staged.items():
        (target/f'{kind}.csv').write_text(value, encoding='utf-8', newline='')
    (target/'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == '__main__':
    main()

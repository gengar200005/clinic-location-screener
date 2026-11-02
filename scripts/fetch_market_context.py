"""Cache-only by default; opt in to public CSV acquisition for MISSING files.

Never overwrites an existing raw file. No API key required. No 4-week download.
Run: python -B -m scripts.fetch_market_context [--acquire-missing]
"""
import argparse
import csv
import hashlib
import io
import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from scripts.fetch_revenue import ExportForm
from scripts.market_demand import CACHE, DATASETS, ROOT, index_rows


def inspect(raw):
    reader=csv.DictReader(io.StringIO(raw.decode('cp949')))
    rows=list(reader)
    index=index_rows(rows)
    return dict(rows=len(rows),columns=reader.fieldnames,
                quarters=sorted({q for _,q in index}),sha256=hashlib.sha256(raw).hexdigest())


def acquire(dataset):
    params=urlencode(dict(infId=dataset,srvType='S',serviceKind='1')).encode()
    req=Request(f'https://data.seoul.go.kr/dataList/sheetView.do?infId={dataset}&srvType=S',data=params)
    with urlopen(req,timeout=60) as response:page=response.read().decode('utf-8')
    form=ExportForm();form.feed(page)
    if form.fields.get('infId')!=dataset or 'strWhere' not in form.fields:
        raise ValueError('Official export form changed; inspect before acquisition')
    req=Request('https://datafile.seoul.go.kr/bigfile/iot/sheet/csv/download.do',data=urlencode(form.fields).encode())
    with urlopen(req,timeout=120) as response:raw=response.read()
    inspect(raw)  # validate before the caller can create a file
    return raw


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--acquire-missing',action='store_true')
    args=parser.parse_args();results={}
    for kind,dataset in DATASETS.items():
        path=CACHE/(dataset+'.csv')
        try:
            if not path.exists() and args.acquire_missing:
                raw=acquire(dataset);CACHE.mkdir(parents=True,exist_ok=True)
                with path.open('xb') as output:output.write(raw)
            if not path.exists():results[dataset]={'status':'unavailable'};continue
            results[dataset]=dict(status='cached_validated',**inspect(path.read_bytes()))
        except Exception as exc:
            results[dataset]=dict(status='failed',error=type(exc).__name__+': '+str(exc))
    # This diagnostic manifest is derived; existing raw caches are immutable.
    report=ROOT/'reports/market_demand_20261102';report.mkdir(parents=True,exist_ok=True)
    (report/'cache_validation.json').write_text(json.dumps(dict(
        checked_at=datetime.now(timezone.utc).isoformat(),files=results),ensure_ascii=False,indent=2),encoding='utf-8')
    print({dataset:r['status'] for dataset,r in results.items()})


if __name__=='__main__':main()

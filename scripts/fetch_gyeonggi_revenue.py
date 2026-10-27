"""Retry the publicly linked download; an HTTP 200 is not proof of CSV data.

Acquisition only: neither a downloaded CSV nor catalog metadata approves an
industry, code crosswalk or time series for publication.
"""
import csv
import hashlib
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASET = 'H92OOSWZXICJM0UO4IHH38259217'
PAGE = f'https://data.gg.go.kr/portal/data/service/selectServicePage.do?infId={DATASET}&infSeq=1'
DOWNLOAD = 'https://data.gg.go.kr/portal/data/sheet/downloadSheetData.do'
CATALOG = 'https://www.data.go.kr/data/15151650/fileData.do'


def inspect_download(status, content):
    """Do not misclassify HTML alerts/error pages, empty files or junk as data."""
    if status != 200:
        return dict(status='http_error', http_status=status)
    for encoding in ('utf-8-sig', 'cp949'):
        try:
            text = content.decode(encoding)
            break
        except UnicodeDecodeError:
            pass
    else:
        return dict(status='unknown_encoding')
    if re.search(r'<(?:!doctype|html|script|head|body)\b', text, re.I):
        return dict(status='html_instead_of_csv')
    try:
        rows = list(csv.reader(io.StringIO(text), strict=True))
    except csv.Error:
        return dict(status='invalid_csv')
    if len(rows) < 2 or len(rows[0]) < 4 or any(len(r) != len(rows[0]) for r in rows[1:]):
        return dict(status='invalid_csv_shape')
    if len(set(rows[0])) != len(rows[0]) or any(not c.strip() for c in rows[0]):
        return dict(status='invalid_header')
    return dict(status='downloaded_unverified', encoding=encoding,
                columns=rows[0], row_count=len(rows)-1)


def main():
    import requests
    folder = ROOT / 'data/manual/revenue/gyeonggi'
    folder.mkdir(parents=True, exist_ok=True)
    audit = dict(checked_at=datetime.now(timezone.utc).isoformat(),
                 dataset=DATASET, catalog_url=CATALOG, source_url=PAGE,
                 download_url=DOWNLOAD, attempts=[])
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0'
    token = None
    try:
        response = session.get(PAGE, timeout=40)
        match = re.search(r'name="_csrf" content="([^"]+)"', response.text)
        token = match.group(1) if match else None
        audit['detail_page'] = dict(http_status=response.status_code,
                                    bytes=len(response.content),
                                    complete_html='</html>' in response.text.lower())
    except requests.RequestException as exc:
        audit['detail_page'] = dict(error=type(exc).__name__)
    params = dict(downloadType='C', infId=DATASET, infSeq='1')
    for method in ('GET', 'POST'):
        if method == 'POST' and not token:
            continue
        try:
            headers = {'Referer': PAGE}
            kwargs = dict(params=params) if method == 'GET' else dict(data={**params, '_csrf': token})
            if method == 'POST':
                headers['X-CSRF-TOKEN'] = token
            response = session.request(method, DOWNLOAD, headers=headers, timeout=40, **kwargs)
            result = inspect_download(response.status_code, response.content)
            result.update(method=method, bytes=len(response.content),
                          sha256=hashlib.sha256(response.content).hexdigest())
            audit['attempts'].append(result)
            if result['status'] == 'downloaded_unverified':
                # Immutable, content-addressed original. Never promote it to UI here.
                path = folder / (result['sha256'] + '.csv')
                if not path.exists():
                    path.write_bytes(response.content)
                audit['raw_file'] = path.name
                break
        except requests.RequestException as exc:
            audit['attempts'].append(dict(method=method, status='network_error', error=type(exc).__name__))
    (folder / 'acquisition.json').write_text(json.dumps(audit, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(audit, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

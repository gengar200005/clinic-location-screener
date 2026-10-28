"""Separate field investigation evidence; never rewrite screener scores or equipment."""
import csv
import hashlib
import json
import math
from pathlib import Path
from scripts.build_visit_evidence import growth, summarize

ROOT = Path(__file__).resolve().parents[1]
CHECKED = '2026-10-29'


def numeric(value):
    if value is None or str(value).strip() in ('', '-', 'null', 'NA'):
        return None
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError('Invalid nonnegative raw value')
    return result


def raw_index(path):
    result = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            if row['서비스_업종_코드'] != 'CS200006':
                continue
            key = (row['행정동_코드'], row['기준_년분기_코드'])
            if key in result:
                raise ValueError(f'Duplicate raw record: {key}')
            result[key] = row
    return result


def decomposition(current, prior):
    amount = numeric(current.get('당월_매출_금액'))
    count = numeric(current.get('당월_매출_건수'))
    old_amount = numeric(prior.get('당월_매출_금액'))
    old_count = numeric(prior.get('당월_매출_건수'))
    unit = amount / count if amount is not None and count else None
    old_unit = old_amount / old_count if old_amount is not None and old_count else None
    return dict(amount=amount, transactions=count, prior_amount=old_amount,
                prior_transactions=old_count, amount_yoy=growth(amount, old_amount),
                transactions_yoy=growth(count, old_count), per_transaction=unit,
                prior_per_transaction=old_unit, per_transaction_yoy=growth(unit, old_unit))


def distance(lat, lon, p):
    a, b = math.radians(lat), math.radians(float(p['lat']))
    x = math.sin((b-a)/2)**2 + math.cos(a)*math.cos(b)*math.sin(math.radians(float(p['lon'])-lon)/2)**2
    return 6371000 * 2 * math.asin(min(1, math.sqrt(x)))


def build(root=ROOT):
    revenue = json.loads((root/'web/data/revenue.json').read_text(encoding='utf-8'))
    raw = raw_index(root/'data/manual/revenue/sales.csv')
    shops = raw_index(root/'data/manual/revenue/shops.csv')
    latest = revenue['latest']
    prior = str(int(latest[:4])-1)+latest[-1]
    evidence = json.loads((root/'web/data/visit-evidence.json').read_text(encoding='utf-8'))
    areas = []
    pairs = [(c['zone'], c['revenue_code']) for c in evidence['candidates']]
    pairs.append(('증산동 · 두빌 소재지', '11380640'))
    for zone, code in pairs:
        current, old = raw.get((code, latest), {}), raw.get((code, prior), {})
        row = dict(zone=zone, code=code, latest=latest, prior=prior, **decomposition(current, old))
        for field, raw_field in [('shops','전체_점포_수'),('openings','개업_점포_수'),('closures','폐업_점포_수')]:
            row[field] = numeric(shops.get((code, latest), {}).get(raw_field))
        row['prior_shops'] = numeric(shops.get((code, prior), {}).get('전체_점포_수'))
        normalized = next((r for r in revenue['records'] if r['code']==code and r['quarter']==latest), None)
        if normalized and row['amount'] != normalized['amount']:
            raise ValueError(f'Raw/normalized amount mismatch {code}')
        areas.append(row)
    provider_data = json.loads((root/'web/data/research_providers.json').read_text(encoding='utf-8'))
    providers = [dict(zip(provider_data['columns'], r)) for r in provider_data['rows']]
    focus = [
        ('상도',37.4954671494,126.9546990372,['서울에이스내과의원','강호경내과의원']),
        ('정릉',37.6087667817,127.0115066166,['바른플러스내과의원','김성경내과의원']),
        ('까치산',37.5312107477,126.847842461,['김영내과의원','속시원한내과의원','까치산내과의원','연세위드내과의원']),
        ('두빌191',37.5782607,126.9010885,['프리즘의원','재단법인 한국산업보건연구재단 서울디엠씨건강의원','주홍돈내과의원']),
    ]
    verified = {
        '서울에이스내과의원': ('https://seoul-ace.com/', '위·대장내시경, 검진, 초음파, 수액 공식 안내'),
        '바른플러스내과의원': ('https://xn--vb0bn4en6hitbo9ecpnmo6a.kr/', '대장내시경·용종절제, 검진, 초음파, 수액 공식 안내. 2025 장비 미관측을 현재 미운영으로 해석 금지'),
        '프리즘의원': ('https://www.prismclinic.co.kr/', '초음파·수액 공식 안내. 내시경 여부는 미확인'),
    }
    competitors = []
    for zone, lat, lon, names in focus:
        for name in names:
            candidates = [p for p in providers if p['name']==name and p.get('lat') and p.get('lon')]
            if not candidates:
                raise ValueError(f'Missing provider {name}')
            p = min(candidates, key=lambda p:distance(lat,lon,p))
            source, service = verified.get(name, ('', '기존 HIRA 자료만 연결 · 현재 진료내용 미검증'))
            competitors.append(dict(zone=zone, name=name, address=p['address'],
                straight_distance_m=round(distance(lat,lon,p)), internal_specialists=p['internal_specialists'],
                equipment_2025=p['egd_equipment_2025_positive'], opened=p['개설일자'],
                service=service, source=source, checked_on=CHECKED if source else None))
    buildings = [
        dict(name='두빌191', address='서울특별시 은평구 수색로 191', lat=37.5782607,lon=126.9010885,
             source='https://www.prismclinic.co.kr/index.php/html/19',
             note='프리즘의원 4층: 초음파·수액 부분 경쟁. 내시경 여부 미확인. 신경외과 입점도 확인.'),
        dict(name='두빌195', address='서울특별시 은평구 수색로 195',lat=37.578574,lon=126.900694,
             source='https://saramsengak.com/',
             note='정신건강의학과 공식 안내: 기계식 주차·대형 SUV 제한. 해당 의원 방문 정책이며 새 임차인의 주차 배정은 미확인.'),
    ]
    from shapely.geometry import Point, shape
    boundaries = json.loads((root/'web/data/boundaries.geojson').read_text(encoding='utf-8'))
    for b in buildings:
        names = [f['properties']['adm_nm'] for f in boundaries['features'] if shape(f['geometry']).covers(Point(b['lon'],b['lat']))]
        if names != ['서울특별시 은평구 증산동']:
            raise ValueError(f'Building boundary mismatch {names}')
        b.update(administrative_area=names[0],revenue_code='11380640',boundary_code='11120670',
                 coordinate_basis='입점 의원 좌표 · 출입구/필지 좌표 미검증',
                 exclusive_area_sqm=None, rent_month_krw=None,deposit_krw=None,
                 availability='현재 공실·95~100평 전용면적 미확인',checked_on=CHECKED)
    files=['data/manual/revenue/sales.csv','data/manual/revenue/shops.csv','web/data/revenue.json',
           'web/data/research_providers.json','web/data/boundaries.geojson','web/data/visit-evidence.json']
    return dict(as_of=CHECKED, areas=areas, competitors=competitors, buildings=buildings,
        sources=[dict(file=p,sha256=hashlib.sha256((root/p).read_bytes()).hexdigest()) for p in files],
        limitations=['결제 건수는 환자 수·내시경 검사 수가 아님. 건당 금액은 진료 가격이 아님.',
        '전체 일반의원 업종 변화. 기관 규모·진료과 구성·추정 방식 변화로도 발생할 수 있어 원인 확정 불가.',
        '점포 개폐업은 같은 분기 행정동 집계이며 개별 의원 매출 변화와 직접 연결하지 않음.',
        '거리 모두 직선거리. 철길 횡단·보행 시간·실제 진료권은 미검증.',
        '기존 모델·순위·2025 장비 원본은 변경하지 않음. 건물은 매물이 아닌 조사 대상.'])


if __name__ == '__main__':
    data=build()
    (ROOT/'web/data/field-followup.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf-8')
    print(f"{len(data['areas'])} areas, {len(data['competitors'])} providers, {len(data['buildings'])} buildings")

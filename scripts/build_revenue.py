"""Build a display-only clinic-market snapshot from official Seoul CSV exports.

No imputation, no changes to screening scores. Standard-library only.
"""
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDUSTRY = 'CS200006'


def previous(q, years=False):
    y, n = int(q[:4]), int(q[4])
    return f'{y-1}{n}' if years else (f'{y}{n-1}' if n > 1 else f'{y-1}4')


def growth(now, before):
    return None if now is None or before is None or before <= 0 else round((now / before - 1) * 100, 2)


def read_rows(path):
    rows = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            if row['서비스_업종_코드'] != INDUSTRY:
                continue
            key = (row['기준_년분기_코드'], row['행정동_코드'])
            if len(key[0]) != 5 or key[0][-1] not in '1234':
                raise ValueError(f'Invalid quarter: {key}')
            if key in rows:
                raise ValueError(f'Duplicate key: {key}')
            rows[key] = row
    if not rows:
        raise ValueError(f'No clinic rows: {path}')
    return rows


def number(row, field):
    if not row or row.get(field, '') == '':
        return None
    value = int(row[field])
    if value < 0:
        raise ValueError(f'Negative value: {field}')
    return value


def read_population(path):
    rows = {}
    with path.open(encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            key = (row['기준_년분기_코드'], row['행정동_코드'])
            if key in rows:
                raise ValueError(f'Duplicate population key: {key}')
            if number(row, '총_상주인구_수') != number(row, '남성_상주인구_수') + number(row, '여성_상주인구_수'):
                raise ValueError(f'Inconsistent population total: {key}')
            rows[key] = row
    return rows


def build(sales, shops, anchors, population=None):
    quarters = sorted({q for q, _ in sales})
    records = []
    for q, code in sorted(sales.keys() | shops.keys()):
        s, p = sales.get((q, code)), shops.get((q, code))
        amount = number(s, '당월_매출_금액')
        count = number(p, '전체_점포_수')
        if s and abs(amount - number(s, '주중_매출_금액') - number(s, '주말_매출_금액')) > 5:
            raise ValueError(f'Inconsistent weekday/weekend total: {q}/{code}')
        prev = sales.get((previous(q), code))
        year = sales.get((previous(q, True), code))
        prev_shops = number(shops.get((previous(q), code)), '전체_점포_수')
        pop = (population or {}).get((q, code))
        same_name = pop and pop['행정동_코드_명'] == (s or p)['행정동_코드_명']
        residents = number(pop, '총_상주인구_수') if same_name else None
        records.append(dict(quarter=q, code=code, name=(s or p)['행정동_코드_명'],
            amount=amount, shops=count, qoq=growth(amount, number(prev, '당월_매출_금액')),
            yoy=growth(amount, number(year, '당월_매출_금액')),
            shop_change=None if count is None or prev_shops is None else count-prev_shops,
            per_shop=None if amount is None or not count else round(amount/count),
            population=residents,
            per_resident=None if amount is None or not residents else round(amount/residents, 2),
            population_status='matched_same_quarter_code_name' if same_name else 'unmatched',
            female_pct=None if not amount else round(number(s, '여성_매출_금액')/amount*100, 1),
            age50plus_pct=None if not amount else round((number(s, '연령대_50_매출_금액')+number(s, '연령대_60_이상_매출_금액'))/amount*100, 1),
            status='available' if s else 'missing_sales'))
    latest = [r for r in records if r['quarter'] == quarters[-1]]
    names = Counter(r['name'] for r in latest)
    by_name = {r['name']: r['code'] for r in latest if names[r['name']] == 1}
    matches = []
    for a in anchors:
        if a.get('region') != '서울':
            continue
        name = a['adm_nm'].split()[-1]
        # SGIS and MOIS codes must never be directly joined. Only unique exact
        # Seoul-wide names are used; punctuation variants/duplicates stay pending.
        matches.append(dict(anchor_id=a['adm_cd'], name=a['adm_nm'],
                            code=by_name.get(name), method='unique_exact_name' if name in by_name else 'unmatched',
                            research_order=a.get('research_order')))
    return dict(industry=INDUSTRY, label='일반의원 시장 · 내과 전용 아님',
                quarters=quarters, latest=quarters[-1], records=records, matches=matches,
                coverage=dict(latest_shop_areas=len([r for r in latest if r['shops'] is not None]),
                              latest_sales_areas=len([r for r in latest if r['amount'] is not None]),
                              matched_anchors=sum(m['code'] is not None for m in matches),
                              seoul_anchors=len(matches)))


def main():
    paths = [ROOT/'data/manual/revenue'/f'{kind}.csv' for kind in ('sales','shops')]
    history_path = ROOT/'data/manual/revenue/sales_2024.csv'
    pop_path = ROOT/'data/manual/revenue/population.csv'
    sales, shops = (read_rows(p) for p in paths)
    history = read_rows(history_path)
    if sales.keys() & history.keys():
        raise ValueError('Historical and current sales overlap; compare revisions before merging')
    sales.update(history)
    historical_paths = [ROOT/'data/manual/revenue'/f'sales_{year}.csv' for year in (2021,2022,2023)]
    for path in historical_paths:
        earlier = read_rows(path)
        if sales.keys() & earlier.keys():
            raise ValueError('Overlapping historical sales require revision review')
        sales.update(earlier)
    population = read_population(pop_path)
    pop_history_path = ROOT/'data/manual/revenue/population_2021_2023.csv'
    earlier_population = read_population(pop_history_path)
    if population.keys() & earlier_population.keys():
        raise ValueError('Overlapping population periods require revision review')
    population.update(earlier_population)
    result = build(sales, shops, json.loads((ROOT/'web/data/research.json').read_text(encoding='utf-8'))['anchors'], population)
    result['retrieved_at'] = json.loads((ROOT/'data/manual/revenue/manifest.json').read_text())['retrieved_at']
    result['sources'] = [dict(dataset=id_, url=f'https://data.seoul.go.kr/dataList/{id_}/S/1/datasetView.do',
                             sha256=hashlib.sha256(p.read_bytes()).hexdigest())
                         for id_, p in zip(('OA-22175','OA-22172'), paths)]
    result['license'] = '공공누리 1유형 · 출처: 서울특별시 / 서울신용보증재단'
    result['sources'].extend([dict(dataset=dataset, file=p.name, sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
                                  url=f'https://data.seoul.go.kr/dataList/{dataset}/S/1/datasetView.do')
                              for dataset,p in [('OA-22175',history_path),('OA-22183',pop_path)]])
    result['extended_at'] = '2026-10-28'
    result['sources'].append(dict(dataset='OA-22183', file=pop_history_path.name,
        sha256=hashlib.sha256(pop_history_path.read_bytes()).hexdigest(),
        url='https://data.seoul.go.kr/dataList/OA-22183/S/1/datasetView.do'))
    result['sources'].extend(dict(dataset='OA-22175', file=p.name,
        sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
        url='https://data.seoul.go.kr/dataList/OA-22175/S/1/datasetView.do') for p in historical_paths)
    result['population_basis'] = '동일 분기·행정동 코드·동명 일치 상주인구. 주민의 실제 의료 소비액이나 환자당 매출이 아님.'
    result['amount_basis'] = '원자료 당월_매출_금액; 의료업종 월평균/분기합계 해석 확정 전 금액은 원자료 값으로 표시'
    (ROOT/'web/data/revenue.json').write_text(json.dumps(result, ensure_ascii=False, separators=(',',':'))+'\n', encoding='utf-8')
    print(json.dumps(result['coverage'], ensure_ascii=False))


if __name__ == '__main__':
    main()

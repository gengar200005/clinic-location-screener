"""Join existing shortlist to public administrative sales; never change model ranks."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def growth(now, before):
    if now is None or before is None or before <= 0:
        return None
    return round((now / before - 1) * 100, 2)


def summarize(records, latest):
    by_q = {r['quarter']: r for r in records}
    if len(by_q) != len(records):
        raise ValueError('Duplicate quarter for administrative code')
    current = by_q.get(latest)
    if not current:
        return {'status': 'unmatched', 'history': []}
    year, q = int(latest[:4]), int(latest[-1])
    quarters = [f'{year - (q-i-1 < 0)}{(q-i-1)%4+1}' for i in range(4)]
    previous = [str(int(x[:4])-1)+x[-1] for x in quarters]
    complete = all(x in by_q and by_q[x]['amount'] is not None for x in quarters+previous)
    trend = growth(sum(by_q[x]['amount'] for x in quarters), sum(by_q[x]['amount'] for x in previous)) if complete else None
    prior = by_q.get(f'{year-1}{q}', {})
    yoy = growth(current['amount'], prior.get('amount'))
    signal = '자료 보완'
    if yoy is not None and trend is not None:
        signal = '증가 방향 일치' if yoy > 0 and trend > 0 else '감소 방향 일치' if yoy < 0 and trend < 0 else '방향 혼재·보합'
    return {'status': 'matched', 'latest': latest, 'amount': current['amount'],
            'per_resident': current.get('per_resident'), 'population': current.get('population'),
            'shops': current.get('shops'), 'yoy': yoy, 'four_quarter_yoy': trend,
            'signal': signal, 'history': [{'quarter': k, 'amount': by_q[k]['amount']} for k in sorted(by_q)]}


def build(root=ROOT):
    sales_path = root/'web/data/revenue.json'
    site_path = root/'SITE_CANDIDATES.csv'
    sales = json.loads(sales_path.read_text(encoding='utf-8'))
    matches = {}
    for m in sales['matches']:
        if m['code']:
            if m['name'] in matches and matches[m['name']] != m['code']:
                raise ValueError('Ambiguous administrative name')
            matches[m['name']] = m['code']
    with site_path.open(encoding='utf-8-sig', newline='') as f:
        sites = [r for r in csv.DictReader(f) if r['record_type'] == '기존 조사좌표·공실 아님']
    sites.append({'zone': '수색·DMC', 'administrative_area': '서울특별시 은평구 수색동',
                  'address': '', 'visit_order': '', 'v3_joint_rank': '',
                  'classification': '추가 검증', 'access_route_status': '철길 분리 현장 관찰 · 횡단 경로 미검증'})
    allowed = ['zone','administrative_area','address','visit_order','v3_joint_rank','classification',
               'relevant_clinics_1km','endoscopy_positive_1km','unknown_specialist_clinics_1km',
               'shift_frontier_conditions','shift_conditions','access_route_status']
    result = []
    for site in sites:
        code = matches.get(site['administrative_area'])
        records = [r for r in sales['records'] if code and r['code'] == code]
        row = {k: site.get(k, '') for k in allowed}
        row['revenue_code'] = code
        row['revenue'] = summarize(records, sales['latest'])
        row['next_check'] = '이 수색동 매출은 배후 비교용. 두빌191·195 소재지는 증산동으로 상단 후속 검증표 참조. 철길 횡단지점·각 동 공실과 전용면적 확인' if site['zone']=='수색·DMC' else '주거단지→건물 실제 동선, 경쟁 내시경 운영, 전용 95~100평·임대조건 확인'
        result.append(row)
    return {'as_of':'2026-10-28', 'amount_basis':sales['amount_basis'], 'candidates':result,
            'sources': [{'file':str(p.relative_to(root)), 'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in [sales_path,site_path]],
            'limitations':['일반의원 행정동 추정매출이며 내과·후보 반경·개별 의원 매출이 아님',
                           '4개 분기 원자료 금액의 합을 전년 동일 4분기와 비교한 증감률; 연매출로 해석하지 않음',
                           '지역 경계·코드·카드 추정 방식의 연속성은 완전 검증되지 않음',
                           '미연결 지역은 0점·부적합이 아님. 경기 전체 확장 미완료',
                           '매출 방향은 검증 신호이며 추천 순위·수익성·성공확률이 아님']}


if __name__ == '__main__':
    output = build()
    (ROOT/'web/data/visit-evidence.json').write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"{len(output['candidates'])} candidates; {sum(x['revenue']['status']=='matched' for x in output['candidates'])} sales matches")

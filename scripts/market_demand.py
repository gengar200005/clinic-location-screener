"""Pilot evidence only: exact code joins, decimal arithmetic, no new score."""
import csv
import hashlib
import io
import json
import math
import re
import zipfile
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from shapely.geometry import Point, shape
from scripts.build_revenue import previous

ROOT=Path(__file__).resolve().parents[1]
CACHE=ROOT/'data/manual/market_demand_20261102'
DATASETS={'consumption':'OA-22166','workers':'OA-22184','street':'OA-22178'}
SOURCES={v:f'https://data.seoul.go.kr/dataList/{v}/S/1/datasetView.do' for v in [*DATASETS.values(),'OA-22784']}


def cell(value):
    if value is None or str(value).strip() in ('','-','NULL'):return {'state':'missing','value':None}
    if str(value).strip()=='*':return {'state':'suppressed','value':None}
    try:n=Decimal(str(value).strip())
    except InvalidOperation:return {'state':'invalid','value':None}
    if not n.is_finite() or n<0:return {'state':'invalid','value':None}
    return {'state':'observed','value':str(n)}


def dec(value):
    c=cell(value)
    return Decimal(c['value']) if c['state']=='observed' else None


def ratio(a,b,percent=False):
    a,b=dec(a),dec(b)
    if a is None or b is None or b<=0:return None
    return str((a/b*(100 if percent else 1)).quantize(Decimal('.01')))


def change(a,b):
    a,b=dec(a),dec(b)
    if a is None or b is None or b<=0:return None
    return str(((a/b-1)*100).quantize(Decimal('.01')))


def unit_change(amount, count, prior_amount, prior_count):
    a,n,pa,pn=map(dec,[amount,count,prior_amount,prior_count])
    return change(a/n,pa/pn) if all(x is not None for x in [a,n,pa,pn]) and n>0 and pn>0 else None


def index_rows(rows, industry=None):
    index={}
    for r in rows:
        if industry and r.get('서비스_업종_코드')!=industry:continue
        key=(r['행정동_코드'].strip(),r['기준_년분기_코드'].strip())
        if not re.fullmatch(r'\d{8}',key[0]) or not re.fullmatch(r'\d{4}[1-4]',key[1]):raise ValueError('Invalid code/quarter')
        if key in index:raise ValueError(f'Duplicate source identity {key}')
        index[key]=r
    return index


def read_csv(path,encoding='cp949'):
    with path.open(encoding=encoding,newline='') as f:return list(csv.DictReader(f))


def annual_context(index,code,quarter,field):
    """Calendar basis follows documented Q4 update. No qoq/yoy fabricated."""
    row=index.get((code,quarter),{})
    year,q=int(quarter[:4]),int(quarter[-1]);basis=f'{year if q==4 else year-1}4'
    b=index.get((code,basis),{})
    return dict(display_quarter=quarter,documented_update_quarter=basis,
        actual_observation_date=None,value=cell(row.get(field)),
        matches_update_quarter=(row.get(field)==b.get(field)) if row and b else None,
        timing_state='공식 연1회 설명과 원자료 반복 여부를 별도 확인',
        quarterly_growth=None,annual_growth=None,
        cadence='연1회 Q4갱신·다음해 Q1~Q3 반복. 실제 측정일 미제공')


def distributions(row,pattern):
    return {k:cell(v) for k,v in row.items() if re.search(pattern,k)}


def medical_history(raw,shops,normalized,code,name):
    history=[];seen=set()
    for record in sorted((r for r in normalized['records'] if r['code']==code),key=lambda r:r['quarter']):
        if record['quarter'] in seen:raise ValueError('Duplicate normalized medical quarter')
        seen.add(record['quarter'])
        q=record['quarter'];r=raw.get((code,q),{});prior=raw.get((code,previous(q,years=True)),{});s=shops.get((code,q),{})
        for x in [r,prior,s]:
            if x and x['행정동_코드_명']!=name:raise ValueError('Code/name changed in medical input')
        if r and dec(r['당월_매출_금액'])!=dec(record['amount']):raise ValueError('Raw/normalized medical amount mismatch')
        amount=r.get('당월_매출_금액',record['amount']);transactions=r.get('당월_매출_건수')
        # Growth uses unrounded unit amounts; displayed units may be rounded.
        unit_growth=unit_change(amount,transactions,prior.get('당월_매출_금액'),prior.get('당월_매출_건수'))
        ages=distributions(r,r'^연령대_.*_매출_금액$')
        history.append(dict(quarter=q,amount=cell(amount),transactions=cell(transactions),
            amount_yoy=change(amount,prior.get('당월_매출_금액')) if r else None,
            transactions_yoy=change(transactions,prior.get('당월_매출_건수')),
            per_transaction=ratio(amount,transactions),per_transaction_yoy=unit_growth,
            shops=cell(s.get('전체_점포_수',record.get('shops'))),
            shop_change=(dec(s.get('전체_점포_수'))-dec(shops[(code,previous(q,years=True))].get('전체_점포_수'))).__str__()
              if dec(s.get('전체_점포_수')) is not None and (code,previous(q,years=True)) in shops and dec(shops[(code,previous(q,years=True))].get('전체_점포_수')) is not None else None,
            age_amounts=ages,age_shares={k:ratio(v['value'],amount,True) for k,v in ages.items()},
            same_record_scope=bool(r),basis='일반의원 CS200006·행정동·동일 분기 원자료'))
    return history


def living_audit(pilots):
    """Inspect, do NOT collapse different administrative records at a grid/hour."""
    paths=[CACHE/'grid_centers.json',CACHE/'250_LOCAL_RESD_20261029.zip']
    if not all(p.exists() for p in paths):
        profile=dict(status='unavailable',sample_dates=[],sample_hours=None,raw_rows=None,
          extra_admin_rows_at_same_grid_hour=None,reason='생활인구 샘플 또는 격자 캐시 미확보',
          four_week_status='미수행',spatial_method='반경 집계 미수행',boundary_source='미확인',
          age_time_cross_columns=None,totals=None,means=None,unique_visitors=None)
        for p in pilots:p['living_population']=dict(profile,grid_counts={'1000':None,'1500':None})
        return profile
    grid=json.loads(paths[0].read_text(encoding='utf-8'))
    def distance(a,b,c,d):
        x,y=map(math.radians,[a,c]);dl=math.radians(d-b)
        return 6371000*2*math.asin(min(1,math.sqrt(math.sin((y-x)/2)**2+math.cos(x)*math.cos(y)*math.sin(dl/2)**2)))
    maps={p['id']:{str(radius):{g[2] for g in grid if distance(p['lat'],p['lon'],g[0],g[1])<=radius} for radius in [1000,1500]} for p in pilots}
    wanted=set().union(*(x for v in maps.values() for x in v.values()))
    original_keys=set();grid_hours=Counter();dates=set();hours=set();states=Counter();selected=0;sample_groups=defaultdict(set);observed_grids=set()
    path=CACHE/'250_LOCAL_RESD_20261029.zip'
    with zipfile.ZipFile(path) as z,io.TextIOWrapper(z.open(z.namelist()[0]),encoding='cp949',newline='') as f:
        fields=None
        for r in csv.DictReader(f):
            fields=list(r);day=r['일자'].strip();hour=r['시간'].strip();code=r['행정동코드'].strip();gid=r['250M격자'].strip()
            if not re.fullmatch(r'\d{8}',day) or not re.fullmatch(r'\d{8}',code) or not hour.isdigit() or not 0<=int(hour)<=23 or not gid:
                raise ValueError('Invalid living date/hour/code/grid key')
            key=(day,hour,code,gid)
            if key in original_keys:raise ValueError('Duplicate living full key')
            original_keys.add(key);grid_hours[(day,hour,gid)]+=1;dates.add(day);hours.add(hour);observed_grids.add(gid)
            if gid in wanted:
                selected+=1;sample_groups[gid].add(code)
                for field in fields[4:]:states[cell(r[field])['state']]+=1
    profile=dict(sample_dates=sorted(dates),sample_observation_days=len(dates),sample_hours=len(hours),raw_rows=len(original_keys),
        raw_grid_ids=len(observed_grids),unlinked_raw_grid_ids=len(observed_grids-{g[2] for g in grid}),
        extra_admin_rows_at_same_grid_hour=sum(n-1 for n in grid_hours.values()),
        selected_radius_rows=selected,cell_states=dict(states),age_time_cross_columns=bool(fields and '남자 50~54세' in fields),
        status='aggregation_blocked_admin_grid_semantics',
        reason='같은 격자·시간에 여러 행정동 행. 고유키는 일자/시간/행정동/격자. 행 합산·대표값 선택의 의미 검증 전 반경 인구 산출 보류',
        four_week_status='미수행: 하루 샘플의 행 합산 의미 검증을 통과하지 않아 확장하지 않음',
        boundary_source='공식 격자 선택기 중심점·행정동2025-06-30; 공식EPSG5179 SHP도 별도 확보',
        spatial_method='1km/1.5km 직선반경의 격자 중심점 포함 방식. 부분 교차 셀 면적배분 미적용·도보 진료권 아님',
        totals=None,means=None,unique_visitors=None)
    for p in pilots:
        p['living_population']=dict(profile,grid_counts={r:len(ids) for r,ids in maps[p['id']].items()},
            observed_grid_counts={r:len(ids & observed_grids) for r,ids in maps[p['id']].items()},
            multi_admin_grid_count=sum(len(sample_groups[g])>1 for g in maps[p['id']]['1500']))
    return profile


def build():
    pilots=json.loads((CACHE/'pilots.json').read_text(encoding='utf-8'))
    sites=json.loads((ROOT/'web/data/visit-evidence.json').read_text(encoding='utf-8'))['candidates']
    phase1=read_csv(ROOT/'reports/patient_opportunity_phase1_20261102_final/field_visit_axes.csv','utf-8-sig')
    reg={(r['anchor_id'],r['radius_m']):r for r in phase1}
    if len(reg)!=len(phase1):raise ValueError('Duplicate phase1 candidate/radius key')
    ledger=read_csv(ROOT/'SITE_CANDIDATES.csv','utf-8-sig')
    boundaries=json.loads((CACHE/'boundaries_20250630.geojson').read_text(encoding='utf-8'))['features']
    contexts={kind:index_rows(read_csv(CACHE/(dataset+'.csv'))) if (CACHE/(dataset+'.csv')).exists() else {} for kind,dataset in DATASETS.items()}
    normalized=json.loads((ROOT/'web/data/revenue.json').read_text(encoding='utf-8'))
    anchors=json.loads((ROOT/'web/data/research.json').read_text(encoding='utf-8'))['anchors']
    raw_rows=[]
    for p in sorted((ROOT/'data/manual/revenue').glob('sales*.csv')):raw_rows.extend(read_csv(p,'utf-8-sig'))
    raw=index_rows(raw_rows,'CS200006');shops=index_rows(read_csv(ROOT/'data/manual/revenue/shops.csv','utf-8-sig'),'CS200006')
    for p in pilots:
        site=next((s for s in sites if s['zone']==p['name']),None)
        code=site['revenue_code'] if site else '11380640' if p['id']=='deuxville191' else None
        expected='증산동' if p['id']=='deuxville191' else site['administrative_area'].split()[-1]
        found=[f['properties']['name'] for f in boundaries if shape(f['geometry']).covers(Point(p['lon'],p['lat']))]
        if found!=[expected]:raise ValueError('Pilot boundary/name mismatch')
        p.update(administrative_code=code,administrative_name=expected,
          administrative_fullname=site['administrative_area'] if site else '서울특별시 은평구 증산동',
          code_link='후보 실제좌표→공식2025-06-30 행정동경계명→기존 검증 코드. 원자료 동명 동시 일치 확인',
          boundary_warning='경계2025-06-30; 관측분기까지 경계 연속성 최종 미검증',
          coordinates_basis='기존 v3 조사점' if site else '두빌191 입점의원 좌표·출입구 미검증',
          demands={str(radius):reg.get((p['id'],str(radius))) for radius in [1000,1500]},building_status='공실·전용95~100평·설비 미확인. 근거 확보 후 방문',
          medical=medical_history(raw,shops,normalized,code,expected))
        old=[a for a in anchors if a['adm_nm']==p['administrative_fullname']]
        p['existing_snapshot']=dict(v7_administrative_rank=old[0]['rank'] if len(old)==1 else None,
          v3_joint_rank=site['v3_joint_rank'] if site else None,visit_order=site['visit_order'] if site else None,
          note='별개 평가대상: v7 행정동과 v3 조사점. 신규 지표로 재계산하지 않음')
        building_zones={'deuxville191':['두빌191','두빌195']}.get(p['id'],[p['name']+' 비교 확장',p['name']+' 비교권',p['name']])
        p['buildings']=[{k:r[k] for k in ['record_type','zone','address','exclusive_area_sqm','availability','source','checked_on']} for r in ledger if r['zone'] in building_zones and r['record_type']!='기존 조사좌표·공실 아님']
        for kind,index in contexts.items():
            qs=sorted(q for c,q in index if c==code)
            if not qs:
                if kind=='street':p[kind]=dict(status='unmatched',quarter=None,total=cell(None),age={},time={},weekday={},age_time_cross=None,note='원자료 미확보 또는 코드 미연결; 0점 아님')
                else:p[kind]=dict(status='unmatched',display_quarter=None,documented_update_quarter=None,actual_observation_date=None,value=cell(None),matches_update_quarter=None,medical_spending=cell(None),age={},interpretation='원자료 미확보 또는 코드 미연결; 0점 아님')
                continue
            q=qs[-1];r=index[(code,q)]
            if r['행정동_코드_명']!=expected:raise ValueError('Context code/name mismatch')
            if kind=='street':
                p[kind]=dict(status='matched',quarter=q,total=cell(r['총_유동인구_수']),
                    age=distributions(r,r'^연령대_'),time=distributions(r,r'^시간대_'),weekday=distributions(r,r'요일_'),
                    age_time_cross=None,note='행정동 생활인구 원자료 집계값. 실제 보행량·고유방문자 아님. 연령×시간 교차 미제공')
            else:
                field='지출_총금액' if kind=='consumption' else '총_직장_인구_수'
                p[kind]=dict(status='matched',**annual_context(index,code,q,field),
                    medical_spending=cell(r.get('의료비_지출_총금액')) if kind=='consumption' else None,
                    age=distributions(r,r'^연령대_') if kind=='workers' else {},
                    interpretation='상권 소비활동·주민 소득 아님. 거주지/결제지·추정대상·금액단위 세부 미확인; 금액원문만 표시' if kind=='consumption' else '소재 직장의 인구·거주인구와 합산하지 않음')
        p['judgement']=dict(before='기존 4권역 공간 사전확인 대상' if site and p['name'] not in ['까치산·강서로12길'] else '비교·조건부 대상',
            after='보류·자료 보완' if p['id']=='deuxville191' else '유지·새 지표로 순위 변경 없음',
            favorable=['주거 수요·기존 모델 안정성은 기존 자료로 비교','시장 활동과 의료 결제 구성을 분리해 확인 가능'],
            adverse=['근접 공급의 실제 검사 수용여력 미확인','적합 규모 건물 미확보'],
            unknown=['250m 반경 인구 합산 의미·4주 비교 미검증','상권 소비의 거주/결제 기준 및 금액단위 세부 미확인'],
            next_check='층·공실·전용도면 확보 → 같은 접근권의 현재 내시경/검진 운영·예약 여력 확인')
        judgement_notes={
          '숭실대입구·상도':('유지·공간 사전확인','의료결제 건수 증가가 금액 증가와 동반. 직장인구를 주거 수요와 더하지 않음. 근접 서울에이스와의 차별화는 별도 확인'),
          '구로동로·구로시장 주변':('재검토·거래 구성 확인','의료금액 증가와 거래건수 감소가 함께 나타남. 큰 주거수요·직장인구는 문맥이며 신규 여유 수요 증거 아님. 광고는 다른 위치'),
          '화곡역·화곡로':('유지·공간 사전확인','금액은 보합권이고 건수는 증가. 건당 금액 감소를 환자 감소로 바꾸지 않음. 근접 내과와 가용층 확인'),
          '서울대입구·중앙길':('유지·공간 사전확인','의료금액과 거래건수 모두 증가. 적은 직장인구가 주간 수요 부족을 입증하지 않음. 상권 유입·검진 동선 구분'),
          '까치산·강서로12길':('예비 유지·자동 상향 안 함','금액 증가보다 건수 증가가 작아 건당 금액 변화도 큼. 진료 구성·카드 추정 방식 영향 가능. 공급이 많아도 자동 탈락 안 함'),
          '수색·DMC 두빌191':('보류·공간/정의 확인','증산동 의료금액·건수 감소를 두빌이나 수색 배후의 미충족 수요로 해석하지 않음. 두 동의 공실·도면 먼저 확인')}
        p['judgement']['after'],p['judgement']['new_evidence']=judgement_notes[p['name']]
    living=living_audit(pilots)
    return dict(as_of='2026-11-02',pilots=pilots,living_audit=living,
        coverage={kind:sum(p[kind]['status']=='matched' for p in pilots) for kind in DATASETS},
        sources={**SOURCES,'OA-22175':'https://data.seoul.go.kr/dataList/OA-22175/S/1/datasetView.do','OA-22172':'https://data.seoul.go.kr/dataList/OA-22172/S/1/datasetView.do'},limitations=['새 점수·순위·예상 환자수·점유율 없음','의료매출은 일반의원 행정동 카드 기반 추정자료·공단부담금 포함 총매출 아님',
         '행정동 자료를 건물/반경 매출로 배분하지 않음','의료/상권 소비/인구 자료를 합산하지 않음','기간이 다른 자료의 상관관계·전환율 산출 없음',
         '경기 자료는 기존 미검증 상태 보존·서울과 통합 순위 비교 금지','정릉 제외·화성 제외·통근60분 러프조건 및 공간 확인 후 방문 원칙 유지'])


if __name__=='__main__':
    data=build()
    (ROOT/'web/data/market-demand.json').write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    print(dict(pilots=len(data['pilots']),coverage=data['coverage'],living=data['living_audit']['status']))

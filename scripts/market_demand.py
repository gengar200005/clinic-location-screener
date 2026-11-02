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



if __name__=='__main__':
    # The six-pilot JSON is an immutable historical snapshot. Current output has one builder.
    from scripts.build_current_recommendations import main
    main()

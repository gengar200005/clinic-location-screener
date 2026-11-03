"""Current investigation queue from cached whole-cohort evidence. No network/spatial recomputation."""
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
from shapely.geometry import Point, shape
from scripts.market_demand import read_csv, index_rows, medical_history, ROOT, CACHE, change, dec

REPORT=ROOT/'reports/recommendation_reliability_20261103'
LEGACY=ROOT.parent/'clinic-location-screener/reports/screening_v3/2026-10-23'
from scripts.recommendation_policy import as_of_date, parse_rules, premises_review
from scripts.recommendation_density import reference_weights, percentile
AS_OF=as_of_date()


def number(x):
    try:n=float(x)
    except (TypeError,ValueError):return None
    return n if np.isfinite(n) else None


def trend(history):
    """Four exact same-quarter comparisons, plus consecutive four-quarter totals."""
    indexed={r['quarter']:r for r in history}
    if len(indexed)!=len(history):raise ValueError('Duplicate medical periods')
    latest=max(indexed,default=None)
    from scripts.build_revenue import previous
    qs=[];q=latest
    for _ in range(4):
        if q is None:break
        qs.append(q);q=previous(q)
    qs=sorted(qs)
    pairs=[(indexed.get(q),indexed.get(previous(q,years=True))) for q in qs]
    valid=[(a,b) for a,b in pairs if a and b and all(dec(r[k]['value']) is not None for r in [a,b] for k in ['amount','transactions']) and dec(b['amount']['value'])>0 and dec(b['transactions']['value'])>0]
    out=dict(status='unlinked_or_insufficient',periods=qs,compared_quarters=len(valid),amount_yoy=None,transactions_yoy=None,unit_yoy=None,positive_pairs=0,negative_transaction_pairs=0,explanation='자료 미연결/정확한 대응4분기 부족. 저수요·0으로 판정하지 않음')
    if len(valid)!=4:return out
    amount=sum(dec(a['amount']['value']) for a,b in valid);prior_amount=sum(dec(b['amount']['value']) for a,b in valid)
    count=sum(dec(a['transactions']['value']) for a,b in valid);prior_count=sum(dec(b['transactions']['value']) for a,b in valid)
    pos=sum(dec(a['amount']['value'])>dec(b['amount']['value']) and dec(a['transactions']['value'])>dec(b['transactions']['value']) for a,b in valid)
    neg=sum(dec(a['transactions']['value'])<dec(b['transactions']['value']) for a,b in valid)
    # Exact matching scopes precede division; no rounded per-transaction growth.
    ay=float(change(amount,prior_amount));cy=float(change(count,prior_count))
    uy=float(change(amount/count,prior_amount/prior_count)) if count>0 and prior_count>0 else None
    status='persistent_growth' if pos>=3 and cy>0 and float(valid[-1][0]['transactions']['value'])>=float(valid[-1][1]['transactions']['value']) else 'persistent_decline' if neg>=3 and cy<0 else 'amount_only' if ay>0 and cy<=0 else 'mixed'
    notes={'persistent_growth':'4분기 중3개 이상에서 금액·거래 동반 증가. 환자 증가나 신규기관 몫을 뜻하지 않음',
      'persistent_decline':'4분기 중3개 이상 거래 감소. 미충족 수요나 신규 진입 기회로 바꾸지 않음',
      'amount_only':'4분기 금액 증가와 거래 감소/보합이 동반. 환자 수요 증가로 설명하지 않음',
      'mixed':'분기 방향이 혼재하거나 지속 증가 기준 미충족. 단일분기 급등으로 상향하지 않음'}
    return dict(out,status=status,amount_yoy=ay,transactions_yoy=cy,unit_yoy=uy,positive_pairs=int(pos),negative_transaction_pairs=int(neg),
        explanation=notes[status],low_base_or_outlier=any(abs(float(a.get('amount_yoy') or 0))>100 or abs(float(a.get('transactions_yoy') or 0))>100 for a,b in valid),
        shop_change=history[-1]['shop_change'])


def excluded(area):
    if '정릉' in area:return '개인조건 제외: 정릉'
    if not area.startswith(('서울특별시','경기도')):return '현재 서울·경기 범위 밖'
    if any(x in area for x in ['화성','만세구','효행구','병점구','동탄구']):return '개인조건 제외: 화성'
    return None


def visit_state(candidate,buildings,rules=None,as_of=None):
    rules=parse_rules(json.loads((ROOT/'config/current_recommendations.json').read_text(encoding='utf8'))) if rules is None else rules
    return premises_review(candidate,buildings,rules,as_of or AS_OF)['state']


def score_frame(frame,weight,hospital,radius,reference=None):
    weights=reference_weights(frame,reference) if reference else None
    p=frame[f'demand_{radius}']
    demand=percentile(p,weights)
    burdens=pd.concat([percentile(p/(1+frame[f'internal_{radius}']),weights),
        percentile(p/(1+frame[f'endo_{radius}']+hospital*frame[f'hospital_{radius}']),weights),
        percentile(p/(1+frame[f'gastric_{radius}']),weights)],axis=1).median(axis=1)
    return weight*demand+(1-weight)*burdens


def stable_order(frame,score,width=5):
    return frame.assign(_score=score,_band=np.floor(score/width)).sort_values(
        ['commute_lane','_band','structural','anchor_id'],ascending=[True,False,False,True],kind='stable')


def structural_score(frame,rules,weight=None,hospital=None,radius=None,reference=None):
    rules=parse_rules(rules)
    return pd.concat([score_frame(frame,rules['demand_weight'] if weight is None else weight,
        rules['hospital_burden'] if hospital is None else hospital,r,
        rules['reference_distribution'] if reference is None else reference)
        for r in (rules['radii_m'] if radius is None else [radius])],axis=1).min(axis=1)


def include_in_current_view(p):
    return bool(p.get('research_order') or p.get('old_visit_order') or p['id']=='deuxville191' or p.get('visit_state')=='현장 확인 가능')


def build(as_of=None,rules=None):
    cutoff=as_of_date(as_of)
    rules=parse_rules(rules if rules is not None else json.loads((ROOT/'config/current_recommendations.json').read_text(encoding='utf8')))
    baseline_path=REPORT/'before/reports/current_recommendations_20261102/all_candidate_decisions.csv'
    baseline=pd.read_csv(baseline_path,low_memory=False)
    prior_reps={r.area:r for r in baseline[baseline.research_order.notna()].itertuples()}
    input_paths=[]
    def parquet(name):
        path=LEGACY/name;input_paths.append(path);return pd.read_parquet(path)
    registry=parquet('commercial_node_registry.parquet');commute=parquet('full_cohort_rough_commute_worklist.parquet')
    if not registry.population_code.astype(str).str.fullmatch(r'\d{8}00').all():raise ValueError('Expected MOIS10 code ending00; no fuzzy name join')
    for f in [registry,commute]:
        if f.anchor_id.isna().any() or f.anchor_id.duplicated().any():raise ValueError('Registry/commute identity collision')
    f=registry.merge(commute[['anchor_id','car_am_minutes','car_pm_minutes','rough_commute_state']],on='anchor_id',validate='one_to_one',how='left')
    demand=parquet('metro_commercial_age_catchments.parquet');supply=parquet('commercial_node_evidence.parquet')
    for t in [demand,supply]:
        if t.duplicated(['anchor_id','radius_m']).any():raise ValueError('Duplicate radius identity')
    for radius in [1000,1500]:
        d=demand[demand.radius_m.eq(radius)][['anchor_id','comparable_population_5069_lower','comparable_population_5069_upper','total_population_uniform']].rename(columns={
            'comparable_population_5069_lower':f'demand_{radius}','comparable_population_5069_upper':f'demand_upper_{radius}','total_population_uniform':f'residents_{radius}'})
        s=supply[supply.radius_m.eq(radius)][['anchor_id','internal_specialist_clinics','egd_equipment_clinics','egd_equipment_hospitals','gastric_designation_clinics']].rename(columns={
            'internal_specialist_clinics':f'internal_{radius}','egd_equipment_clinics':f'endo_{radius}','egd_equipment_hospitals':f'hospital_{radius}','gastric_designation_clinics':f'gastric_{radius}'})
        f=f.merge(d,on='anchor_id',how='left',validate='one_to_one').merge(s,on='anchor_id',how='left',validate='one_to_one')
    if len(f)!=len(registry):raise ValueError('Cohort changed on join')
    f['region']=f.population_name.str.split().str[0].map({'서울특별시':'서울','경기도':'경기','인천광역시':'인천'})
    f['exclusion']=f.population_name.map(excluded)
    f.loc[f.rough_commute_state.eq('car_likely_over_60_transit_unverified') & f.exclusion.isna(),'exclusion']='러프 자차60분 초과·대중교통 미확인: 추천대기, 확정 통근불가 아님'
    f['commute_lane']=f.rough_commute_state.map({'car_likely_within_60_under_historical_error_band':0,'car_60min_boundary_keep_for_review':1}).fillna(2).astype(int)
    f['admin_code']=f.population_code.astype(str).str[:8]
    metrics=[f'{n}_{r}' for r in rules['radii_m'] for n in ['demand','internal','endo','hospital','gastric']]
    numeric=f[metrics].apply(pd.to_numeric,errors='coerce').replace([np.inf,-np.inf],np.nan)
    f['comparable']=numeric.notna().all(axis=1)&numeric.ge(0).all(axis=1)&f[[f'demand_{r}' for r in rules['radii_m']]].gt(0).all(axis=1)
    f[metrics]=numeric
    bounds=json.loads((CACHE/'boundaries_20250630.geojson').read_text(encoding='utf8'))['features']
    geom={x['properties']['name']:shape(x['geometry']) for x in bounds}
    normalized=json.loads((ROOT/'web/data/revenue.json').read_text(encoding='utf8'))
    raw=[]
    for p in sorted((ROOT/'data/manual/revenue').glob('sales*.csv')):input_paths.append(p);raw.extend(read_csv(p,'utf-8-sig'))
    raw=index_rows(raw,'CS200006');shops=index_rows(read_csv(ROOT/'data/manual/revenue/shops.csv','utf-8-sig'),'CS200006')
    name_sets={}
    for row in normalized['matches']:
        if row.get('code'):name_sets.setdefault(row['code'],set()).add(row['name'])
    names={code:next(iter(values)) for code,values in name_sets.items() if len(values)==1}
    contexts={kind:index_rows(read_csv(CACHE/(dataset+'.csv'))) for kind,dataset in [('consumption','OA-22166'),('workers','OA-22184'),('street','OA-22178')]}
    markets={}
    for code in sorted(set(f.loc[f.region.eq('서울'),'admin_code'])):
        name=names.get(code)
        if not name:continue
        short=name.split()[-1]
        hist=medical_history(raw,shops,normalized,code,short)
        from calendar import monthrange
        def available_quarter(q):
            month=int(q[-1])*3;year=int(q[:4])
            return f'{year:04d}-{month:02d}-{monthrange(year,month)[1]:02d}'<=cutoff
        hist=[h for h in hist if available_quarter(h['quarter'])]
        if not hist:continue
        context={}
        for kind,ix in contexts.items():
            qs=sorted(q for c,q in ix if c==code and available_quarter(q));q=qs[-1] if qs else None;row=ix.get((code,q),{})
            if row and row['행정동_코드_명']!=short:raise ValueError('Context code/name inconsistency')
            field={'consumption':'지출_총금액','workers':'총_직장_인구_수','street':'총_유동인구_수'}[kind]
            context[kind]=dict(period=q,value=row.get(field),fields={k:v for k,v in row.items() if k not in ['기준_년분기_코드','행정동_코드','행정동_코드_명']},status='참고·평가미반영')
        latest=hist[-1]
        markets[code]=dict(area=name,trend=trend(hist),history=[{k:r[k] for k in ['quarter','amount','transactions','per_transaction','shops']} for r in hist],
            latest=latest,context=context,scope='행정동 일반의원 CS200006 카드 추정. 건물/반경/내과 단독 매출 아님')
    def link(row):
        m=markets.get(row.admin_code);short=row.population_name.split()[-1]
        if row.region!='서울':return 'gyeonggi_unverified' if row.region=='경기' else 'outside_current_scope'
        if not m:return 'no_unique_compatible_admin_code'
        if m['area']!=row.population_name:return 'admin_name_not_exact'
        if short not in geom:return 'boundary_missing'
        return 'matched' if geom[short].covers(Point(row.lon,row.lat)) else 'coordinate_boundary_mismatch'
    f['market_link_state']=f.apply(link,axis=1)
    f['market_linked']=f.market_link_state.eq('matched')
    f['market_state']=[markets[c]['trend']['status'] if matched else 'unlinked_or_insufficient' for c,matched in zip(f.admin_code,f.market_linked)]
    f['trend_delta']=np.nan
    for idx,row in f.iterrows():
        if row.region=='서울' and row.market_linked and row.market_state in rules['trend_adjustment']:f.at[idx,'trend_delta']=rules['trend_adjustment'][row.market_state]
    # Gyeonggi uses ONLY the shared supply/demand comparison; no invented market value.
    full=[];variants={};group_reports=[]
    for region in ['서울','경기']:
        group=f[f.region.eq(region)&f.exclusion.isna()&f.comparable].copy()
        group['structural']=structural_score(group,rules)
        adjustments=group.trend_delta.fillna(rules['unknown_trend_investigation_interval'][0]) if region=='서울' else pd.Series(0,index=group.index)
        group['decision_lower']=group.structural+adjustments
        group['decision_upper']=group.structural+(group.trend_delta.fillna(rules['unknown_trend_investigation_interval'][1]) if region=='서울' else 0)
        ordered=stable_order(group,group.decision_lower,rules['priority_band_width'])
        ordered['coordinate_order']=range(1,len(ordered)+1)
        representatives=ordered.drop_duplicates('admin_code').copy();representatives['research_order']=range(1,len(representatives)+1)
        reps=dict(zip(representatives.anchor_id,representatives.research_order))
        alt_ranks={}
        for weight in rules['alternative_demand_weights']:
            for h in rules['hospital_burdens']:
                for radius in rules['radii_m']:
                    score=structural_score(group,rules,weight,h,radius)+adjustments
                    alt=stable_order(group,score,rules['priority_band_width']).drop_duplicates('admin_code')
                    # Rank each current representative's area, not an unrelated replacement point.
                    alt_ranks[f'w{weight}_h{h}_r{radius}']={c:i+1 for i,c in enumerate(alt.admin_code)}
        for size in rules['reference_distribution']['alternative_cell_sizes_m']:
            for dx in rules['reference_distribution']['alternative_offset_fractions']:
                for dy in rules['reference_distribution']['alternative_offset_fractions']:
                    spec=dict(rules['reference_distribution'],cell_size_m=size,offset_m=[dx*size,dy*size])
                    structural=structural_score(group,rules,reference=spec)
                    alt=stable_order(group.assign(structural=structural),structural+adjustments,rules['priority_band_width']).drop_duplicates('admin_code')
                    alt_ranks[f'grid{size}_offset{dx}_{dy}']={c:i+1 for i,c in enumerate(alt.admin_code)}
        if region=='서울':
            optimistic=stable_order(group,group.structural+group.trend_delta.fillna(rules['unknown_trend_investigation_interval'][1]),rules['priority_band_width']).drop_duplicates('admin_code')
            alt_ranks['unknown_trend_optimistic']={c:i+1 for i,c in enumerate(optimistic.admin_code)}
        for idx,r in ordered.iterrows():
            vals=[v[r.admin_code] for v in alt_ranks.values()]
            r['research_order']=reps.get(r.anchor_id);r['sensitivity_rank_low']=min(vals);r['sensitivity_rank_high']=max(vals)
            r['representative']=r.anchor_id in reps
            full.append(r.to_dict())
        variants[region]=alt_ranks
        group_reports.append(dict(region=region,compared_coordinates=len(group),administrative_bundles=len(representatives),market_linked=int(group.market_linked.sum()),four_quarter_comparable=int(group.market_state.ne('unlinked_or_insufficient').sum())))
    ranked=pd.DataFrame(full);result=f.merge(ranked[['anchor_id','structural','decision_lower','decision_upper','coordinate_order','research_order','representative','sensitivity_rank_low','sensitivity_rank_high']],on='anchor_id',how='left',validate='one_to_one')
    representative_ids=dict(zip(ranked.loc[ranked.research_order.notna(),'admin_code'],ranked.loc[ranked.research_order.notna(),'anchor_id']))
    prior=pd.read_csv(ROOT/'reports/patient_opportunity_phase1_20261102_final/field_visit_axes.csv',dtype={'anchor_id':str});prior=prior[prior.radius_m.eq(1000)]
    old={r.anchor_id:r for r in prior.itertuples()};old_areas={r.population_name:r for r in prior.itertuples()}
    selected_areas=set(ranked.loc[ranked.research_order.notna() & ((ranked.region.eq('서울') & ranked.research_order.le(rules['seoul_shortlist'])) | (ranked.region.eq('경기') & ranked.research_order.le(rules['gyeonggi_shortlist']))),'population_name'])
    building_data=json.loads((ROOT/'data/manual/current_building_verifications.json').read_text(encoding='utf8'))['records']
    policy_path=ROOT/'reports/field_visit_phase2_20261102/LATEST_VISIT_POLICY.json'
    policy=json.loads(policy_path.read_text(encoding='utf8'))
    ledger=pd.read_csv(ROOT/'SITE_CANDIDATES.csv').replace({np.nan:None})
    records=[]
    for row in result.to_dict('records'):
        p=dict(id=row['anchor_id'],area=row['population_name'],zone=row['address'],lat=row['lat'],lon=row['lon'],region=row['region'],admin_code=row['admin_code'],
            research_order=number(row.get('research_order')),coordinate_order=number(row.get('coordinate_order')),representative=row.get('representative') is True,
            structural=number(row.get('structural')),decision_interval=[number(row.get('decision_lower')),number(row.get('decision_upper'))],
            sensitivity=[number(row.get('sensitivity_rank_low')),number(row.get('sensitivity_rank_high'))],commute_lane=row['commute_lane'],
            commute=dict(state=row['rough_commute_state'],am=number(row['car_am_minutes']),pm=number(row['car_pm_minutes']),definition='과거 경로의 공간 보간·현재 실측 아님'),
            comparable=bool(row['comparable']),exclusion=row['exclusion'] if isinstance(row['exclusion'],str) else None,
            coordinate_identity=row['building_identity_state'],coordinate_spread_m=number(row['coordinate_spread_bbox_m']),
            market_code=row['admin_code'] if row['market_linked'] else None,market_state=row['market_state'],market_link_state=row['market_link_state'],
            evidence={str(radius):{n:number(row[f'{n}_{radius}']) for n in ['demand','demand_upper','residents','internal','endo','hospital','gastric']} for radius in [1000,1500]})
        old_point=old.get(p['id']);old_area=old_areas.get(p['area'])
        p['representative_id']=representative_ids.get(p['admin_code'])
        previous=prior_reps.get(p['area'])
        p['density_change']=dict(previous_area_order=number(previous.research_order) if previous else None,
            previous_representative_id=previous.id if previous else None,
            representative_changed=bool(previous and p['representative_id']!=previous.id),
            interpretation='같은 시장자료·공통 설정에서 비교기준 보정. 행정동 조사순서이며 특정 건물 안정성이 아님')
        p['name']=old_point.zone if old_point else f"{p['area'].split()[-1]} · {p['zone'].split()[-2] if len(p['zone'].split())>2 else '조사점'}"
        p['old_visit_order']=int(old_point.visit_order) if old_point else None
        p['old_area_visit_order']=int(old_area.visit_order) if old_area else None
        p['change']='개인조건/범위 제외' if p['exclusion'] else '기존 조사점·현재 대표' if old_point and p['research_order'] else '같은 행정동의 대표 조사점 변경' if old_area and p['research_order'] else '신규 조사권' if p['research_order'] else '비대표 좌표·대안 보존'
        was_primary=old_area is not None and int(old_area.visit_order) in [1,2,4,5]
        p['history_change']='개인조건/범위 제외' if p['exclusion'] else '유지' if old_area and was_primary and p['area'] in selected_areas else '상향·예비에서 우선군' if old_area and p['area'] in selected_areas else '하향·현재 우선군 밖' if old_area and was_primary else '예비·비교 유지' if old_area else '신규' if p['area'] in selected_areas else '비교 보존'
        p['manual_opinion']=dict(recorded_on=policy['recorded_on'],source='기존 임장2단계·SITE_CANDIDATES.csv',input_version=hashlib.sha256((ROOT/'SITE_CANDIDATES.csv').read_bytes()).hexdigest()[:12],requires_review=True,reason='새 전체비교 결과와 실제 공간 조건에 대해 수동 의견 재확인 필요') if old_area else None
        p['buildings']=[{k:b[k] for k in ['zone','address','record_type','exclusive_area_sqm','availability','checked_on','source']} for b in ledger.to_dict('records') if old_area and str(b['zone']).startswith(old_area.zone) and b['record_type']!='기존 조사좌표·공실 아님']
        p['building_review']=premises_review(p,building_data,rules,cutoff)
        p['visit_state']=p['building_review']['state']
        m=markets.get(p['market_code']);t=m['trend'] if m else None
        p['reasons']=[f"동일 {p['region']} 비교군의 {rules['radii_m']}m 수요·공급 비교",f"50~69세 하한 추정 {round(p['evidence']['1000']['demand']):,}명·관측 내과 {p['evidence']['1000']['internal']}곳, 내시경 {p['evidence']['1000']['endo']}곳"] if p['comparable'] else ['공통 지표 부족·수요0으로 판정하지 않음']
        p['reasons'].append(t['explanation'] if t else '의료소비 미연결/비교보류. 공통 수요·공급 비교는 유지')
        p['counter_evidence']=['실제 검사 가동률·예약·FTE·보행 장벽 미확인','전용95~100평 공실·설비 미확인']
        if p['coordinate_identity']!='reported_building_id':p['counter_evidence'].append('원본 상가의 건물 식별번호 미연결: 주소·조사좌표부터 재확인. 실제 임대건물 확인 아님')
        if p['coordinate_spread_m'] is not None and p['coordinate_spread_m']>200:p['counter_evidence'].append('원본 좌표 분산200m 초과: 후보 위치 재확인')
        context_idx=1 if '월계' in p['area'] else 2 if any(x in p['area'] for x in ['청량리','답십리']) else 0 if '수색' in p['area'] else None
        p['manual_visit_context']=None
        if context_idx is not None:
            p['manual_visit_context']=dict(policy['visited_areas'][context_idx],source=policy['source'],recorded_on=policy['recorded_on'],match_scope='동명 키워드 참고·정확 임장 경계 중첩 미검증',automatic_score_effect=None)
            p['counter_evidence'].append('사용자 과거 인근 임장에서 적정 규모 공간 미확보. 정확 중첩 미확인·새 공간 근거 없이 반복 방문하지 않음')
        if t and t.get('low_base_or_outlier'):p['counter_evidence'].append('4분기 중 큰 증감: 낮은 기준값/분류·기관 변화 확인 필요')
        p['next_check']='층·현 공실·전용95~100평 도면 확보 → 내시경/초음파/회복·확장 구성·승강기·주차 → 실제 경쟁 운영·통근·보행 동선'
        if p['visit_state']=='현장 확인 가능':
            p['counter_evidence']=[x for x in p['counter_evidence'] if x!='전용95~100평 공실·설비 미확인']+['공간 사전확인 완료·현장 임상 동선/설비/개설 용도는 별도 검증']
            p['next_check']='사전 확보한 층·전용 도면을 들고 현장 확인: 임상 동선·승강기·주차·승하차·환자 접근·용도·설비. 개원 적합 확정 아님'
        records.append(p)
    # A concrete visited building is preserved as a separate non-ranked case, not made a synthetic whole-cohort observation.
    records.append(dict(id='deuxville191',name='수색·DMC 두빌191',area='서울특별시 은평구 증산동',zone='두빌191·195는 별동',lat=37.5782607,lon=126.9010885,region='서울',admin_code='11380640',
        research_order=None,coordinate_order=None,representative=False,structural=None,decision_interval=[None,None],sensitivity=[None,None],commute_lane=2,commute={'state':'미확인','am':None,'pm':None},
        comparable=False,exclusion=None,market_code='11380640',market_state=markets.get('11380640',{}).get('trend',{}).get('status','unlinked_or_insufficient'),evidence={},old_visit_order=None,old_area_visit_order=None,
        change='수동 방문 사례·순위 비교 불가',manual_revisit=True,manual_opinion={'recorded_on':policy['recorded_on'],'source':'사용자 직접 임장·LATEST_VISIT_POLICY.json','requires_review':True},
        buildings=[{k:b[k] for k in ['zone','address','record_type','exclusive_area_sqm','availability','checked_on','source']} for b in ledger.to_dict('records') if str(b['zone']).startswith('두빌')],
        visit_state='새 정보 확보 후 재방문',reasons=['철길 양측의 실제 진료권 미검증. 증산동 매출을 수색 배후에 배분하지 않음'],counter_evidence=['현재 공실·각동 전용95~100평 미확인'],next_check='두빌191·195 각동 층·공실·전용도면 확보 후 재방문'))
    records[-1]['building_review']=premises_review(records[-1],building_data,rules,cutoff)
    records[-1]['visit_state']=records[-1]['building_review']['state']
    coverage=[]
    for region,g in result.groupby('region'):
        coverage.append(dict(region=region,registry_coordinates=len(g),personal_or_scope_excluded=int(g.exclusion.notna().sum()),comparable_common=int((g.comparable & g.exclusion.isna()).sum()),
            common_unlinked_eligible=int((~g.comparable & g.exclusion.isna()).sum()),exclusion_reasons=g.exclusion.dropna().value_counts().to_dict(),
            market_link_failure_reasons=g.loc[~g.market_linked,'market_link_state'].value_counts().to_dict(),
            market_linked=int(g.market_linked.sum()),unlinked_market=int((~g.market_linked).sum()),commute_unknown=int(g.commute_lane.eq(2).sum())))
    extra=[ROOT/'scripts/build_current_recommendations.py',ROOT/'scripts/market_demand.py',ROOT/'scripts/recommendation_policy.py',ROOT/'scripts/recommendation_density.py',ROOT/'config/current_recommendations.json',ROOT/'web/data/revenue.json',ROOT/'data/manual/current_building_verifications.json',ROOT/'SITE_CANDIDATES.csv',policy_path,ROOT/'reports/patient_opportunity_phase1_20261102_final/field_visit_axes.csv',CACHE/'boundaries_20250630.geojson',ROOT/'data/manual/revenue/shops.csv']+[CACHE/(x+'.csv') for x in ['OA-22166','OA-22178','OA-22184']]
    inputs={str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else 'legacy-v3/'+p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in input_paths+extra+[baseline_path]}
    version=hashlib.sha256(json.dumps(dict(inputs=inputs,as_of=cutoff,rules=rules),sort_keys=True).encode()).hexdigest()[:12]
    # All rows are exported for reproducible full-cohort audit. Web uses all representatives, old points and scope exclusions.
    public=[p for p in records if include_in_current_view(p)]
    data=dict(version=version,as_of=cutoff,source_main='d122429',reference_definition=('공간 셀당 총가중치1·셀 내 고유 근거서명 균등·가중 중간누적 백분위. 모든 후보 보존' if rules['reference_distribution']['mode']=='spatial_cells' else '보정 전 좌표별 pandas 평균순위/N 백분위'),historical_replay_limit='기준일 이후 건물 확인정보는 미사용. 원자료 수집시점 전체 이력은 없어 과거 당시 알려진 시장정보의 완전한 복원은 아님',collection_dates={'market_context':'2026-11-02 로컬 수집 기록·원자료별 전체 수집 이력 없음','medical':'기존 원자료 재사용·정확한 최초 수집일 미확인','building':'각 항목 checked_on 별도'},rules=rules,coverage=coverage,comparison_groups=group_reports,candidates=public,markets=markets,
        periods={'residents':'2026-09 면적배분/연령추정','providers':'인력2026-09·장비2025-12 신고 스냅샷','medical':'2021Q1~2026Q2; 최근4분기2025Q3~2026Q2','boundary':'2025-06-30 공식경계: 관측분기까지 연속성 최종 미검증'},
        limitations=['행정동별 조사 묶음은 실제 생활권이 아님. 철길·하천·대로 양측을 같은 시장으로 단정하지 않음','공식 점수·순위 보존. 새 값은 조사 배분 가정이며 예상 환자수·성공률 아님','소비·직장·250m 생활인구는 미검증 정의/갱신 때문에 평가 미반영','경기 의료매출 비교보류·서울과 같은 순위로 합치지 않음','미연결 추세는 [-5,+5] 조사 가정 범위로 표시·하한으로 순서 결정. 관측값0 대체가 아님','구조지표의1+기관 분모와 병원1/2/3은 조사용 가정·검사 처리능력 아님'])
    return data,records,inputs,variants


def main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--as-of',type=as_of_date)
    parser.add_argument('--config',type=Path,default=ROOT/'config/current_recommendations.json')
    args=parser.parse_args()
    data,records,inputs,variants=build(args.as_of,json.loads(args.config.read_text(encoding='utf8')));REPORT.mkdir(exist_ok=True)
    (ROOT/'web/data/current-recommendations.json').write_text(json.dumps(data,ensure_ascii=False,allow_nan=False,separators=(',',':')),encoding='utf8')
    flat=[dict({k:v for k,v in p.items() if not isinstance(v,(dict,list))},**{f'{n}_{radius}':v for radius,e in p['evidence'].items() for n,v in e.items()}) for p in records]
    pd.DataFrame(flat).to_csv(REPORT/'all_candidate_decisions.csv',index=False,encoding='utf-8-sig')
    (ROOT/'web/data/current-inventory.csv').write_bytes((REPORT/'all_candidate_decisions.csv').read_bytes())
    pd.DataFrame([p for p in flat if p['research_order'] or p['old_visit_order']]).to_csv(REPORT/'comparison.csv',index=False,encoding='utf-8-sig')
    (REPORT/'input_hashes.json').write_text(json.dumps(inputs,ensure_ascii=False,indent=2),encoding='utf8')
    (REPORT/'sensitivity.json').write_text(json.dumps(variants,ensure_ascii=False),encoding='utf8')
    print(json.dumps(dict(version=data['version'],coverage=data['coverage'],groups=data['comparison_groups'],web_candidates=len(data['candidates'])),ensure_ascii=False))


if __name__=='__main__':main()

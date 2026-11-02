"""Read existing outputs only. Diagnostic demand order is NOT a recommendation."""
import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]


def unique(frame, keys):
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError(f'Ambiguous identity: {keys}')


def demand_order(frame, column):
    values=pd.to_numeric(frame[column],errors='coerce')
    return values.where(values.gt(0)).rank(ascending=False,method='min')


def compare(old_ids, diagnostic_ids):
    a,b=set(old_ids),set(diagnostic_ids)
    return dict(old=len(a),diagnostic=len(b),retained=len(a&b),entered=len(b-a),departed=len(a-b))


def run(legacy, output):
    output.mkdir(parents=True,exist_ok=False)
    sources={}
    def read(path):
        sources[str(path.resolve())]=hashlib.sha256(path.read_bytes()).hexdigest()
        return pd.read_parquet(path)
    revenue_path=ROOT/'web/data/revenue.json'
    sources[str(revenue_path.resolve())]=hashlib.sha256(revenue_path.read_bytes()).hexdigest()
    revenue=json.loads(revenue_path.read_text(encoding='utf-8'))
    codes={}
    for r in revenue['matches']:
        if not r.get('code'):continue
        if r['name'] in codes and codes[r['name']]!=r['code']:raise ValueError('Ambiguous revenue identity')
        codes[r['name']]=r['code']
    observations={r['code']:r for r in revenue['records'] if r['quarter']==revenue['latest']}
    def market(names):
        result=[]
        for name in names:
            code=codes.get(name);obs=observations.get(code,{})
            result.append(dict(market_code=code,market_period=revenue['latest'] if code else None,
                market_amount=obs.get('amount'),market_yoy=obs.get('yoy'),
                market_scope='행정동 일반의원 원자료; 후보 반경·내과 매출 아님' if code else '서울 일반의원 자료 미연결; 경기 과거 의료기관 자료와 별도',
                market_role='시장활동 참고. 포화/미충족 수요/신규 의원 몫을 뜻하지 않음'))
        return pd.DataFrame(result,index=names.index)
    score_path=max((ROOT/'data/scored').glob('scores_*.parquet'))
    v7=read(score_path);unique(v7,['adm_cd'])
    v7=pd.concat([v7,market(v7.adm_nm)],axis=1)
    v7['demand_only_diagnostic_rank']=demand_order(v7,'catchment_pop_5069')
    v7['existing_population_component_rank']=demand_order(v7,'p_norm')
    v7['diagnostic_position_change']=v7['rank']-v7['demand_only_diagnostic_rank']
    v7['reason']='연령대 배후인구만 비교. 기존 경쟁/검진 공급·인구 추세 항을 사용하지 않는 설명용 순서. 성공순위 아님.'
    v7['entry_opportunity']='판단 보류: 기존 기관 가동률·예약 대기·환자 선택·유효 면적 미확인'
    v7cols=['adm_cd','adm_nm','rank','score','demand_only_diagnostic_rank','existing_population_component_rank','diagnostic_position_change',
        'pop_total','pop_5069','catchment_pop_1_5km','catchment_pop_5069','trend_5069_3y',
        'n_clinic_med','n_doctors_med','n_doctors_med_weighted','n_doctors_within_radius_med',
        'n_endo_ck_clinic','n_endo_ck_hosp','c_norm','p_norm','reason','entry_opportunity',
        'market_code','market_period','market_amount','market_yoy','market_scope','market_role']
    v7[v7cols].to_csv(output/'v7_all_axes.csv',index=False,encoding='utf-8-sig')
    v7.loc[(v7['rank']<=30)|(v7.demand_only_diagnostic_rank<=30),v7cols].to_csv(output/'v7_top30_diagnostic_comparison.csv',index=False,encoding='utf-8-sig')
    src=legacy/'reports/screening_v3/2026-10-23'
    existing=ROOT/'reports/integration_20261028'
    prior=read(src/'commute_priority_all.parquet');prior=prior[prior.cohort.eq('primary')].copy();unique(prior,['anchor_id'])
    demand=read(src/'metro_commercial_age_catchments.parquet');unique(demand,['anchor_id','radius_m'])
    supply=read(src/'commercial_node_evidence.parquet');unique(supply,['anchor_id','radius_m'])
    diagnostic=read(existing/'diagnostic_priorities.parquet');unique(diagnostic,['mode','anchor_id'])
    modes=diagnostic.pivot(index='anchor_id',columns='mode',values='conditional_priority_rank').add_prefix('existing_rank_').reset_index()
    rows=[];summaries=[]
    for radius in [1000,1500]:
        f=prior.merge(demand[demand.radius_m.eq(radius)],on='anchor_id',validate='one_to_one').merge(
            supply[supply.radius_m.eq(radius)][['anchor_id','internal_specialist_clinics','general_or_internal_clinics',
            'egd_equipment_clinics','egd_equipment_hospitals','gastric_designation_clinics','medical_clinics_nhis_unlinked']],
            on='anchor_id',validate='one_to_one').merge(modes,on='anchor_id',validate='one_to_one')
        if len(f)!=len(prior):raise ValueError('Cohort shrank during join')
        f['demand_only_diagnostic_rank']=demand_order(f,'comparable_population_5069_lower')
        f['diagnostic_position_change']=f.conditional_priority_rank-f.demand_only_diagnostic_rank
        f['entry_opportunity']='판단 보류: 순수 신규 환자 유입/기관 가동률 자료 없음'
        f=pd.concat([f,market(f.population_name)],axis=1)
        f['personal_scope']=''
        f.loc[f.population_name.str.contains('정릉',na=False),'personal_scope']='정릉 개인 제외'
        f.loc[f.population_name.str.contains('화성',na=False),'personal_scope']='화성 제외'
        f.loc[f.population_name.str.startswith('인천',na=False),'personal_scope']='현재 서울·경기 범위 밖'
        summaries.append(dict(radius_m=radius,cohort=len(f),**compare(f.loc[f.top30_including_boundary_ties,'anchor_id'],f.loc[f.demand_only_diagnostic_rank<=30,'anchor_id'])))
        rows.append(f)
    v3=pd.concat(rows,ignore_index=True)
    cols=['anchor_id','name','population_name','address','lat','lon','radius_m','conditional_priority_rank',
        'demand_only_diagnostic_rank','diagnostic_position_change','frontier_conditions','scenarios',
        'total_population_uniform','comparable_population_5069_lower','comparable_population_5069_upper',
        'internal_specialist_clinics','general_or_internal_clinics','egd_equipment_clinics','egd_equipment_hospitals',
        'gastric_designation_clinics','medical_clinics_nhis_unlinked','existing_rank_without_age',
        'existing_rank_without_hospital','existing_rank_without_outpatient','personal_scope','entry_opportunity',
        'market_code','market_period','market_amount','market_yoy','market_scope','market_role']
    v3[cols].to_csv(output/'v3_all_axes.csv',index=False,encoding='utf-8-sig')
    v3.loc[(v3.conditional_priority_rank<=30)|(v3.demand_only_diagnostic_rank<=30),cols].to_csv(output/'v3_priority_diagnostic_comparison.csv',index=False,encoding='utf-8-sig')
    visits_path=src/'field_visit_shortlist.json'
    sources[str(visits_path.resolve())]=hashlib.sha256(visits_path.read_bytes()).hexdigest()
    visits=pd.DataFrame(json.loads(visits_path.read_text(encoding='utf-8'))['visits'])[['anchor_id','zone','visit_order']]
    questions={1: '서울에이스내과 실제 검진 수용·대기와 주거 출입구 동선, 신규기관 차별화·95~100평 도면',
     2: '검진기관별 현재 예약/검사 운영규모 및 구로 광고건물 현공실·전용면적·접근 동선. 기존좌표와 광고위치 혼용 금지',
     3: '정릉 개인선택 제외. 현재 신규 내시경 공급 근거는 비교 기록으로 보존',
     4: '진료권별 실제 검진 공급·주거 유입과 가능한 건물·임차고정비 확인',
     5: '큰 상권의 유입과 거주수요 구분, 검진기관 예약/검사실 규모·건물 확인',
     6: '금액증가45.1%·결제건수증가11.3%를 환자증가로 해석하지 않고 서비스구성·근접공급 가동여력 확인',
     7: '일반의원 매출미연결을 낮은수요로 취급하지 않고 접근권·전용면적·실제예약 확인',
     8: '현재 서울·경기 범위 밖. 재추천 없이 기존자료 보존',
     9: '주거단지출입구·실제 경쟁 검진서비스·95~100평 건물 확인',
     10: '일반상권 유입과 검진목적 방문 구분·경쟁서비스 가동여력·건물 확인'}
    visit_axes=visits.merge(v3[cols],on='anchor_id',validate='one_to_many')
    visit_axes['next_evidence']=visit_axes.visit_order.map(questions)
    visit_axes['change_reason']='단일 수요규모 순서와 기존 다목적 구간/공급 비교의 목적 차이. 공급능력·진입기회 검증으로 해소해야 함'
    visit_axes.to_csv(output/'field_visit_axes.csv',index=False,encoding='utf-8-sig')
    summary=dict(v7_source=score_path.name,v7_rows=len(v7),v7_demand_diagnostic=compare(v7.loc[v7['rank']<=30,'adm_cd'],v7.loc[v7.demand_only_diagnostic_rank<=30,'adm_cd']),
        v7_population_component_diagnostic=compare(v7.loc[v7['rank']<=30,'adm_cd'],v7.loc[v7.existing_population_component_rank<=30,'adm_cd']),
        v3_rows=len(prior),v3_radius_comparisons=summaries,
        note='수요 규모만 비교한 설명용 진단이며 신규 진입 성공순위가 아님. 기존 8시나리오 재계산 없음.')
    (output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    after={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in sources}
    if sources!=after:raise ValueError('Source changed during analysis')
    (output/'input_hashes.json').write_text(json.dumps(sources,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--legacy',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();run(a.legacy,a.out)

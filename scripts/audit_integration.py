"""Read-only analysis of both local generations; write only a separate report tree.

Run: python -B -m scripts.audit_integration --legacy-root ../clinic-location-screener
Never invokes source run() functions, collectors, publishers or Git operations.
"""
from pathlib import Path
import argparse
import hashlib
import json
import sys
import numpy as np
import pandas as pd
from scoring.competition import compute_competition_raw
from scoring.weighted_sum import compute_final_scores

ROOT = Path(__file__).resolve().parents[1]


def unique(frame, keys):
    if frame[keys].isna().any().any() or frame.duplicated(keys).any():
        raise ValueError(f'Invalid identity: {keys}')


def membership_comparison(old, new):
    """Outer join so new entrants and departures cannot silently disappear."""
    unique(old, ['anchor_id']); unique(new, ['anchor_id'])
    return old.merge(new, on='anchor_id', how='outer', validate='one_to_one', suffixes=('_old', '_new'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--legacy-root', type=Path, required=True)
    args = parser.parse_args()
    legacy = args.legacy_root.resolve()
    source = legacy/'reports/screening_v3/2026-10-23'
    out = ROOT/'reports/integration_20261028'
    out.mkdir(parents=True, exist_ok=True)
    sys.path.append(str(legacy))
    from screening_v3.commercial_comparison import blocked_dominance
    from screening_v3.commute_priority import priorities
    from screening_v3.anchor_sensitivity import OUTPATIENT, ENDOSCOPY
    evidence = {}
    def read(path):
        evidence[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        return pd.read_parquet(path)
    def save(name, value):
        (out/name).write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    # Phase 0: replay published numeric inputs, not a collector run with new dates.
    latest = max((ROOT/'data/scored').glob('scores_*.parquet'))
    old = read(latest)
    unique(old, ['adm_cd'])
    comp = compute_competition_raw(
        old[['adm_cd','n_doctors_med_weighted']].rename(columns={'n_doctors_med_weighted':'n_clinic'}),
        old[['adm_cd','n_doctors_within_radius_med']].rename(columns={'n_doctors_within_radius_med':'n_within_radius'}),
        old[['adm_cd','catchment_pop_5069']].rename(columns={'catchment_pop_5069':'population'}),
        old[['adm_cd','n_doctors_station_500m_med']],old[['adm_cd','n_doctors_subcluster_max_med']])
    replay = compute_final_scores(old.drop(columns=['c_raw']).merge(comp[['adm_cd','c_raw']], on='adm_cd',validate='one_to_one'))
    compare=old[['adm_cd','adm_nm','rank','score','c_raw']].merge(replay[['adm_cd','rank','score','c_raw']],on='adm_cd',suffixes=('_old','_new'),validate='one_to_one')
    compare['rank_change']=compare['rank_old']-compare['rank_new']
    compare['reason']='입력 검증 강화; 유효한 입력의 공식과 가중치는 보존'
    compare['reliability']='역사적 입력 재계산; 현장 성공 검증 아님'
    compare.to_csv(out/'v7_all_comparison.csv',index=False,encoding='utf-8-sig')
    compare[(compare.rank_old<=30)|(compare.rank_new<=30)].to_csv(out/'v7_top30_comparison.csv',index=False,encoding='utf-8-sig')
    topfile=latest.with_name(latest.name.replace('scores_', 'top30_'))
    stored_top=read(topfile)
    p0=dict(source=str(latest),rows=len(old),top30_matches_full=stored_top.adm_cd.tolist()==old.sort_values('rank').head(30).adm_cd.tolist(),
        max_competition_error=float((compare.c_raw_old-compare.c_raw_new).abs().max()),max_score_error=float((compare.score_old-compare.score_new).abs().max()),
        rank_changes=int(compare.rank_change.ne(0).sum()),invalid_population_rows=int((old.catchment_pop_5069.isna()|old.catchment_pop_5069.le(0)).sum()),
        missing_trend=int(old.trend_5069_3y.isna().sum()),jungneung=compare[compare.adm_nm.str.contains('정릉2동')].to_dict('records'),
        limitation='Published-input replay only; full raw-to-score collection is not rerun. Original dates remain unchanged.')
    save('phase0.json',p0)
    print('Phase 0',json.dumps(p0,ensure_ascii=False),flush=True)

    # Phase 1: observe missingness; provider absence is not inferred from missing fields.
    providers=read(source/'provider_checkup_evidence.parquet');unique(providers,['provider_id'])
    supply=read(source/'commercial_node_evidence.parquet');unique(supply,['anchor_id','radius_m'])
    demand=read(source/'metro_commercial_age_catchments.parquet');unique(demand,['anchor_id','radius_m'])
    rows=[]
    for region,g in providers[providers.is_clinic].groupby('region'):
        row=dict(region=region,clinics=len(g),valid_coordinates=int(g.coord_valid.sum()))
        for col in ['internal_specialists','total_doctors','ultrasound_devices_reported']:
            row[col+'_missing']=int(g[col].isna().sum());row[col+'_missing_pct']=round(g[col].isna().mean()*100,2)
        row['equipment_absence_confirmed']=int(g.equipment_absence_confirmed.sum())
        rows.append(row)
    save('phase1_provider_coverage.json',rows)

    # Phase 2: re-run the actual eight scenarios on the latest primary cohort.
    all_prior=read(source/'commute_priority_all.parquet');unique(all_prior,['cohort','anchor_id'])
    prior=all_prior[all_prior.cohort.eq('primary')].copy()
    cohort=sorted(prior.anchor_id)
    frames={r:demand[demand.radius_m.eq(r)].merge(supply,on=['anchor_id','radius_m'],validate='one_to_one').set_index('anchor_id').loc[cohort] for r in [1000,1500]}
    scenarios=[]
    for mode in ['baseline','without_age','without_outpatient','without_hospital']:
        for radius,f in frames.items():
            cols=['comparable_population_5069_lower','total_population_uniform']
            upper=['comparable_population_5069_upper','total_population_uniform']
            if mode=='without_age': cols=cols[1:];upper=upper[1:]
            for op in OUTPATIENT:
                for endo in ENDOSCOPY:
                    cost=[op,endo,'egd_equipment_hospitals']
                    if mode=='without_outpatient':cost.remove(op)
                    if mode=='without_hospital':cost.remove('egd_equipment_hospitals')
                    below,_=blocked_dominance(f[cols].to_numpy(),f[upper].to_numpy(),f[cost].to_numpy())
                    scenarios.extend(dict(mode=mode,anchor_id=k,radius_m=radius,outpatient=op,endoscopy=endo,dominated_by=int(n)) for k,n in zip(cohort,below))
        print('Recomputed',mode,len(cohort),'coordinates',flush=True)
    scenarios=pd.DataFrame(scenarios)
    scenarios.to_parquet(out/'diagnostic_scenarios.parquet',index=False)
    summaries=[]
    for mode,g in scenarios.groupby('mode'):
        a=priorities(g);a['mode']=mode;summaries.append(a)
    summary=pd.concat(summaries,ignore_index=True)
    summary.to_parquet(out/'diagnostic_priorities.parquet',index=False)
    new=summary[summary['mode'].eq('baseline')]
    cmp=membership_comparison(prior[['anchor_id','name','population_name','conditional_priority_rank','worst_dominators','frontier_conditions']],new[['anchor_id','conditional_priority_rank','worst_dominators','frontier_conditions']])
    cmp['rank_change']=cmp.conditional_priority_rank_old-cmp.conditional_priority_rank_new
    cmp['reason']='8개 기존 시나리오 독립 재계산; 정책 변경 없음'
    cmp['score_old']=None;cmp['score_new']=None
    cmp['score_note']='가중합 점수 없음; 공동순위와 우월 후보 수로 비교'
    cmp['reliability']='조건부 조사 우선순위; 직선반경·과거 자동차 추정·공간 균등배분'
    cmp.to_csv(out/'v3_all_comparison.csv',index=False,encoding='utf-8-sig')
    cmp[(cmp.conditional_priority_rank_old<=30)|(cmp.conditional_priority_rank_new<=30)].to_csv(out/'v3_top30_with_ties_comparison.csv',index=False,encoding='utf-8-sig')
    selected=set(new.loc[new.top30_including_boundary_ties,'anchor_id'])
    changes=[]
    for mode,g in summary.groupby('mode'):
        picked=set(g.loc[g.top30_including_boundary_ties,'anchor_id'])
        changes.append(dict(mode=mode,top30_with_ties=len(picked),retained=len(selected&picked),entered=len(picked-selected),departed=len(selected-picked)))
    save('ablation_summary.json',changes)
    review=prior[prior.anchor_id.isin(selected)][['anchor_id','name','population_name','conditional_priority_rank','frontier_conditions','area_95_100_pyeong']].copy()
    for mode,g in summary.groupby('mode'):
        review[mode+'_top30']=review.anchor_id.isin(g.loc[g.top30_including_boundary_ties,'anchor_id'])
    shifts=read(source/'commute_priority_position_overview.parquet')
    shifts=shifts[shifts.cohort.eq('primary')]
    unique(shifts,['anchor_id'])
    review=review.merge(shifts[['anchor_id','all_shifts_undominated']],on='anchor_id',how='left',validate='one_to_one')
    pairs=read(source/'priority_web_rough_commute_nearby_competitors.parquet')
    unique(pairs,['anchor_id','provider_id'])
    close=[]
    for k in review.anchor_id:
        a=pairs[pairs.anchor_id.eq(k)&pairs.distance_m.le(500)]
        c=a[a.is_clinic]
        close.append(dict(anchor_id=k,observed_relevant_clinics_500m=len(c),
            observed_endoscopy_positive_clinics_500m=int(c.endoscopy_positive.sum()),
            internal_specialist_unknown_clinics_500m=int(c.internal_specialists.isna().sum()),
            observed_internal_specialists_500m=c.internal_specialists.sum(min_count=1),
            observed_egd_positive_hospitals_500m=int(a.is_hospital.sum())))
    review=review.merge(pd.DataFrame(close),on='anchor_id',validate='one_to_one')
    review['interpretation']='500m 직선거리의 관측 공급; 미확인 장비·인력은 실제 부재 아님. 독립 참고지표, 점수 미반영.'
    review.to_csv(out/'candidate_evidence_review.csv',index=False,encoding='utf-8-sig')
    # Lightweight demand-only screening recall: deliberately not a new rank model.
    recall=[]
    for size in [250,1000,2500,len(cohort)]:
        light=frames[1000].total_population_uniform.rank(method='min',ascending=False).le(size)
        kept=set(frames[1000].index[light]);recall.append(dict(nominal_pool=size,actual_pool=len(kept),original_top_ties=len(selected),retained=len(selected&kept),missed=len(selected-kept)))
    save('lightweight_filter_recall.json',recall)
    corr=frames[1000][['comparable_population_5069_lower','total_population_uniform',*OUTPATIENT,*ENDOSCOPY,'egd_equipment_hospitals']].corr(method='spearman')
    corr.to_csv(out/'indicator_spearman.csv',encoding='utf-8-sig')
    boundary=all_prior[all_prior.cohort.eq('boundary_included')&all_prior.top30_including_boundary_ties]
    p2=dict(cohort_size=len(cohort),top30_with_ties=len(selected),replay_rank_changes=int(cmp.rank_change.ne(0).sum()),
        max_dominator_error=int((cmp.worst_dominators_old-cmp.worst_dominators_new).abs().max()),
        boundary_top30_with_ties=len(boundary),boundary_retained=len(selected&set(boundary.anchor_id)),
        unverified_premises=int(prior[prior.anchor_id.isin(selected)].area_95_100_pyeong.eq('unverified').sum()),
        limitations=['Ablations are diagnostics, not adopted ranking changes.','No road-network travel-time catchment has been computed.','Known Susaek railway separation requires observed crossings; a circular buffer is not a walkable catchment.','Success probability and revenue forecast remain unavailable.'])
    save('phase2.json',p2);save('input_hashes.json',evidence)
    print('Phase 2',json.dumps(p2,ensure_ascii=False),flush=True)
    # Separate historical models by geography; do not subtract incomparable ranks.
    grouped=prior[prior.anchor_id.isin(selected)].groupby(['population_code','population_name'],as_index=False).agg(selected_coordinates=('anchor_id','size'),best_coordinate_rank=('conditional_priority_rank','min'))
    historical=old[['adm_cd10','adm_nm','rank','score']].rename(columns={'adm_cd10':'population_code','rank':'v7_administrative_rank','score':'v7_administrative_score'})
    if historical.population_code.duplicated().any():raise ValueError('Ambiguous legacy geography')
    grouped.merge(historical,on='population_code',how='left',validate='one_to_one').to_csv(out/'administrative_coordinate_reference.csv',index=False,encoding='utf-8-sig')


if __name__=='__main__':main()

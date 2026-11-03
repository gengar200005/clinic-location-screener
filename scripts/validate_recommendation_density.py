"""Cached full-cohort density controls; no collection or catchment recomputation."""
import json
from pathlib import Path
import copy
import hashlib
import numpy as np
import pandas as pd
from scripts.build_current_recommendations import ROOT, LEGACY, REPORT, DENSITY_REPORT, structural_score, score_frame, stable_order
from scripts.recommendation_density import density_summary, reference_weights, percentile


def main():
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--legacy-dir',type=Path,default=LEGACY)
    args=parser.parse_args()
    before=pd.read_csv(DENSITY_REPORT/'before/reports/current_recommendations_20261102/all_candidate_decisions.csv',low_memory=False)
    data=json.loads((ROOT/'web/data/current-recommendations.json').read_text(encoding='utf8'))
    rules=data['rules'];registry=pd.read_parquet(args.legacy_dir/'commercial_node_registry.parquet')
    f=before.rename(columns={'id':'anchor_id','area':'population_name'}).merge(registry[['anchor_id','building_key','building_identity_state','x','y']],on='anchor_id',how='inner',validate='one_to_one')
    density_summary(f,rules['reference_distribution']).to_csv(REPORT/'density_by_admin.csv',index=False,encoding='utf-8-sig')
    cells=f.assign(cell_x=np.floor(f.x/250).astype(int),cell_y=np.floor(f.y/250).astype(int)).groupby(['region','cell_x','cell_y']).size().rename('coordinate_rows')
    cells.to_csv(REPORT/'density_by_cell.csv',encoding='utf-8-sig')
    summary={};comparison=[];components=[]
    def representatives(g, structural, delta, width):
        return stable_order(g.assign(structural=structural),structural+delta,width).drop_duplicates('admin_code').copy()
    for region in ['서울','경기']:
        g=f[f.region.eq(region)&f.exclusion.isna()&f.comparable].copy()
        n=rules['seoul_shortlist' if region=='서울' else 'gyeonggi_shortlist']
        def adjustment(r):
            if region=='경기':return 0.
            return rules['trend_adjustment'].get(r.market_state,rules['unknown_trend_investigation_interval'][0])
        delta=g.apply(adjustment,axis=1)
        legacy_spec=dict(rules['reference_distribution'],mode='legacy_coordinates')
        old=structural_score(g,rules,reference=legacy_spec)
        error=float(np.abs(old-g.structural).max())
        if error>1e-10:raise ValueError('Default pre-correction calculation changed')
        corrected=structural_score(g,rules)
        ref_weights=reference_weights(g,rules['reference_distribution'])
        for radius in rules['radii_m']:
            p=g[f'demand_{radius}']
            ratios={'internal':p/(1+g[f'internal_{radius}']),
                'endoscopy':p/(1+g[f'endo_{radius}']+rules['hospital_burden']*g[f'hospital_{radius}']),
                'gastric':p/(1+g[f'gastric_{radius}'])}
            detail=g[['anchor_id','region','admin_code','population_name']].copy()
            detail['radius_m']=radius
            detail['legacy_demand_percentile']=percentile(p)
            detail['corrected_demand_percentile']=percentile(p,ref_weights)
            for kind,ratio in ratios.items():
                detail[f'{kind}_ratio']=ratio
                detail[f'legacy_{kind}_percentile']=percentile(ratio)
                detail[f'corrected_{kind}_percentile']=percentile(ratio,ref_weights)
            detail['legacy_structural']=old;detail['corrected_structural']=corrected
            detail['medical_adjustment']=delta
            detail['corrected_decision_lower']=corrected+delta
            components.append(detail)
        baseline=representatives(g,old,delta,rules['priority_band_width'])
        adjusted=representatives(g,corrected,delta,rules['priority_band_width'])
        base_top=set(baseline.head(n).admin_code);new_top=set(adjusted.head(n).admin_code)
        old_rank={c:i+1 for i,c in enumerate(baseline.admin_code)};new_rank={c:i+1 for i,c in enumerate(adjusted.admin_code)}
        old_id=baseline.set_index('admin_code').anchor_id.to_dict();new_id=adjusted.set_index('admin_code').anchor_id.to_dict()
        old_nomed=representatives(g,old,0,rules['priority_band_width']);new_nomed=representatives(g,corrected,0,rules['priority_band_width'])
        midpoint=pd.concat([score_frame(g,rules['demand_weight'],rules['hospital_burden'],r,dict(legacy_spec,mode='uniform_midpoint')) for r in rules['radii_m']],axis=1).min(axis=1)
        midpoint_top=set(representatives(g,midpoint,delta,rules['priority_band_width']).head(n).admin_code)
        def membership(a,b):return dict(retained=len(a&b),entered=sorted(b-a),departed=sorted(a-b))
        experiments=[]
        dense_admin=g.admin_code.value_counts().idxmax();target=g[g.admin_code.eq(dense_admin)].sort_values('anchor_id').iloc[[0]]
        for count in [1,10,100]:
            for kind in ['exact','same_cell_jitter','unknown_boundary_jitter']:
                original=g.copy();target_copy=target.copy()
                if kind=='unknown_boundary_jitter':
                    # Expose the unidentifiable cell-boundary limitation deliberately.
                    original.loc[target.index,'building_identity_state']='unresolved'
                    target_copy['building_identity_state']='unresolved'
                    original.loc[target.index,'x']=np.floor(target.x.iloc[0]/250)*250+249.9
                    target_copy['x']=np.floor(target.x.iloc[0]/250)*250+250.1
                elif kind=='same_cell_jitter':
                    target_copy['x']+=.2;target_copy['y']+=.2
                clones=pd.concat([target_copy.assign(anchor_id=f'copy_{i}') for i in range(count)],ignore_index=True)
                expanded=pd.concat([original,clones],ignore_index=True)
                expanded_delta=expanded.apply(adjustment,axis=1)
                new_scores=structural_score(expanded,rules)
                original_score=structural_score(original,rules)
                shift=float(np.abs(new_scores.iloc[:len(original)].to_numpy()-original_score.to_numpy()).max())
                legacy_new=structural_score(expanded,rules,reference=legacy_spec)
                legacy_old=structural_score(original,rules,reference=legacy_spec)
                legacy_shift=float(np.abs(legacy_new.iloc[:len(original)].to_numpy()-legacy_old.to_numpy()).max())
                ranked=representatives(expanded,new_scores,expanded_delta,rules['priority_band_width'])
                experiments.append(dict(kind=kind,copies=count,legacy_max_score_shift=legacy_shift,corrected_max_score_shift=shift,
                    retained_top=len(set(ranked.head(n).admin_code)&new_top),target_admin=str(dense_admin),
                    invariant_claim=kind!='unknown_boundary_jitter'))
                if kind!='unknown_boundary_jitter' and shift>1e-10:raise ValueError('Duplicate-invariance failed')
        thinning=[]
        for size in rules['reference_distribution']['alternative_cell_sizes_m']:
            for seed in [7,19,43]:
                temp=g.assign(_cx=np.floor(g.x/size),_cy=np.floor(g.y/size)).sample(frac=1,random_state=seed)
                # Fixed baseline scores isolate B: search opportunities only.
                reduced=temp.drop_duplicates(['_cx','_cy'])
                sparse=representatives(reduced,corrected.reindex(reduced.index),delta.reindex(reduced.index),rules['priority_band_width'])
                thinning.append(dict(cell_size_m=size,seed=seed,retained_coordinates=len(reduced),
                    retained_top_fixed_reference=len(set(sparse.head(n).admin_code)&new_top),
                    unrepresented_admins=len(set(g.admin_code)-set(reduced.admin_code)),
                    interpretation='B만: 기준분포·기존 좌표 점수 고정, 탐색대상만 축소. 미선택 동은 부적합 아님'))
        restored_id=adjusted.iloc[0].anchor_id
        withheld=g[g.anchor_id.ne(restored_id)].copy()
        withheld_score=structural_score(withheld,rules)
        withheld_order=representatives(withheld,withheld_score,delta.reindex(withheld.index),rules['priority_band_width'])
        new_location_control=dict(actual_cached_location_id=restored_id,withheld_coordinates=len(withheld),restored_coordinates=len(g),
            old_representative_for_area=withheld_order.set_index('admin_code').anchor_id.get(adjusted.iloc[0].admin_code),
            restored_representative_for_area=restored_id,
            max_existing_score_change=float(np.abs(withheld_score-corrected.reindex(withheld.index)).max()),
            meaning='기존 캐시의 실제 서로 다른 위치를 잠시 보류했다가 복원. 가상 매물·새 관측값 생성 아님')
        for code in sorted(set(old_rank)|set(new_rank)):
            row=g[g.admin_code.eq(code)].iloc[0]
            comparison.append(dict(region=region,admin_code=str(code),area=row.population_name,old_order=old_rank[code],new_order=new_rank[code],
                old_id=old_id[code],new_id=new_id[code],representative_changed=old_id[code]!=new_id[code],
                old_priority=code in base_top,new_priority=code in new_top,
                cause='같은 시장자료·설정에서 비교기준 보정. 행정동 대표는 실제 동일 생활권 아님'))
        weights=reference_weights(g,rules['reference_distribution'])
        g.assign(reference_weight=weights).groupby(['admin_code','population_name']).agg(coordinates=('anchor_id','size'),
            reference_mass=('reference_weight','sum')).reset_index().assign(region=region).to_csv(REPORT/f'reference_mass_{region}.csv',index=False,encoding='utf-8-sig')
        variants=json.loads((REPORT/'sensitivity.json').read_text(encoding='utf8'))[region]
        tracked=[]
        for name in ['쌍문3동','길음2동','상도1동']:
            if region!='서울':continue
            match=g[g.population_name.str.endswith(' '+name)]
            code=match.admin_code.iloc[0]
            tracked.append(dict(area=name,old_order=old_rank[code],new_order=new_rank[code],old_id=old_id[code],new_id=new_id[code],
                alternative_range=[min(v[str(code)] for v in variants.values()),max(v[str(code)] for v in variants.values())]))
        summary[region]=dict(compared_coordinates=len(g),baseline_max_calculation_error=error,
            corrected_membership=membership(base_top,new_top),midpoint_only_membership=membership(base_top,midpoint_top),
            density_after_midpoint=membership(midpoint_top,new_top),
            medical_effect_before_correction=membership(set(old_nomed.head(n).admin_code),base_top),
            medical_effect_after_correction=membership(set(new_nomed.head(n).admin_code),new_top),
            representatives_changed=sum(old_id[c]!=new_id[c] for c in old_rank),tracked_candidates=tracked,
            duplicate_controls=experiments,search_opportunity_controls=thinning,
            actual_new_location_control=new_location_control,
            new_top=[dict(area=r.population_name,candidate_id=r.anchor_id,order=i+1) for i,r in enumerate(adjusted.head(5).itertuples())])
    pd.DataFrame(comparison).to_csv(REPORT/'density_before_after.csv',index=False,encoding='utf-8-sig')
    pd.concat(components,ignore_index=True).to_csv(REPORT/'baseline_corrected_components.csv',index=False,encoding='utf-8-sig')
    (REPORT/'DENSITY_VALIDATION.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps({k:{a:b for a,b in v.items() if a not in ['duplicate_controls','search_opportunity_controls']} for k,v in summary.items()},ensure_ascii=False))


if __name__=='__main__':main()

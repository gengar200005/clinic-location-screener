"""Audit new queue, preserve old evidence and compare memberships without rank subtraction."""
import hashlib
import json
from pathlib import Path
import pandas as pd
from scripts.build_current_recommendations import ROOT,REPORT,stable_order


def main():
    d=json.loads((ROOT/'web/data/current-recommendations.json').read_text(encoding='utf8'))
    f=pd.read_csv(REPORT/'all_candidate_decisions.csv',low_memory=False)
    if f.id.duplicated().any():raise ValueError('Duplicate final candidate')
    before=json.loads((REPORT/'preservation_before.json').read_text(encoding='utf8'))
    def protected_path(name):
        if name.startswith('legacy-project/'):
            return ROOT.parent/'clinic-location-screener'/name.removeprefix('legacy-project/')
        path=Path(name)
        return path if path.is_absolute() else ROOT/path
    changed=[p for p,h in before.items() if hashlib.sha256(protected_path(p).read_bytes()).hexdigest()!=h]
    if changed:raise ValueError('Protected artifacts changed: '+str(changed))
    selected=[p for p in d['candidates'] if not p['exclusion'] and p['research_order'] and p['research_order']<=d['rules']['seoul_shortlist' if p['region']=='서울' else 'gyeonggi_shortlist']]
    if len(selected)!=sum(min(d['rules']['seoul_shortlist' if g['region']=='서울' else 'gyeonggi_shortlist'],g['administrative_bundles']) for g in d['comparison_groups']) or any('정릉' in p['area'] for p in selected):raise ValueError('Shortlist contract broken')
    representatives={p['area']:p for p in d['candidates'] if p['research_order'] and not p['exclusion']}
    selected_names={p['area'] for p in selected}
    old=[p for p in d['candidates'] if p['old_visit_order']]
    rows=[]
    for p in sorted(old,key=lambda p:p['old_visit_order']):
        current=representatives.get(p['area'])
        rows.append(dict(zone=p['name'],old_v3_visit_order=p['old_visit_order'],old_candidate_id=p['id'],area=p['area'],
            current_representative_id=current['id'] if current else None,current_region=current['region'] if current else None,
            current_research_order=current['research_order'] if current else None,current_in_priority_group=p['area'] in selected_names,
            change=current['history_change'] if current else p['history_change'],visit_state=current['visit_state'] if current else p['visit_state'],
            reason=' · '.join(current['reasons'] if current else p['reasons']),counter_evidence=' · '.join(current['counter_evidence'] if current else p['counter_evidence']),
            comparison_caution='과거10권역 순서와 새 지역별 전체비교 순서는 모집단이 다름. 순위 차감 없음'))
    pd.DataFrame(rows).to_csv(REPORT/'old_v3_comparison.csv',index=False,encoding='utf-8-sig')
    legacy=json.loads((ROOT/'web/data/research.json').read_text(encoding='utf8'))['anchors']
    v7=pd.read_csv(ROOT/'reports/patient_opportunity_phase1_20261102_final/v7_all_axes.csv')
    latest_v7=[dict(adm_nm=r.adm_nm,rank=r.rank,research_order=None) for r in v7[v7['rank'].le(30)].itertuples()]
    for name,anchors in [('legacy_main_membership',[p for p in legacy if p['research_order']]),('v7_reference_membership',latest_v7)]:
        result=[]
        for a in anchors:
            p=representatives.get(a['adm_nm'])
            result.append(dict(administrative_area=a['adm_nm'],historical_v7_rank=a['rank'],historical_research_order=a['research_order'],
                current_region=p['region'] if p else None,current_coordinate_id=p['id'] if p else None,
                current_research_order=p['research_order'] if p else None,in_current_priority_group=a['adm_nm'] in selected_names,
                caution='행정동 참조점과 새 조사좌표는 다른 단위. 역사적 순위 차감 금지'))
        pd.DataFrame(result).to_csv(REPORT/(name+'.csv'),index=False,encoding='utf-8-sig')
    references=[]
    for a in v7.itertuples():
        p=representatives.get(a.adm_nm)
        references.append(dict(historical_admin_id=a.adm_cd,area=a.adm_nm,historical_v7_rank=a.rank,
            current_area_coordinate=p['id'] if p else None,current_region=p['region'] if p else None,
            link_role='행정동 이름 기준 조사묶음 참고·서로 다른 좌표/순위 단위' if p else '현재 상가좌표 비교군 미연결·자동 저평가하지 않음'))
    pd.DataFrame(references).to_csv(REPORT/'v7_all_reference_coverage.csv',index=False,encoding='utf-8-sig')
    sensitivity=json.loads((REPORT/'sensitivity.json').read_text(encoding='utf8'));variant_summary={}
    no_medical_effect={}
    for region in ['서울','경기']:
        group=f[f.region.eq(region)&f.structural.notna()].rename(columns={'id':'anchor_id'})
        n=d['rules']['seoul_shortlist' if region=='서울' else 'gyeonggi_shortlist']
        common_only=stable_order(group,group.structural,d['rules']['priority_band_width']).drop_duplicates('admin_code').head(n)
        baseline=set(common_only.area);current={p['area'] for p in selected if p['region']==region}
        no_medical_effect[region]=dict(common_only_size=len(baseline),current_size=len(current),retained=len(current&baseline),entered=sorted(current-baseline),departed=sorted(baseline-current))
    for region,variants in sensitivity.items():
        n=d['rules']['seoul_shortlist' if region=='서울' else 'gyeonggi_shortlist'];codes={p['admin_code'] for p in selected if p['region']==region}
        overlaps={key:len(codes & {c for c,r in ranks.items() if r<=n}) for key,ranks in variants.items()}
        sets=[{c for c,r in ranks.items() if r<=n} for ranks in variants.values()]
        variant_summary[region]=dict(variants=len(variants),priority_group=n,minimum_retained=min(overlaps.values()),maximum_retained=max(overlaps.values()),all_variants_common=len(set.intersection(*sets)),overlaps=overlaps)
    validation=dict(version=d['version'],protected_files=len(before),changed_protected_files=changed,
        registry_total=sum(c['registry_coordinates'] for c in d['coverage']),export_rows=len(f),supplemental_manual_cases=1,
        common_comparison_total=sum(c['comparable_common'] for c in d['coverage']),coverage=d['coverage'],groups=d['comparison_groups'],
        v7_reference_rows=len(v7),v7_admin_areas_linked_to_current_bundle=sum(r['current_area_coordinate'] is not None for r in references),
        current_priority_candidates=len(selected),ready_for_visit=sum(p['visit_state']=='현장 확인 가능' for p in selected),
        changes_by_old_v3_membership=pd.Series([p['history_change'] for p in selected]).value_counts().to_dict(),
        sensitivity=variant_summary,medical_increment_counterfactual=no_medical_effect,old_v3_comparison=rows,
        top_three=[{k:p[k] for k in ['id','name','area','zone','research_order','sensitivity','visit_state']} for p in sorted(selected,key=lambda p:p['research_order']) if p['region']=='서울' and p['research_order']<=3])
    (REPORT/'VALIDATION.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps({k:v for k,v in validation.items() if k not in ['old_v3_comparison','sensitivity','coverage','groups']},ensure_ascii=False))


if __name__=='__main__':main()

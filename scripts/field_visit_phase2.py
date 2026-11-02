"""Evidence-based visit triage, no success score and no production changes."""
import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
SELECTION={1:('우선 임장','주거형·상대적 저공급 가설 검증'),5:('우선 임장','큰 상권의 실제 주거·검진 동선 검증'),
           2:('우선 임장','고령 수요 큰 시장·건물 광고 비교'),4:('우선 임장','역세권 수요·공급 혼재와 건물 확보 검증')}


def validate_selection(frame):
    selected=frame[frame.visit_status.eq('우선 임장')]
    if not 3<=len(selected)<=5:raise ValueError('Visit count outside requested scope')
    if selected.anchor_id.duplicated().any():raise ValueError('Duplicate visit')
    if selected.personal_scope.notna().any():raise ValueError('Personal/scope exclusion selected')


def run(legacy,out):
    out.mkdir(parents=True,exist_ok=False)
    phase1=ROOT/'reports/patient_opportunity_phase1_20261102_final/field_visit_axes.csv'
    source=legacy/'reports/screening_v3/2026-10-23'
    paths=[phase1,source/'field_visit_shortlist.json',source/'priority_web_rough_commute_nearby_competitors.parquet',
           ROOT/'SITE_CANDIDATES.csv',ROOT/'web/data/field-followup.json']
    hashes={str(p.resolve()):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    f=pd.read_csv(phase1);f=f[f.radius_m.eq(1000)].copy()
    visits=pd.DataFrame(json.loads(paths[1].read_text(encoding='utf-8'))['visits'])
    f=f.merge(visits[['anchor_id','all_shifts_undominated','undominated_shift_conditions','conditions',
       'car_am_minutes','car_pm_minutes','rough_commute_state']],on='anchor_id',validate='one_to_one')
    pairs=pd.read_parquet(paths[2])
    if pairs.duplicated(['anchor_id','provider_id']).any():raise ValueError('Duplicate provider pair')
    nearest=[]
    for r in f.to_dict('records'):
        k=r['anchor_id'];p=pairs[pairs.anchor_id.eq(k)&pairs.is_clinic]
        endo=p[p.distance_m.le(1000)&p.endoscopy_positive]
        f.loc[f.anchor_id.eq(k),'endoscopy_with_internal_specialist_1km']=int(endo.internal_specialists.gt(0).sum())
        f.loc[f.anchor_id.eq(k),'endoscopy_without_confirmed_internal_specialist_1km']=int((~endo.internal_specialists.gt(0)).sum())
        nearest.extend(p.sort_values('distance_m').head(3).assign(zone=r['zone']).to_dict('records'))
    decisions={3:('개인 제외','정릉 지인 개원에 따른 사용자 선택'),8:('범위 제외','현재 서울·경기 범위 밖'),
       6:('예비','까치산은 큰 수요 유지. 이번 화곡권 임장은 기존8조건 모두 경계상위인 화곡역을 먼저 조사; 까치산4/8·매출 증가만으로 상향하지 않음'),
       7:('예비','연령제거71위·이동조건22/32. 부적합 아닌 추가 검증순서 후순위'),
       9:('예비','연령제거41위·이동조건18/32. 부적합 아닌 조사 집중'),
       10:('예비','8조건중 경계우월0·이동조건16/32. 기존 공동17위 보존; 신규진입기회 반증 아님')}
    f['visit_status']=f.visit_order.map(lambda n:SELECTION.get(n,decisions.get(n))[0])
    f['selection_reason']=f.visit_order.map(lambda n:SELECTION.get(n,decisions.get(n))[1])
    f['adequate_building_verified']=False
    f['commute_interpretation']='과거 자동차 오차대 기반 러프 판정. 현재 경로/실측 통근시간 아님'
    validate_selection(f)
    f.to_csv(out/'ALL_CANDIDATE_DECISIONS.csv',index=False,encoding='utf-8-sig')
    selected=f[f.visit_status.eq('우선 임장')].copy()
    selected['visit_sequence']=selected.visit_order.map({1:1,5:2,2:3,4:4})
    selected.sort_values('visit_sequence').to_csv(out/'PRIMARY_4.csv',index=False,encoding='utf-8-sig')
    pd.DataFrame(nearest)[['anchor_id','zone','provider_id','name','address','distance_m','internal_specialists',
          'endoscopy_positive','current_service_capacity_state']].to_csv(out/'NEAREST_PROVIDER_REVIEW.csv',index=False,encoding='utf-8-sig')
    buildings=json.loads(paths[4].read_text(encoding='utf-8'))['buildings']
    for b in buildings:
        b.update(visit_status='조건부 재방문',zone='수색·DMC 두빌',condition='각 동 전용95~100평 가능 층·공실·도면 확보 후',
          population_inherited=False,priority_score=None)
    (out/'CONDITIONAL_DEUXVILLE.json').write_text(json.dumps(buildings,ensure_ascii=False,indent=2),encoding='utf-8')
    if hashes!={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in hashes}:raise ValueError('Source changed')
    (out/'input_hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf-8')
    print(dict(primary=len(selected),conditional_living_areas=1,conditional_buildings=len(buildings),excluded=2,reserve=4))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--legacy',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();run(a.legacy,a.out)

"""Validated investigation policy and dated premises evidence (not opening approval)."""
from datetime import date, datetime
from zoneinfo import ZoneInfo
import copy
import math


def as_of_date(value=None):
    if value is None:
        return datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat()
    try:
        parsed = date.fromisoformat(value)
        if parsed.isoformat() != value:
            raise ValueError()
        return value
    except (TypeError, ValueError):
        raise ValueError('기준일은 YYYY-MM-DD 형식이어야 합니다') from None


def parse_rules(raw):
    r = copy.deepcopy(raw)
    def finite(x):
        return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)
    def weight(x):
        return finite(x) and 0 <= x <= 1
    if not weight(r.get('demand_weight')) or not r.get('alternative_demand_weights') or not all(weight(x) for x in r['alternative_demand_weights']):
        raise ValueError('수요 비중은 0~1 유한값이어야 합니다')
    if not finite(r.get('hospital_burden')) or r['hospital_burden'] <= 0 or not r.get('hospital_burdens') or not all(finite(x) and x > 0 for x in r['hospital_burdens']):
        raise ValueError('병원 배수는 양수여야 합니다')
    radii = r.get('radii_m')
    if not radii or len(set(radii)) != len(radii) or any(type(x) is not int or x not in (1000, 1500) for x in radii):
        raise ValueError('반경은 확보 캐시 1000/1500m 중 중복 없이 선택해야 합니다')
    if not finite(r.get('priority_band_width')) or r['priority_band_width'] <= 0:
        raise ValueError('조사군 간격은 양수여야 합니다')
    for key in ['seoul_shortlist', 'gyeonggi_shortlist']:
        if type(r.get(key)) is not int or r[key] <= 0:
            raise ValueError('우선군 개수는 양의 정수여야 합니다')
    interval = r.get('unknown_trend_investigation_interval', [])
    if len(interval) != 2 or not all(finite(x) for x in interval) or interval[0] > interval[1]:
        raise ValueError('미확인 추세 범위 오류')
    if not all(finite(r.get('trend_adjustment', {}).get(k)) for k in ['persistent_growth','mixed','amount_only','persistent_decline']):
        raise ValueError('매출 조정 설정 오류')
    ref = r['reference_distribution']
    if ref['mode'] not in ['legacy_coordinates', 'spatial_cells']:
        raise ValueError('백분위 비교기준 오류')
    for size in [ref['cell_size_m']] + ref['alternative_cell_sizes_m']:
        if not finite(size) or size <= 0:
            raise ValueError('공간 셀 크기는 양수여야 합니다')
    if len(ref['offset_m']) != 2 or not all(finite(x) for x in ref['offset_m']):
        raise ValueError('공간 셀 원점 오류')
    if not ref['alternative_offset_fractions'] or not all(finite(x) and 0 <= x < 1 for x in ref['alternative_offset_fractions']):
        raise ValueError('대안 셀 원점 오류')
    for k in ['vacancy', 'lease', 'area', 'layout', 'elevator', 'parking']:
        if type(r['building_freshness_days'].get(k)) is not int or r['building_freshness_days'][k] < 0:
            raise ValueError('항목별 재확인 기간 오류')
    return r


def date_state(value, as_of, ttl):
    if not value:
        return dict(state='missing', checked_on=value, age_days=None)
    try:
        day = date.fromisoformat(as_of_date(value))
    except ValueError:
        return dict(state='invalid', checked_on=value, age_days=None)
    age = (date.fromisoformat(as_of) - day).days
    return dict(state='future' if age < 0 else 'fresh' if age <= ttl else 'expired', checked_on=value, age_days=age)


# Field absence leaves existing evidence alone. Explicit null is a dated unknown.
FIELDS = {
    'vacant': ('vacancy', ('vacant',), '공실'),
    'area': ('area', ('exclusive_pyeong',), '전용면적'),
    'layout': ('layout', ('layout_prechecked','layout_verified'), '배치'),
    'elevator': ('elevator', ('elevator_prechecked','elevator_verified'), '승강기'),
    'parking': ('parking', ('parking',), '주차'),
    'dropoff': ('parking', ('dropoff',), '승하차'),
    'patient_access': ('parking', ('patient_access',), '환자 접근'),
    'lease': ('lease', ('lease_terms',), '임대조건'),
    'location': ('area', ('same_location_verified',), '위치'),
}


def floor_key(value):
    import re
    if type(value) is int:
        return str(value)
    if isinstance(value,str) and value.strip():
        value=value.strip()
        return str(int(value)) if re.fullmatch(r'-?[0-9]+',value) else value
    return None


def evidence_value(field, raw, candidate, record):
    if field=='area':
        try:
            value=float(raw)
            return value if not isinstance(raw,bool) and math.isfinite(value) else None
        except (TypeError,ValueError):
            return None
    if field=='lease':
        return raw
    if field=='location' and raw is True and (record.get('lat') is not None or record.get('lon') is not None):
        try:
            return all(abs(float(record[k])-candidate[k])<1e-7 for k in ('lat','lon'))
        except (TypeError,ValueError,KeyError):
            return False
    return raw if type(raw) is bool else None


def latest_evidence(field, rows, candidate, rules, as_of):
    import json
    kind, aliases, label=FIELDS[field]
    observations=[]
    for row in rows:
        key=next((k for k in aliases if k in row),None)
        if key is None:
            continue
        checked=row.get(field+'_checked_on', row.get(kind+'_checked_on', row.get('checked_on')))
        if field=='location':
            checked=row.get('location_checked_on',row.get('checked_on',row.get('area_checked_on')))
        timing=date_state(checked,as_of,rules['building_freshness_days'][kind])
        value=evidence_value(field,row[key],candidate,row) if timing['state'] in ('fresh','expired') else None
        observations.append(dict(timing,value=value,source=row.get('source')))
    observations.sort(key=lambda e:json.dumps(e,sort_keys=True,ensure_ascii=False))
    usable=[e for e in observations if e['state'] in ('fresh','expired')]
    latest=max((e['checked_on'] for e in usable),default=None)
    selected=[e for e in usable if e['checked_on']==latest]
    values={json.dumps(e['value'],sort_keys=True,ensure_ascii=False) for e in selected}
    if not selected:
        timing=observations[0] if observations else date_state(None,as_of,0)
        return dict(state=timing['state'],checked_on=timing['checked_on'],value=None,observations=observations)
    conflict=len(values)>1
    return dict(state='conflict' if conflict else selected[0]['state'], checked_on=latest,
                value=None if conflict else selected[0]['value'],observations=observations,
                sourced=all(bool(e['source']) for e in selected))


def premises_review(candidate, buildings, rules, as_of):
    """Resolve dated facts for the exact building, floor and optional unit."""
    groups={}
    for row in buildings:
        if row.get('candidate_id')!=candidate['id']:
            continue
        key=(row.get('building_id'),floor_key(row.get('floor')),row.get('unit',row.get('unit_id',row.get('room'))))
        groups.setdefault(key,[]).append(row)
    records=[]
    for (building,floor,unit),rows in sorted(groups.items(),key=lambda item:repr(item[0])):
        facts={field:latest_evidence(field,rows,candidate,rules,as_of) for field in FIELDS}
        def fresh(field):
            return facts[field]['state']=='fresh' and facts[field].get('sourced',False)
        def value(field):
            return facts[field]['value']
        area=value('area')
        area_state='unknown' if area is None else 'below_target' if area<95 else 'above_target' if area>100 else 'within_target'
        location_match=facts['location'].get('sourced',False) and value('location') is True
        required=['vacant','area','layout','elevator']
        ready=bool(building and floor is not None and location_match and all(fresh(k) for k in required)
                   and area_state=='within_target' and all(value(k) is True for k in ('vacant','layout','elevator')))
        pending=['현장 임상 동선·설비·확장 배치','의료기관 개설 용도 및 구조','환자 접근·승하차 현장 확인']
        for field,fact in facts.items():
            label=FIELDS[field][2]
            if fact['state']=='conflict':
                pending.append(f'{label} 같은 확인일 근거 충돌·재확인')
            elif field in required and not fresh(field):
                pending.append(f'{label} 유효한 근거 재확인')
            elif field in ('vacant','layout','elevator') and value(field) is not True:
                pending.append(f'{label} 미확인' if value(field) is None else f'{label} 조건 미충족')
        for field in ('parking','dropoff','patient_access'):
            if not fresh(field) or value(field) is not True:
                pending.append(f'{FIELDS[field][2]} 제약' if fresh(field) and value(field) is False else f'{FIELDS[field][2]} 미확인·재확인')
        if area_state=='above_target': pending.append('목표 면적 초과·별도 규모/비용 검토')
        if area_state=='below_target': pending.append('목표 최소면적 미달·다른 공간 확인')
        if not location_match: pending.append('다른 주소 또는 위치 미확인·해당 건물 좌표 수요/공급 재평가 필요')
        records.append(dict(building_id=building,floor=floor,unit=unit,exclusive_pyeong=area,area_state=area_state,
                            vacant=value('vacant'),parking='confirmed' if fresh('parking') and value('parking') is True else 'constraint' if fresh('parking') and value('parking') is False else 'unknown_or_stale',
                            dropoff=value('dropoff') if fresh('dropoff') else None,patient_access=value('patient_access') if fresh('patient_access') else None,
                            dates=facts,location_reassessment_required=not location_match,previsit_ready=ready,
                            opening_suitability='판단 보류',pending=pending))
    if candidate.get('exclusion'):
        state = '추가 분석 필요' if candidate['exclusion'].startswith('러프') else '개인조건 또는 범위 제외'
    elif any(r['previsit_ready'] for r in records): state = '현장 확인 가능'
    elif candidate.get('manual_revisit'): state = '새 정보 확보 후 재방문'
    elif candidate.get('commute_lane', 2) > 0 or not candidate.get('comparable', True): state = '추가 분석 필요'
    else: state = '매물·공간 사전확인'
    return dict(state=state, records=records, opening_suitability='판단 보류',
                note='전화·광고·도면의 사전확인과 현장 검증은 별도. 현장 확인 가능은 개원 적합 확정이 아닙니다.')

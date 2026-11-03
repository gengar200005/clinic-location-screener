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


def premises_review(candidate, buildings, rules, as_of):
    """Pre-visit evidence may justify a visit; site-only checks stay explicit."""
    records = []
    for source in buildings:
        if source.get('candidate_id') != candidate['id']:
            continue
        b = dict(source)
        dates = {k: date_state(b.get(k + '_checked_on', b.get('checked_on')), as_of, days)
                 for k, days in rules['building_freshness_days'].items()}
        # Future facts are not usable during historical replay. Dates remain visible.
        for kind, fields in {'vacancy':['vacant'], 'area':['exclusive_pyeong'],
                'layout':['layout_prechecked','layout_verified'], 'elevator':['elevator_prechecked','elevator_verified'],
                'parking':['parking','dropoff','patient_access']}.items():
            if dates[kind]['state'] in ['future','missing','invalid']:
                for field in fields: b[field] = None
        fresh = lambda k: dates[k]['state'] == 'fresh'
        try:
            area = float(b.get('exclusive_pyeong'))
            if not math.isfinite(area): area = None
        except (ValueError, TypeError): area = None
        area_state = 'unknown' if area is None else 'below_target' if area < 95 else 'above_target' if area > 100 else 'within_target'
        # New addresses cannot silently inherit the original point's catchment.
        location_match = b.get('same_location_verified') is True
        if b.get('lat') is not None or b.get('lon') is not None:
            try:
                location_match = location_match and abs(float(b['lat'])-candidate['lat']) < 1e-7 and abs(float(b['lon'])-candidate['lon']) < 1e-7
            except (ValueError, TypeError, KeyError): location_match = False
        layout = b.get('layout_prechecked', b.get('layout_verified')) is True
        elevator = b.get('elevator_prechecked', b.get('elevator_verified')) is True
        prerequisites = bool(b.get('source') and b.get('building_id') and b.get('floor') is not None and
                             b.get('vacant') is True and fresh('vacancy') and fresh('area') and
                             area_state == 'within_target' and layout and fresh('layout') and elevator and fresh('elevator') and location_match)
        pending = ['현장 임상 동선·설비·확장 배치', '의료기관 개설 용도 및 구조', '환자 접근·승하차 현장 확인']
        if b.get('parking') is not True or not fresh('parking'):
            pending.append('주차 미확인·재확인' if b.get('parking') is not False else '주차 명시적 제약·대안 검토')
        if b.get('dropoff') is not True: pending.append('차량 승하차 미확인/제약')
        if b.get('patient_access') is not True: pending.append('환자 접근 미확인/제약')
        if area_state == 'above_target': pending.append('목표 면적 초과·별도 규모/비용 검토')
        if area_state == 'below_target': pending.append('목표 최소면적 미달·다른 공간 확인')
        if not location_match: pending.append('다른 주소 또는 위치 미확인·해당 건물 좌표 수요/공급 재평가 필요')
        records.append(dict(building_id=b.get('building_id'), floor=b.get('floor'), source=b.get('source'),
                            exclusive_pyeong=area, area_state=area_state, vacant=b.get('vacant'),
                            parking='confirmed' if b.get('parking') is True and fresh('parking') else 'constraint' if b.get('parking') is False and fresh('parking') else 'unknown_or_stale',
                            dropoff=b.get('dropoff') if fresh('parking') else None, patient_access=b.get('patient_access') if fresh('parking') else None, dates=dates,
                            location_reassessment_required=not location_match, previsit_ready=prerequisites,
                            record_checked_on=b.get('checked_on'), opening_suitability='판단 보류', pending=pending))
    # A newer occupied record supersedes a still-fresh older vacancy report.
    # Future/invalid records do not supersede known past facts during replay.
    for b in records:
        peers=[r for r in records if (r['building_id'],r['floor'])==(b['building_id'],b['floor']) and
               date_state(r['record_checked_on'],as_of,10**7)['state']=='fresh']
        latest=max((r['record_checked_on'] for r in peers),default=None)
        b['superseded']=bool(latest and (date_state(b['record_checked_on'],as_of,10**7)['state']!='fresh' or b['record_checked_on']<latest))
        latest_peers=[r for r in peers if r['record_checked_on']==latest]
        conflict=len({r['vacant'] for r in latest_peers})>1
        if b['superseded'] or conflict:
            b['previsit_ready']=False
            b['pending'].append('더 최근 확인기록 우선·과거 기록' if b['superseded'] else '동일 확인일 공실 근거 충돌·재확인')
    if candidate.get('exclusion'):
        state = '추가 분석 필요' if candidate['exclusion'].startswith('러프') else '개인조건 또는 범위 제외'
    elif any(r['previsit_ready'] for r in records): state = '현장 확인 가능'
    elif candidate.get('manual_revisit'): state = '새 정보 확보 후 재방문'
    elif candidate.get('commute_lane', 2) > 0 or not candidate.get('comparable', True): state = '추가 분석 필요'
    else: state = '매물·공간 사전확인'
    return dict(state=state, records=records, opening_suitability='판단 보류',
                note='전화·광고·도면의 사전확인과 현장 검증은 별도. 현장 확인 가능은 개원 적합 확정이 아닙니다.')

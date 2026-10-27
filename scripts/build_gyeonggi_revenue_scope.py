"""Inventory ranked Gyeonggi candidates; never synthesize revenue observations."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]


def build(anchors):
    rows = [dict(anchor_id=a['adm_cd'], name=a['adm_nm'], rank=a['rank'],
                 research_order=a.get('research_order'), status='not_acquired')
            for a in anchors if a['adm_nm'].startswith('경기도 ') and a.get('rank') is not None]
    return sorted(rows, key=lambda r: (r['rank'], r['anchor_id']))


if __name__ == '__main__':
    source = json.loads((ROOT/'web/data/research.json').read_text())
    result = dict(scope='research.json 내 비교 순위가 있는 경기도 후보',
                  ranking_as_of=source['as_of'], checked_at='2026-10-27',
                  status='source_unverified',
                  source_url='https://data.gg.go.kr/portal/data/service/selectServicePage.do?infId=H92OOSWZXICJM0UO4IHH38259217&infSeq=1',
                  reason='공식 행정동 카드매출 데이터셋을 찾았으나 상세 자료 요청이 HTTP 503으로 실패. 의원 업종·시계열·이용조건 미검증.',
                  candidates=build(source['anchors']))
    (ROOT/'web/data/gyeonggi-revenue-scope.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n')
    print(len(result['candidates']))

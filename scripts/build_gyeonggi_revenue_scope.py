"""Inventory ranked Gyeonggi candidates; never synthesize revenue observations."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]


def build(anchors):
    rows = [dict(anchor_id=a['adm_cd'], name=a['adm_nm'], rank=a['rank'],
                 research_order=a.get('research_order'), status='not_acquired',
                 revenue_code=None, code_match_status='pending_source_codebook',
                 periods=[], yoy=None, mom=None)
            for a in anchors if a['adm_nm'].startswith('경기도 ') and a.get('rank') is not None]
    if len({r['anchor_id'] for r in rows}) != len(rows):
        raise ValueError('Duplicate Gyeonggi candidate identifiers')
    return sorted(rows, key=lambda r: (r['rank'], r['anchor_id']))


def snapshot(source, verification, acquisition):
    # No observation importer yet: fetching a file cannot approve its semantics.
    candidates = build(source['anchors'])
    return dict(scope='research.json 내 비교 순위가 있는 경기도 후보',
                ranking_as_of=source['as_of'], checked_at=acquisition['checked_at'],
                status='validation_pending', source_url=acquisition['source_url'],
                catalog_url=acquisition['catalog_url'],
                reason=verification['summary'], checks=verification['checks'],
                coverage=dict(candidates=len(candidates), verified_trends=0,
                              verified_code_matches=0), candidates=candidates)


if __name__ == '__main__':
    source = json.loads((ROOT/'web/data/research.json').read_text(encoding='utf-8'))
    folder = ROOT/'data/manual/revenue/gyeonggi'
    result = snapshot(source, json.loads((folder/'verification.json').read_text(encoding='utf-8')),
                      json.loads((folder/'acquisition.json').read_text(encoding='utf-8')))
    (ROOT/'web/data/gyeonggi-revenue-scope.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(len(result['candidates']))

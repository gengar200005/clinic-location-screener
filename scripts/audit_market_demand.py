"""Produce a comparison CSV, input hashes and preservation evidence; no network."""
import csv
import hashlib
import json
from pathlib import Path
from scripts.market_demand import ROOT, CACHE


def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    report=ROOT/'reports/market_demand_20261102'
    data=json.loads((ROOT/'web/data/market-demand.json').read_text(encoding='utf-8'))
    before=json.loads((report/'preservation_before.json').read_text(encoding='utf-8'))
    after={name:digest(ROOT/name) for name in before}
    changes=[name for name in before if before[name]!=after[name]]
    result=dict(files_checked=len(before),changed=changes,unchanged=not changes,hashes=after)
    (report/'preservation_after.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    inputs=[*CACHE.iterdir(),*sorted((ROOT/'data/manual/revenue').glob('sales*.csv')),
            ROOT/'data/manual/revenue/shops.csv',ROOT/'web/data/revenue.json',ROOT/'web/data/visit-evidence.json',
            ROOT/'reports/patient_opportunity_phase1_20261102_final/field_visit_axes.csv',ROOT/'SITE_CANDIDATES.csv']
    provenance={str(p.relative_to(ROOT)):dict(bytes=p.stat().st_size,sha256=digest(p)) for p in inputs if p.is_file()}
    (report/'inputs.json').write_text(json.dumps(provenance,ensure_ascii=False,indent=2),encoding='utf-8')
    rows=[]
    for p in data['pilots']:
        latest=p['medical'][-1]
        rows.append(dict(candidate_id=p['id'],zone=p['name'],lat=p['lat'],lon=p['lon'],admin_code=p['administrative_code'],
          administrative_area=p['administrative_fullname'],medical_quarter=latest['quarter'],
          amount=latest['amount']['value'],transactions=latest['transactions']['value'],unit_amount=latest['per_transaction'],
          amount_yoy=latest['amount_yoy'],transactions_yoy=latest['transactions_yoy'],unit_yoy=latest['per_transaction_yoy'],
          shops=latest['shops']['value'],shops_yoy_change=latest['shop_change'],
          consumption_raw=p['consumption']['value']['value'],context_quarter=p['consumption']['display_quarter'],
          consumption_cadence_matches=p['consumption']['matches_update_quarter'],workers=p['workers']['value']['value'],
          living_status=p['living_population']['status'],building_status=p['building_status'],
          previous_judgement=p['judgement']['before'],new_evidence=p['judgement']['new_evidence'],
          current_judgement=p['judgement']['after'],next_check=p['judgement']['next_check']))
    with (report/'comparison.csv').open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,rows[0].keys());writer.writeheader();writer.writerows(rows)
    print(dict(preservation=result['unchanged'],files=result['files_checked'],comparison_rows=len(rows),input_files=len(provenance)))
    if changes:raise ValueError('Protected snapshot changed: '+str(changes))


if __name__=='__main__':main()

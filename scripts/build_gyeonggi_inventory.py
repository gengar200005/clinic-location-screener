"""Audit historical observations without claiming verified clinic revenue."""
import argparse
import collections
import csv
import hashlib
import json
import io
import zipfile
from pathlib import Path
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]


def source_rows(raw):
    if raw.suffix.lower()=='.zip':
        with zipfile.ZipFile(raw) as z:
            names=[n for n in z.namelist() if n.lower().endswith('.csv')]
            if len(names)!=1: raise ValueError('Expected exactly one CSV in ZIP')
            text=z.read(names[0]).decode('cp949')
        mapping={'std_ym':'기준년월','admdong_cd':'행정동코드',
                 'mdclass_indutype_cd':'중분류업종코드','sales_amt':'매출금액'}
        reader=csv.DictReader(io.StringIO(text))
        if set(reader.fieldnames or [])!=set(mapping): raise ValueError('Unexpected ZIP schema')
        for row in reader:
            if None in row or any(v is None for v in row.values()):raise ValueError('Malformed ZIP row')
            yield {mapping[k]:v for k,v in row.items()}
    else:
        with raw.open(encoding='cp949',newline='') as f:
            yield from csv.DictReader(f)


def build(raw, book):
    lookup=collections.defaultdict(set)
    for code,city,dong in pd.read_excel(book,'ADMDONG_CD',dtype=str).itertuples(index=False,name=None):
        if all(pd.notna(v) for v in [code,city,dong]):
            lookup[''.join(f'경기도 {city.strip()} {dong.strip()}'.split())].add(code.strip())
    medical=collections.defaultdict(list)
    months=set(); industries=set(); seen=set(); duplicates=0; count=0
    for r in source_rows(raw):
        key=(r['행정동코드'],r['기준년월'],r['중분류업종코드'])
        if key in seen: duplicates+=1
        seen.add(key);months.add(key[1]);industries.add(key[2]);count+=1
        if key[2]=='J0':medical[key[0]].append(dict(month=key[1],raw_amount=r['매출금액']))
    if duplicates:raise ValueError('Duplicate observations; do not aggregate')
    anchors=json.loads((ROOT/'web/data/research.json').read_text(encoding='utf-8'))['anchors']
    candidates=[]
    for r in anchors:
        if not r['adm_nm'].startswith('경기도 ') or r.get('rank') is None:continue
        matches=lookup.get(''.join(r['adm_nm'].split()),set());code=next(iter(matches)) if len(matches)==1 else None
        history=sorted(medical.get(code,[]),key=lambda r:r['month'])
        candidates.append(dict(anchor_id=r['adm_cd'],name=r['adm_nm'],code_2024=code,
            code_status='2024 공식 코드표 전체 동명 일치·시점별 경계 연속성 미검증' if code else '공식 코드표 전체 동명 미일치 또는 중복',
            historical_observations=len(history),first=history[0]['month'] if history else None,
            last=history[-1]['month'] if history else None,history=history,
            trend_status='금액단위·추정정의 및 시점별 코드 연속성 미검증; 비교 보류'))
    return dict(checked_on='2026-11-02',industry=dict(code='J0',label='의료기관',
        scope='의원 단독 아님. 병원·치과·한의원·약국 포함 상세 범위 미확인',new_codes='2024~2025 3자리 업종코드 대응 미검증'),
        amount_unit='미검증·원자료 문자열 보존',coverage=dict(candidates=len(candidates),
        codebook_name_matches=sum(bool(r['code_2024']) for r in candidates),
        historical_observation_candidates=sum(bool(r['history']) for r in candidates),verified_clinic_trends=0),
        raw_profile=dict(rows=count,duplicate_keys=duplicates,first=min(months),last=max(months),
        months=sorted(months),industry_codes=sorted(industries),medical_last=max(x['month'] for v in medical.values() for x in v)),
        sources=[dict(file=str(p.relative_to(ROOT)) if p.is_relative_to(ROOT) else p.name,
        sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in [raw,book]], candidates=candidates)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--raw',type=Path,required=True)
    parser.add_argument('--codebook',type=Path,required=True);args=parser.parse_args()
    data=build(args.raw,args.codebook)
    (ROOT/'web/data/gyeonggi-revenue-inventory.json').write_text(json.dumps(data,ensure_ascii=False,separators=(',',':')),encoding='utf-8')
    print(data['coverage'])

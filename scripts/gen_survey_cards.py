"""각 후보 동에 대한 답사 카드(.md) 자동 생성.

입력:
  - data/scored/scores_{date}.parquet (점수·플래그·역세권)
  - data/cleaned/clinics_by_dong.parquet (의원 리스트)
  - data/cache/admin_centroid_pop.parquet (좌표)

출력:
  data/scored/survey_cards/{rank:02d}_{adm_cd}_{adm_nm_safe}.md
  + INDEX.md (전체 목록 + 빠른 점프)

사용:
  python -m scripts.gen_survey_cards --top 30
  python -m scripts.gen_survey_cards --sido 경기도 --top 20
"""
from __future__ import annotations

import argparse
import logging
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from config.constants import DATA_CACHE, DATA_CLEANED, DATA_SCORED, ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("survey")

OUT_DIR = DATA_SCORED / "survey_cards"
TODAY = date.today().isoformat()


def safe_name(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]", "_", s)
    return s.strip().replace(" ", "_")


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = np.sin(dlat/2)**2 + np.cos(np.radians(lat1))*np.cos(np.radians(lat2))*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))


def fmt_int(x, default="-"):
    if pd.isna(x):
        return default
    return f"{int(x):,}"


def fmt_pct(x, default="-"):
    if pd.isna(x):
        return default
    return f"{x*100:.1f}%"


def fmt_score(x, default="-"):
    if pd.isna(x):
        return default
    return f"{x:.3f}"


def gen_card(row, nearby_clinics: pd.DataFrame, centroid_lat: float, centroid_lon: float) -> str:
    """단일 동 답사 카드 markdown 생성."""
    adm_nm = row["adm_nm"]
    adm_cd = row["adm_cd"]
    sido = row["sido"]
    sgg = row["sgg"]
    rank = int(row["rank"])
    rank_sido = int(row.get("rank_sido", -1))

    flags = []
    if row.get("med_desert_flag"):
        flags.append("🏜️ med_desert (의원 부족)")
    if row.get("centroid_mismatch_flag"):
        flags.append("⚠️ centroid_mismatch (지리 중심과 인구 중심 불일치)")
    if row.get("suburban_cluster_flag"):
        flags.append("🏘️ suburban_cluster (외곽 군집)")
    flag_str = " · ".join(flags) if flags else "(특이 플래그 없음)"

    # 지도 링크
    kakao_url = f"https://map.kakao.com/?map_type=TYPE_MAP&q={adm_nm}"
    naver_url = f"https://map.naver.com/v5/search/{adm_nm}"
    # b=B2 (월세) — 상가 매물은 거의 월세. 매매(A1)·전세(B1)로는 빈 결과.
    naver_estate_url = f"https://new.land.naver.com/offices?ms={centroid_lat},{centroid_lon},16&a=SG&b=B2&e=RETAIL"

    # 가까운 의원 5개 (해당 동 내)
    clinic_lines = []
    if len(nearby_clinics) > 0:
        sub = nearby_clinics.head(5)
        for _, c in sub.iterrows():
            name = c.get("yadmNm", "?")
            addr = c.get("addr", "")
            cls = c.get("clCdNm", "")
            tel = c.get("telno", "")
            estb = c.get("estbDd", "")
            estb_yr = str(estb)[:4] if estb else ""
            clinic_lines.append(f"- **{name}** ({cls}) · {addr} · {tel} · 개설 {estb_yr}")
    else:
        clinic_lines.append("- (동 내 등록 의원 없음 = med_desert 후보)")

    # 점수 분해 표
    md = f"""# {rank}위. {adm_nm}

> **{sido} {sgg}** · 행정코드 `{adm_cd}` · 종합 점수 **{fmt_score(row['score'])}**
> 카테고리 순위: {sido} 중 **{rank_sido}위**
> {flag_str}

## 점수 분해

| 항목 | 정규화 | 원시값 | 해석 |
|---|---|---|---|
| 경쟁 (C) | {fmt_score(row.get('c_norm'))} | c_raw {fmt_score(row.get('c_raw'))} | 동 내 의원 {fmt_int(row.get('n_clinic'))}개 / 500m {fmt_int(row.get('n_within_radius'))}개 |
| 인구 (P) | {fmt_score(row.get('p_norm'))} | {fmt_int(row.get('p_raw'))} | 1.5km 배후 40+ |
| 통근 (T) | {fmt_score(row.get('t_norm'))} | 자차 {fmt_int(row.get('t_raw'))}분 / 대중교통 {fmt_int(row.get('t_transit'))}분 | 이촌1동 기준 |

## 인구·시장

- 동 인구: {fmt_int(row.get('pop_total'))}
- 동 내 40+: {fmt_int(row.get('pop_40plus'))} ({fmt_pct(row.get('ratio_40plus'))})
- 1.5km 배후 인구 (전 연령): {fmt_int(row.get('catchment_pop_1_5km'))}
- 1.5km 배후 40+: {fmt_int(row.get('catchment_pop_40plus'))}
- 인구 1만명당 의원 수: {fmt_score(row.get('density_per_10k'))}

## 경쟁

- 동 내 의원: **{fmt_int(row.get('n_clinic'))}개** (소화기 태그 {fmt_int(row.get('n_clinic_gi'))}개)
- 반경 의원 수: 500m {fmt_int(row.get('n_clinic_500m'))} · 1km {fmt_int(row.get('n_clinic_1km'))} · 2km {fmt_int(row.get('n_clinic_2km'))}

### 동 내 등록 의원 (Top 5)
{chr(10).join(clinic_lines)}

> 전체 의원 리스트는 [web detail](../../web/detail/{adm_cd}.json) 참조 (가능 시).

## 접근성·교통

- 가장 가까운 역: **{row.get('nearest_station', '-')}** ({fmt_int(row.get('station_dist_m'))}m)
- 역세권 500m 의원: {fmt_int(row.get('n_clinic_station_500m'))}개
- 자차 통근: {fmt_int(row.get('t_raw'))}분
- 대중교통 통근: {fmt_int(row.get('t_transit'))}분

## 답사 준비 링크

- 🗺️ [카카오맵 검색]({kakao_url})
- 🗺️ [네이버 지도]({naver_url})
- 🏢 [네이버 부동산 상가 (1종 근생)]({naver_estate_url})
  - 필터: 건물용도 = **제1종 근린생활시설**, 면적 80-150㎡, 1-3층 권장
- 📋 [답사 체크리스트 템플릿](../../docs/SURVEY_CHECKLIST.md) — 출력해서 가져갈 것

## 답사 메모 (방문 후 기입)

- 방문일:
- 시간대:
- 첫인상:
- 방문 매물 1: 주소 / 평수 / 보증금 / 월세 / 평당가 / 1종 근생 여부
- 방문 매물 2:
- 방문 매물 3:
- 경쟁 의원 답사 메모:
- 종합 평가 (1-5):
- 다음 액션:

---
*카드 생성: {TODAY}*
"""
    return md


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", default=str(DATA_SCORED / f"scores_{TODAY}.parquet"))
    ap.add_argument("--top", type=int, default=30, help="상위 N개 동 카드 생성")
    ap.add_argument("--sido", default=None, help="특정 시도만 (예: '서울특별시', '경기도')")
    args = ap.parse_args()

    src = Path(args.scores)
    if not src.exists():
        candidates = sorted(DATA_SCORED.glob("scores_*.parquet"))
        src = candidates[-1]
    log.info("loading %s", src)
    df = pd.read_parquet(src)

    if args.sido:
        df = df[df["sido"] == args.sido].copy()
        df = df.sort_values("score", ascending=False).reset_index(drop=True)
        df["rank_sido"] = df.index + 1
        log.info("filter sido=%s: %d rows", args.sido, len(df))
    else:
        # 시도별 rank 계산
        df["rank_sido"] = df.groupby("sido")["score"].rank(method="min", ascending=False).astype(int)

    df = df[df["rank"] <= args.top].copy()
    log.info("generating cards for top %d", len(df))

    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")
    centroid = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")
    centroid["adm_cd"] = centroid["adm_cd"].astype(str)
    centroid_lookup = centroid.set_index("adm_cd")[["lat_pop", "lon_pop"]]

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # INDEX 작성
    index_lines = [
        f"# 답사 카드 INDEX ({TODAY})",
        "",
        f"총 {len(df)}개 동" + (f" · 시도 필터: {args.sido}" if args.sido else ""),
        "",
        "| 순위 | 시도 순위 | 동 | 시군구 | 점수 | 자차(분) | 가까운역 |",
        "|---|---|---|---|---|---|---|",
    ]

    for _, row in df.iterrows():
        adm_cd = str(row["adm_cd"])
        adm_nm = row["adm_nm"]
        rank = int(row["rank"])
        c_info = centroid_lookup.loc[adm_cd] if adm_cd in centroid_lookup.index else None
        if c_info is not None:
            lat, lon = c_info["lat_pop"], c_info["lon_pop"]
        else:
            lat, lon = 37.5, 127.0

        # 동 내 의원 + 개설일 최신순 정렬
        nearby = clinics[clinics["adm_cd"] == adm_cd].copy()
        if len(nearby):
            nearby = nearby.sort_values("estbDd", ascending=False)

        md = gen_card(row, nearby, lat, lon)
        fname = f"{rank:02d}_{adm_cd}_{safe_name(adm_nm)}.md"
        (OUT_DIR / fname).write_text(md, encoding="utf-8")

        index_lines.append(
            f"| {rank} | {int(row['rank_sido'])} | [{adm_nm}](./{fname}) | {row['sgg']} | "
            f"{fmt_score(row['score'])} | {fmt_int(row.get('t_raw'))} | "
            f"{row.get('nearest_station', '-')} ({fmt_int(row.get('station_dist_m'))}m) |"
        )

    (OUT_DIR / "INDEX.md").write_text("\n".join(index_lines), encoding="utf-8")
    log.info("wrote %d cards + INDEX.md → %s", len(df), OUT_DIR)


if __name__ == "__main__":
    main()

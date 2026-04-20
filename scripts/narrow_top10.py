"""답사 후보 압축 스크립트.

여러 기준으로 Top10 병렬 출력 → 사용자가 골라 답사 일정 짤 때 쓰는 메뉴.

기준:
  1. score        — 종합 점수 순 (기본)
  2. commute      — score × 자차 통근 페널티 (가까운 동 우선)
  3. new_town     — 신도시·택지지구만 (1·2·3기, 신축 상가 풍부)
  4. desert       — med_desert (의원 부족) 동만
  5. p_density    — 인구당 의원 수가 적은 동 (잠재 미충족 수요)

출력: data/scored/narrow_top10_{date}.csv (탭별 시트 1개에 모두)
"""
from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from config.constants import DATA_SCORED, ROOT

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("narrow")

NEW_TOWNS_YAML = ROOT / "config" / "new_towns.yaml"
T_RAW_MAX = 50
TODAY = date.today().isoformat()

EXPORT_COLS = [
    "criterion", "rank_in_list", "sido", "sgg", "adm_nm",
    "score", "score_adj",
    "n_clinic", "n_clinic_500m", "n_clinic_1km",
    "p_raw", "ratio_40plus", "density_per_10k",
    "t_raw", "t_transit", "nearest_station", "station_dist_m",
    "med_desert_flag", "centroid_mismatch_flag", "suburban_cluster_flag",
    "town_tag", "adm_cd",
]


def load_new_towns() -> dict[str, list[str]]:
    with open(NEW_TOWNS_YAML, encoding="utf-8") as f:
        return yaml.safe_load(f)


def tag_new_town(adm_nm: str, sgg: str, towns: dict[str, list[str]]) -> str:
    """동 이름·시군구에 신도시 keyword 포함되면 태그 부여 (gen_1/gen_2/gen_3/seoul_renew)."""
    text = f"{sgg} {adm_nm}"
    tags = []
    for tag, kws in towns.items():
        for kw in kws:
            if kw in text:
                tags.append(tag)
                break
    return ",".join(tags) if tags else ""


def build_lists(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    out = {}

    # 1. 종합 점수 (이미 정렬됨)
    s = df.sort_values("score", ascending=False).head(10).copy()
    s["score_adj"] = s["score"]
    s["criterion"] = "score"
    s["rank_in_list"] = range(1, len(s) + 1)
    out["score"] = s

    # 2. score × 자차 페널티 (t_norm 가중 강화: score * (1 - 0.3*(1-t_norm)))
    df2 = df.copy()
    df2["score_adj"] = df2["score"] * (1 - 0.3 * (1 - df2["t_norm"]))
    s2 = df2.sort_values("score_adj", ascending=False).head(10)
    s2["criterion"] = "commute_priority"
    s2["rank_in_list"] = range(1, len(s2) + 1)
    out["commute"] = s2

    # 3. 신도시만
    nt = df[df["town_tag"] != ""].sort_values("score", ascending=False).head(10).copy()
    nt["score_adj"] = nt["score"]
    nt["criterion"] = "new_town"
    nt["rank_in_list"] = range(1, len(nt) + 1)
    out["new_town"] = nt

    # 4. med_desert 플래그 동
    md = df[df["med_desert_flag"] == True].sort_values("score", ascending=False).head(10).copy()
    md["score_adj"] = md["score"]
    md["criterion"] = "med_desert"
    md["rank_in_list"] = range(1, len(md) + 1)
    out["desert"] = md

    # 5. 인구당 의원 적음 (density_per_10k 낮은 + p_raw 높은)
    df5 = df.copy()
    # 인구 충분(상위 50%)한 동 중 density 낮은 순
    p_med = df5["p_raw"].median()
    df5 = df5[df5["p_raw"] >= p_med]
    df5 = df5.sort_values("density_per_10k", ascending=True).head(10).copy()
    df5["score_adj"] = df5["score"]
    df5["criterion"] = "low_density"
    df5["rank_in_list"] = range(1, len(df5) + 1)
    out["p_density"] = df5

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scores", default=str(DATA_SCORED / f"scores_{TODAY}.parquet"))
    ap.add_argument("--sido", default=None, help="필터 (예: '서울특별시')")
    ap.add_argument("--t-raw-max", type=int, default=T_RAW_MAX)
    args = ap.parse_args()

    src = Path(args.scores)
    if not src.exists():
        candidates = sorted(DATA_SCORED.glob("scores_*.parquet"))
        src = candidates[-1]
    log.info("loading %s", src)
    df = pd.read_parquet(src)

    # 자차 50분 컷
    n0 = len(df)
    df = df[df["t_raw"] <= args.t_raw_max].copy()
    log.info("filter t_raw <= %d: %d → %d", args.t_raw_max, n0, len(df))

    if args.sido:
        df = df[df["sido"] == args.sido].copy()
        log.info("filter sido=%s: %d", args.sido, len(df))

    # 신도시 태그
    towns = load_new_towns()
    df["town_tag"] = df.apply(lambda r: tag_new_town(r["adm_nm"], r["sgg"], towns), axis=1)
    log.info("new_town tagged: %d", (df["town_tag"] != "").sum())

    # 5개 기준 리스트
    lists = build_lists(df)

    # 모두 합쳐서 한 CSV
    rows = []
    for key, sub in lists.items():
        cols = [c for c in EXPORT_COLS if c in sub.columns]
        rows.append(sub[cols])
    full = pd.concat(rows, ignore_index=True)

    suffix = f"_{args.sido}" if args.sido else ""
    out = DATA_SCORED / f"narrow_top10{suffix}_{TODAY}.csv"
    full.to_csv(out, index=False, encoding="utf-8-sig")
    log.info("wrote %s (%d rows × 5 criteria)", out, len(full))

    # 콘솔 요약
    print()
    for key, sub in lists.items():
        print(f"=== [{key}] (n={len(sub)}) ===")
        cols_show = ["rank_in_list", "sido", "sgg", "adm_nm", "score", "score_adj",
                     "t_raw", "n_clinic", "town_tag"]
        cols_show = [c for c in cols_show if c in sub.columns]
        print(sub[cols_show].to_string(index=False))
        print()


if __name__ == "__main__":
    main()

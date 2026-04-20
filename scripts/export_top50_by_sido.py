"""서울/경기 분리 Top50 CSV export.

필터: 자차 통근시간(t_raw) > 50분 동은 제외 (Kakao 999 outlier 포함).
필터 후 sido 안에서 score 내림차순 재순위.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from config.constants import DATA_SCORED

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("export")

T_RAW_MAX = 50  # 자차 통근 50분 컷
TODAY = date.today().isoformat()

OUT_DIR = DATA_SCORED  # data/scored/
EXPORT_COLS = [
    "rank_sido", "sido", "sgg", "adm_nm", "score",
    "c_norm", "p_norm", "t_norm",
    "n_clinic", "n_clinic_500m", "n_clinic_1km", "n_clinic_2km",
    "p_raw", "pop_total", "pop_40plus", "ratio_40plus",
    "catchment_pop_1_5km", "density_per_10k",
    "t_raw", "t_transit",
    "nearest_station", "station_dist_m", "n_clinic_station_500m",
    "med_desert_flag", "centroid_mismatch_flag", "suburban_cluster_flag",
    "adm_cd", "adm_cd10",
]


def main():
    src = OUT_DIR / f"scores_{TODAY}.parquet"
    if not src.exists():
        # latest fallback
        candidates = sorted(OUT_DIR.glob("scores_*.parquet"))
        src = candidates[-1]
    log.info("loading %s", src)
    df = pd.read_parquet(src)

    n_before = len(df)
    df = df[df["t_raw"] <= T_RAW_MAX].copy()
    log.info("filter t_raw <= %dmin: %d -> %d (-%d)",
             T_RAW_MAX, n_before, len(df), n_before - len(df))

    # Top50 sido별 분리 + 재순위
    for sido_name, file_tag in [("서울특별시", "seoul"), ("경기도", "gyeonggi")]:
        sub = df[df["sido"] == sido_name].sort_values("score", ascending=False).reset_index(drop=True)
        sub["rank_sido"] = sub.index + 1
        top50 = sub.head(50)

        out = OUT_DIR / f"top50_{file_tag}_{TODAY}.csv"
        cols = [c for c in EXPORT_COLS if c in top50.columns]
        top50[cols].to_csv(out, index=False, encoding="utf-8-sig")
        log.info("wrote %s (%d rows)", out, len(top50))

        # 콘솔 요약
        agg = top50[["score", "c_norm", "p_norm", "t_norm",
                     "n_clinic", "p_raw", "t_raw"]].mean().round(3)
        log.info("  %s 평균: %s", file_tag, agg.to_dict())


if __name__ == "__main__":
    main()

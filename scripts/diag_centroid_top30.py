"""Top30 centroid 진단.

각 동의 "중심점"이 실제 1km 반경 의료상권을 잘 잡고 있는지 진단:
- 인구 가중 중심점 (lat_pop, lon_pop) — 운영값
- 기하 중심점 (lat, lon) — 폴리곤 무게중심
- 둘 사이 거리 (m): 클수록 동 모양에 비해 인구가 한쪽으로 쏠림
- pop_weighted=False면 폴백 (인구 0 → 기하 중심)
- n_clinic_500m / 1km / 2km: 중심점 기준 동심원 의원 분포
  → 500m 적고 2km 많으면 중심점이 의료상권 밖
  → 1km 안에 충분히 잡히면 OK
- centroid_mismatch_flag, suburban_cluster_flag

사용:
    python -m scripts.diag_centroid_top30 [--date 2026-04-22]
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from config.constants import DATA_CACHE, DATA_CLEANED, DATA_SCORED


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-22")
    args = parser.parse_args()

    top30 = pd.read_parquet(DATA_SCORED / f"top30_{args.date}.parquet")
    pop_centroid = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")
    geom_centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")

    pop_centroid["adm_cd"] = pop_centroid["adm_cd"].astype(str)
    geom_centroid["adm_cd"] = geom_centroid["adm_cd"].astype(str)
    top30["adm_cd"] = top30["adm_cd"].astype(str)

    df = top30.merge(
        pop_centroid[["adm_cd", "lat_pop", "lon_pop", "pop_weighted"]],
        on="adm_cd", how="left",
    )
    df = df.merge(
        geom_centroid[["adm_cd", "lat", "lon"]].rename(
            columns={"lat": "lat_geom", "lon": "lon_geom"}
        ),
        on="adm_cd", how="left",
    )

    # 인구 가중 ↔ 기하 중심 거리 (m)
    dlat = (df["lat_pop"] - df["lat_geom"]) * 111_000
    dlon = (df["lon_pop"] - df["lon_geom"]) * 111_000 * np.cos(np.radians(df["lat_geom"]))
    df["shift_m"] = np.sqrt(dlat * dlat + dlon * dlon).round(0).astype(int)

    df["pop_weighted"] = df["pop_weighted"].fillna(False)

    # 표시 컬럼 정리
    show = df[[
        "rank", "sgg", "adm_nm",
        "score",
        "n_clinic_500m", "n_clinic_1km", "n_clinic_2km",
        "shift_m", "pop_weighted",
        "centroid_mismatch_flag", "suburban_cluster_flag",
        "catchment_pop_1_5km",
    ]].copy()
    show["centroid_mismatch_flag"] = show["centroid_mismatch_flag"].map({True: "Y", False: ""})
    show["suburban_cluster_flag"] = show["suburban_cluster_flag"].map({True: "Y", False: ""})
    show["pop_weighted"] = show["pop_weighted"].map({True: "", False: "FALLBACK"})
    show.columns = ["순위", "시군구", "동", "점수",
                    "500m", "1km", "2km",
                    "이동(m)", "폴백",
                    "MIS", "SUB",
                    "1.5km인구"]

    # 1km 카운트 기준 정렬: 적은 동부터 (의심)
    show = show.sort_values("1km").reset_index(drop=True)
    print("=" * 110)
    print("[Top30 centroid 진단] — 1km 안 의원 적은 순 (의심 후보가 위)")
    print("MIS=centroid_mismatch_flag (500m=0 AND 2km≥50)")
    print("SUB=suburban_cluster_flag (동내≥10 AND 1km≤5, 신도시 상가 밀집)")
    print("이동(m): 인구 가중 ↔ 기하 중심점 거리. 클수록 동 모양 대비 인구 쏠림 큼")
    print("=" * 110)
    print(show.to_string(index=False))
    print()

    # 요약
    n_mis = (df["centroid_mismatch_flag"] == True).sum()
    n_sub = (df["suburban_cluster_flag"] == True).sum()
    n_fallback = (df["pop_weighted"] == False).sum()
    n_500_zero = (df["n_clinic_500m"] == 0).sum()
    n_1km_lt_5 = (df["n_clinic_1km"] < 5).sum()
    print("=" * 60)
    print(f"Top30 요약")
    print("=" * 60)
    print(f"  centroid_mismatch_flag : {n_mis}")
    print(f"  suburban_cluster_flag  : {n_sub}")
    print(f"  중심점 폴백 (인구 0)   : {n_fallback}")
    print(f"  중심점 500m 의원 = 0   : {n_500_zero}")
    print(f"  중심점 1km 의원 < 5    : {n_1km_lt_5}")
    print(f"  shift(m): 중앙 {df['shift_m'].median():.0f} · p90 {df['shift_m'].quantile(0.9):.0f} · max {df['shift_m'].max()}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

"""Top30 인구 가중 중심점이 상가 영역(1·2층) 안에 있는지 진단.

사용자 가설: "중심점은 적어도 토지 분류상 상가가 들어올 수 있는 위치여야".

검증:
- 각 Top30 동의 인구 가중 중심점(lat_pop, lon_pop)이 shops_hull(1·2층 상가 convex hull) 내부인가?
- 외부면 hull boundary까지 거리(m)
- 인구 가중 중심점 ↔ 상가 mean 거리(m)
- 만약 보정한다면 어떻게 변할지 (shops_mean으로 교체했을 때 1km 의원 카운트 변화 — 향후 분석)

사용:
    python -m scripts.diag_centroid_vs_shops [--date 2026-04-22]
"""
from __future__ import annotations

import argparse

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import Point

from config.constants import DATA_CACHE, DATA_CLEANED, DATA_SCORED, EPSG_KOREA, EPSG_WGS84


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-22")
    args = parser.parse_args()

    top30 = pd.read_parquet(DATA_SCORED / f"top30_{args.date}.parquet")
    shops = pd.read_parquet(DATA_CLEANED / "shops_by_dong.parquet")
    pop_centroid = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")

    top30["adm_cd10"] = top30["adm_cd10"].astype(str)
    shops["adm_cd10"] = shops["adm_cd10"].astype(str)
    pop_centroid["adm_cd"] = pop_centroid["adm_cd"].astype(str)
    top30["adm_cd"] = top30["adm_cd"].astype(str)

    df = top30.merge(
        shops[["adm_cd10", "n_shops_floor12", "shops_lat_mean", "shops_lon_mean", "shops_hull_wkt"]],
        on="adm_cd10", how="left",
    )
    df = df.merge(
        pop_centroid[["adm_cd", "lat_pop", "lon_pop"]],
        on="adm_cd", how="left",
    )

    # hull WKT → shapely (없는 동은 None — 상가 3개 미만)
    def parse_hull(w):
        if w is None or (isinstance(w, float) and np.isnan(w)):
            return None
        try:
            return wkt.loads(w)
        except Exception:
            return None
    df["hull"] = df["shops_hull_wkt"].apply(parse_hull)

    # WGS84 → EPSG:5179 변환 (거리 m 단위 측정용)
    pop_pts_4326 = gpd.GeoSeries(
        [Point(lo, la) for lo, la in zip(df["lon_pop"], df["lat_pop"])],
        crs=EPSG_WGS84,
    )
    pop_pts_5179 = pop_pts_4326.to_crs(EPSG_KOREA)

    shops_pts_4326 = gpd.GeoSeries(
        [Point(lo, la) if pd.notna(lo) else None
         for lo, la in zip(df["shops_lon_mean"], df["shops_lat_mean"])],
        crs=EPSG_WGS84,
    )
    shops_pts_5179 = shops_pts_4326.to_crs(EPSG_KOREA)

    # hull → 5179 (각 행마다)
    hull_5179 = []
    for h in df["hull"]:
        if h is None:
            hull_5179.append(None)
        else:
            gh = gpd.GeoSeries([h], crs=EPSG_WGS84).to_crs(EPSG_KOREA).iloc[0]
            hull_5179.append(gh)

    # within hull?
    inside = []
    dist_to_hull = []
    for pt, hu in zip(pop_pts_5179, hull_5179):
        if hu is None:
            inside.append(None)
            dist_to_hull.append(None)
        elif hu.contains(pt):
            inside.append(True)
            dist_to_hull.append(0.0)
        else:
            inside.append(False)
            dist_to_hull.append(pt.distance(hu.boundary))

    # pop ↔ shops distance
    pop_to_shops = []
    for p, s in zip(pop_pts_5179, shops_pts_5179):
        if s is None or s.is_empty:
            pop_to_shops.append(None)
        else:
            pop_to_shops.append(p.distance(s))

    df["인구중심_상가hull내"] = [
        "Y" if i is True else ("N" if i is False else "-") for i in inside
    ]
    df["hull까지_m"] = [int(d) if d is not None else None for d in dist_to_hull]
    df["인구↔상가_m"] = [int(d) if d is not None else None for d in pop_to_shops]

    show = df[[
        "rank", "sgg", "adm_nm",
        "n_shops_floor12",
        "인구중심_상가hull내", "hull까지_m", "인구↔상가_m",
        "n_clinic_500m", "n_clinic_1km",
    ]].copy()
    show.columns = ["순위", "시군구", "동", "1·2층상가",
                    "hull내?", "hull까지(m)", "인구↔상가(m)",
                    "500m의원", "1km의원"]
    # hull 밖인 동을 위로 (큰 hull까지 순)
    show = show.sort_values(["hull내?", "hull까지(m)"], ascending=[True, False]).reset_index(drop=True)

    print("=" * 100)
    print("[Top30] 인구 가중 중심점이 1·2층 상가 hull 안에 있는가?")
    print("hull내?=N: 중심점이 상가 영역 밖 (주거지·공원·산·녹지 가능성)")
    print("hull까지(m): 가장 가까운 상가 hull 경계까지 거리. 0이면 hull 안.")
    print("인구↔상가(m): 인구 가중 중심점 ↔ 상가 1·2층 평균 좌표 거리.")
    print("=" * 100)
    print(show.to_string(index=False))
    print()

    n_outside = (df["인구중심_상가hull내"] == "N").sum()
    n_no_hull = (df["인구중심_상가hull내"] == "-").sum()
    far = df[df["hull까지_m"].notna() & (df["hull까지_m"] > 0)]["hull까지_m"]
    pop_shops = df["인구↔상가_m"].dropna()

    print("=" * 60)
    print("Top30 요약")
    print("=" * 60)
    print(f"  중심점이 상가 hull 밖 (N) : {n_outside} / {len(df)}")
    print(f"  shops 데이터 없음 (-)     : {n_no_hull}")
    if len(far) > 0:
        print(f"  hull 밖 동들 거리 — 중앙 {far.median():.0f}m, p90 {far.quantile(0.9):.0f}m, max {far.max():.0f}m")
    print(f"  인구↔상가 거리 — 중앙 {pop_shops.median():.0f}m, p90 {pop_shops.quantile(0.9):.0f}m, max {pop_shops.max():.0f}m")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

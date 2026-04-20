"""Sensitivity: 역세권 동의 중심점을 역으로 옮기면 점수가 어떻게 변하나.

가설: 역세권(<=500m) 동에서 pop-centroid → 역으로 중심점 이동하면
- catchment_pop 변화 (배후세대 다르게 잡힘) → p_raw 변화
- n_within_radius 변화 (역 주변 의원 밀집) → c_raw 변화
두 효과가 partially cancel 또는 reinforce 할 수 있음.

단계
1. pop-centroid → 최근접역 거리 분포
2. Top30 역세권 분포
3. 역세권 동만 역 좌표로 catchment + within_radius 재계산
4. 새 P_raw, C_raw → 정규화 → 새 score → rank delta
"""
from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.mask import mask as rio_mask
from shapely.geometry import Point
from shapely.ops import transform as shapely_transform

from config.constants import (
    CATCHMENT_RADIUS_M,
    COMPETITION_RADIUS_M,
    DATA_CACHE,
    DATA_CLEANED,
    DATA_RAW,
    EPSG_KOREA,
    EPSG_WGS84,
    W_COMMUTE,
    W_COMP_DENSITY,
    W_COMP_RADIUS,
    W_COMPETITION,
    W_POPULATION,
)
from scoring.normalize import percentile_rank, percentile_rank_inverted

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sensitivity")

WORLDPOP = DATA_RAW / "worldpop" / "kor_ppp_2020.tif"
STATION_THRESHOLD_M = 500

_TO_5179 = Transformer.from_crs(EPSG_WGS84, EPSG_KOREA, always_xy=True).transform
_TO_4326 = Transformer.from_crs(EPSG_KOREA, EPSG_WGS84, always_xy=True).transform


def buffer_polygon(lon, lat, radius_m):
    x, y = _TO_5179(lon, lat)
    buf = Point(x, y).buffer(radius_m, resolution=32)
    return shapely_transform(lambda a, b, z=None: _TO_4326(a, b), buf)


def catchment_pop(lon, lat, src, radius_m):
    buf = buffer_polygon(lon, lat, radius_m)
    try:
        clipped, _ = rio_mask(src, [buf.__geo_interface__],
                              crop=True, all_touched=True, filled=False)
    except ValueError:
        return 0.0
    arr = clipped[0]
    v = np.where(~arr.mask, arr.data, 0.0) if hasattr(arr, "mask") else arr
    v = np.where(np.isfinite(v) & (v > 0), v, 0.0)
    return float(v.sum())


def main():
    log.info("=== load data ===")
    pop_c = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")
    geom = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")
    stations = pd.read_parquet(DATA_CACHE / "subway_stations.parquet")
    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")
    scored = pd.read_parquet(DATA_CACHE.parent / "scored" / "scores_2026-04-20.parquet")

    pop_c["adm_cd"] = pop_c["adm_cd"].astype(str)
    geom["adm_cd"] = geom["adm_cd"].astype(str)
    scored["adm_cd"] = scored["adm_cd"].astype(str)

    df = pop_c.merge(geom[["adm_cd", "sido", "sgg", "adm_nm"]], on="adm_cd", how="left")

    # pop-centroid 5179
    coords_5179 = np.array([_TO_5179(lon, lat) for lon, lat in zip(df["lon_pop"], df["lat_pop"])])
    df["x_pop"] = coords_5179[:, 0]
    df["y_pop"] = coords_5179[:, 1]

    st_x = stations["x_5179"].to_numpy()
    st_y = stations["y_5179"].to_numpy()

    # 1. nearest station per dong (using pop-centroid)
    log.info("=== 1. station distance ===")
    dx = df["x_pop"].to_numpy()[:, None] - st_x[None, :]
    dy = df["y_pop"].to_numpy()[:, None] - st_y[None, :]
    d2 = dx * dx + dy * dy
    idx = d2.argmin(axis=1)
    df["station_dist_m"] = np.sqrt(d2[np.arange(len(df)), idx]).astype(int)
    df["nearest_station"] = stations["name"].values[idx]
    df["station_lon"] = stations["lon"].values[idx]
    df["station_lat"] = stations["lat"].values[idx]
    df["station_x"] = stations["x_5179"].values[idx]
    df["station_y"] = stations["y_5179"].values[idx]

    dist = df["station_dist_m"]
    log.info("  분포 (n=%d):  <=300m=%d  300-500m=%d  500-1000m=%d  1000m+=%d",
             len(df),
             (dist <= 300).sum(),
             ((dist > 300) & (dist <= 500)).sum(),
             ((dist > 500) & (dist <= 1000)).sum(),
             (dist > 1000).sum())
    log.info("  median=%dm  mean=%dm  max=%dm",
             int(dist.median()), int(dist.mean()), int(dist.max()))

    # 2. Top30 분포 (scored already has station_dist_m)
    top30 = scored[scored["rank"] <= 30]
    log.info("=== 2. Top30 station distance ===")
    log.info("  <=300m=%d  300-500m=%d  500m+=%d (역세권 아님)",
             (top30["station_dist_m"] <= 300).sum(),
             ((top30["station_dist_m"] > 300) & (top30["station_dist_m"] <= 500)).sum(),
             (top30["station_dist_m"] > 500).sum())

    # 3. 역세권 동 (pop-centroid 기준 ≤ STATION_THRESHOLD_M)
    near_mask = df["station_dist_m"] <= STATION_THRESHOLD_M
    near = df[near_mask].copy()
    log.info("=== 3. 역세권 dongs (pop->station 이동): n=%d ===", len(near))

    # 4. 역 좌표로 catchment + within_radius 재계산
    log.info("=== 4. recompute catchment + n_within_radius for 역세권 ===")
    new_catchments = []
    with rasterio.open(WORLDPOP) as src:
        for _, row in near.iterrows():
            cp = catchment_pop(row["station_lon"], row["station_lat"], src, CATCHMENT_RADIUS_M)
            new_catchments.append(cp)
    near["catchment_station"] = new_catchments

    # clinic 5179 좌표 (없으면 변환)
    if "x_5179" not in clinics.columns:
        gdf_cl = gpd.GeoDataFrame(
            clinics,
            geometry=gpd.points_from_xy(
                pd.to_numeric(clinics["XPos"]), pd.to_numeric(clinics["YPos"])
            ),
            crs=EPSG_WGS84,
        ).to_crs(EPSG_KOREA)
        clinics["x_5179"] = gdf_cl.geometry.x.values
        clinics["y_5179"] = gdf_cl.geometry.y.values
    cl_x = clinics["x_5179"].to_numpy(dtype="float32")
    cl_y = clinics["y_5179"].to_numpy(dtype="float32")

    # n_within_radius (station 기준)
    sx = near["station_x"].to_numpy(dtype="float32")
    sy = near["station_y"].to_numpy(dtype="float32")
    ddx = sx[:, None] - cl_x[None, :]
    ddy = sy[:, None] - cl_y[None, :]
    n_within_station = ((ddx * ddx + ddy * ddy) <= COMPETITION_RADIUS_M ** 2).sum(axis=1)
    near["n_within_radius_station"] = n_within_station

    # 5. 새 c_raw, p_raw 만들고 전체 재정규화
    log.info("=== 5. recompute scores ===")
    full = scored.merge(
        near[["adm_cd", "catchment_station", "n_within_radius_station"]],
        on="adm_cd", how="left"
    )

    # 신 catchment / within_radius (역세권 아닌 건 기존값 유지)
    catch_old = pop_c.set_index("adm_cd")["catchment_pop_1_5km"]
    full["catchment_new"] = full["catchment_station"].fillna(full["adm_cd"].map(catch_old))

    # ratio_40plus는 기존 p_raw / catchment_old 로 역산 가능 (p_raw = catch_old × ratio)
    catch_old_aligned = full["adm_cd"].map(catch_old).astype(float)
    ratio = full["p_raw"] / catch_old_aligned.where(catch_old_aligned > 0)
    full["p_raw_new"] = full["catchment_new"] * ratio
    # 역세권 NaN 처리 (원래 p_raw 유지)
    full["p_raw_new"] = full["p_raw_new"].fillna(full["p_raw"])

    # n_within_radius 새로 (역세권은 station 기준, 비역세권은 기존)
    # 기존 n_within_radius는 scored에 없을 수 있으므로 c_raw 역산
    # c_raw = w_dens * (n_clinic / (pop_total/10000)) + w_rad * n_within_radius
    # density 항은 동 단위 n_clinic, pop_total — centroid 이동과 무관
    # 따라서 c_raw 변화 = w_rad * (n_within_new - n_within_old)
    # 하지만 n_within_old는 직접 없음. 일단 단순화: 역세권 동에서만 n_within_radius_station이 있고,
    # density 항은 동일하므로 c_raw_new = c_raw + w_rad * (n_within_station - n_within_old)
    # n_within_old 없이 진행하려면 추정 필요 → 직접 계산하자

    # 모든 동에 대해 pop-centroid 기준 n_within_radius 계산 (baseline 비교용)
    px = df["x_pop"].to_numpy(dtype="float32")
    py = df["y_pop"].to_numpy(dtype="float32")
    pdx = px[:, None] - cl_x[None, :]
    pdy = py[:, None] - cl_y[None, :]
    n_within_old = ((pdx * pdx + pdy * pdy) <= COMPETITION_RADIUS_M ** 2).sum(axis=1)
    df["n_within_radius_old"] = n_within_old

    full = full.merge(df[["adm_cd", "n_within_radius_old"]], on="adm_cd", how="left")
    # 역세권 동: station 기준 / 비역세권: pop-centroid 기준 (기존)
    full["n_within_new"] = full["n_within_radius_station"].fillna(full["n_within_radius_old"])

    # density 항은 c_raw - w_rad * n_within_old 로 역산
    c_density = full["c_raw"] - W_COMP_RADIUS * full["n_within_radius_old"]
    full["c_raw_new"] = c_density + W_COMP_RADIUS * full["n_within_new"]

    # 정규화 + 가중합
    full["c_norm_new"] = percentile_rank_inverted(full["c_raw_new"])
    full["p_norm_new"] = percentile_rank(full["p_raw_new"])
    full["score_new"] = (
        W_COMPETITION * full["c_norm_new"]
        + W_POPULATION * full["p_norm_new"]
        + W_COMMUTE * full["t_norm"]
    )
    full["rank_new"] = full["score_new"].rank(method="min", ascending=False).astype(int)

    # 6. 비교
    log.info("=== 6. rank changes ===")
    in_old = set(full[full["rank"] <= 30]["adm_cd"])
    in_new = set(full[full["rank_new"] <= 30]["adm_cd"])
    enter = in_new - in_old
    exit_ = in_old - in_new
    log.info("  Top30 enter=%d  exit=%d", len(enter), len(exit_))
    # simple rank corr via pearson on ranks
    log.info("  rank corr (pearson on ranks) = %.4f",
             full["rank"].corr(full["rank_new"]))

    full["rank_delta"] = full["rank_new"] - full["rank"]
    log.info("  rank_delta: |delta|>=5 = %d개  |delta|>=20 = %d개",
             (full["rank_delta"].abs() >= 5).sum(),
             (full["rank_delta"].abs() >= 20).sum())

    # 변동 동 표
    show_cols = ["sido", "sgg", "adm_nm", "rank", "rank_new", "rank_delta",
                 "c_raw", "c_raw_new", "p_raw", "p_raw_new",
                 "score", "score_new"]
    changed = full[full["adm_cd"].isin(enter | exit_)].sort_values("rank_new")
    if len(changed):
        log.info("\n=== Top30 진입/퇴출 ===")
        print(changed[show_cols].to_string(index=False))

    # 역세권 Top30 변화 (scored already has station_dist_m, nearest_station)
    top30_near = full[(full["rank"] <= 30) & full["adm_cd"].isin(near["adm_cd"])].copy()
    top30_near = top30_near.sort_values("rank")
    log.info("\n=== Top30 역세권 동들 (rank 변화) ===")
    cols = ["rank", "rank_new", "adm_nm", "nearest_station", "station_dist_m",
            "c_raw", "c_raw_new", "p_raw", "p_raw_new", "score", "score_new"]
    print(top30_near[cols].to_string(index=False))


if __name__ == "__main__":
    main()

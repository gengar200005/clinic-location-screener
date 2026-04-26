"""ABLATION: 중심점을 인구 가중 → 1·2층 상가 가중으로 교체했을 때 Top30 변화.

전체 동(약 652)에 대해 새 catchment_pop · 1.5km 의사 수 · density 재계산 →
c_raw·c_norm·p_raw·p_norm·score 재계산 → 새 Top30 비교.

shops 좌표 없는 동은 기존 인구 가중 좌표로 fallback (변화 없음).
t_raw는 ODSay 캐시 없으니 그대로 사용 (좌표 1km 이동의 통근시간 영향 미미).

사용:
    python -m scripts.ablation_centroid_shops [--date 2026-04-22]
"""
from __future__ import annotations

import argparse
import logging

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
    DATA_SCORED,
    EPSG_KOREA,
    EPSG_WGS84,
    W_COMMUTE,
    W_COMP_DENSITY,
    W_COMP_RADIUS,
    W_COMP_STATION,
    W_COMP_SUBCLUSTER,
    W_COMPETITION,
    W_POPULATION,
)
from scoring.normalize import percentile_rank, percentile_rank_inverted

WORLDPOP_PATH = DATA_RAW / "worldpop" / "kor_ppp_2020.tif"

_TO_5179 = Transformer.from_crs(EPSG_WGS84, EPSG_KOREA, always_xy=True).transform
_TO_4326 = Transformer.from_crs(EPSG_KOREA, EPSG_WGS84, always_xy=True).transform


def catchment_pop(lon: float, lat: float, raster, radius_m: float) -> float:
    """중심점 반경 radius_m 내 WorldPop 픽셀 합."""
    x, y = _TO_5179(lon, lat)
    buf_5179 = Point(x, y).buffer(radius_m, resolution=32)
    buf_wgs = shapely_transform(lambda a, b, z=None: _TO_4326(a, b), buf_5179)
    try:
        clipped, _ = rio_mask(
            raster, [buf_wgs.__geo_interface__],
            crop=True, all_touched=True, filled=False,
        )
    except ValueError:
        return 0.0
    arr = clipped[0]
    if hasattr(arr, "mask"):
        v = np.where(~arr.mask, arr.data, 0.0)
    else:
        v = arr
    v = np.where(np.isfinite(v) & (v > 0), v, 0.0)
    return float(v.sum())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-22")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    log = logging.getLogger("ablation")

    scores = pd.read_parquet(DATA_SCORED / f"scores_{args.date}.parquet")
    shops = pd.read_parquet(DATA_CLEANED / "shops_by_dong.parquet")
    pop_centroid = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")
    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")

    scores["adm_cd"] = scores["adm_cd"].astype(str)
    scores["adm_cd10"] = scores["adm_cd10"].astype(str)
    shops["adm_cd10"] = shops["adm_cd10"].astype(str)
    pop_centroid["adm_cd"] = pop_centroid["adm_cd"].astype(str)

    df = scores.merge(
        shops[["adm_cd10", "shops_lat_mean", "shops_lon_mean"]],
        on="adm_cd10", how="left",
    )
    df = df.merge(
        pop_centroid[["adm_cd", "lat_pop", "lon_pop"]],
        on="adm_cd", how="left",
    )
    n_with_shops = df["shops_lat_mean"].notna().sum()
    log.info(f"shops 좌표 있는 동: {n_with_shops} / {len(df)}")

    # 새 중심점: shops_*_mean (없으면 lat_pop/lon_pop fallback)
    df["lat_new"] = df["shops_lat_mean"].fillna(df["lat_pop"])
    df["lon_new"] = df["shops_lon_mean"].fillna(df["lon_pop"])

    # ── 1. 새 catchment_pop ──
    log.info("recomputing catchment pop for all dongs...")
    with rasterio.open(WORLDPOP_PATH) as src:
        cps = []
        for lon, lat in zip(df["lon_new"], df["lat_new"]):
            cps.append(catchment_pop(lon, lat, src, CATCHMENT_RADIUS_M))
    df["catchment_pop_1_5km_new"] = cps
    df["catchment_pop_40plus_new"] = (
        df["catchment_pop_1_5km_new"] * df["ratio_40plus"]
    )

    # ── 2. 새 좌표 EPSG:5179 변환 ──
    log.info("computing distance matrices...")
    pts_4326 = gpd.GeoSeries(
        [Point(lo, la) for lo, la in zip(df["lon_new"], df["lat_new"])],
        crs=EPSG_WGS84,
    )
    pts_5179 = pts_4326.to_crs(EPSG_KOREA)
    dong_x = np.array([p.x for p in pts_5179], dtype="float32")
    dong_y = np.array([p.y for p in pts_5179], dtype="float32")

    # 클리닉 5179 좌표 (전체 + 내과만)
    cl_gdf = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(
            pd.to_numeric(clinics["XPos"]),
            pd.to_numeric(clinics["YPos"]),
        ),
        crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    cl_x = cl_gdf.geometry.x.values.astype("float32")
    cl_y = cl_gdf.geometry.y.values.astype("float32")
    is_internal = clinics["yadmNm"].str.contains("내과", na=False).to_numpy()
    drs_all = (
        pd.to_numeric(clinics.get("drTotCnt", 0), errors="coerce")
        .fillna(0).astype(int).to_numpy()
    )

    cl_x_med = cl_x[is_internal]
    cl_y_med = cl_y[is_internal]
    drs_med = drs_all[is_internal]

    # 1.5km 거리 행렬 (n_dong x n_clinic_med)
    dx = dong_x[:, None] - cl_x_med[None, :]
    dy = dong_y[:, None] - cl_y_med[None, :]
    d2 = dx * dx + dy * dy
    within_15 = d2 <= (COMPETITION_RADIUS_M ** 2)
    df["n_doctors_within_radius_med_new"] = (within_15 * drs_med[None, :]).sum(axis=1).astype(int)
    # 디스플레이용: 1km / 500m도
    within_1k = d2 <= (1000 ** 2)
    within_500 = d2 <= (500 ** 2)
    df["n_doctors_1km_med_new"] = (within_1k * drs_med[None, :]).sum(axis=1).astype(int)
    df["n_doctors_500m_med_new"] = (within_500 * drs_med[None, :]).sum(axis=1).astype(int)

    # ── 3. subcluster max (동 내 1.5km 안 sliding 500m disk) ──
    log.info("recomputing subcluster max...")
    n = len(df)
    max_drs = np.zeros(n, dtype=int)
    r2_cluster = 500.0 ** 2
    for i in range(n):
        in_outer = within_15[i]
        if not in_outer.any():
            continue
        cx = cl_x_med[in_outer]
        cy = cl_y_med[in_outer]
        cd = drs_med[in_outer]
        ax = cx[:, None] - cx[None, :]
        ay = cy[:, None] - cy[None, :]
        within_c = (ax * ax + ay * ay) <= r2_cluster
        max_drs[i] = int((within_c * cd[None, :]).sum(axis=1).max())
    df["n_doctors_subcluster_max_med_new"] = max_drs

    # ── 4. density ──
    safe_pop = df["catchment_pop_40plus_new"].replace(0, np.nan)
    df["density_per_10k_med_new"] = df["n_doctors_med"] / (safe_pop / 10_000)
    df["density_per_10k_med_new"] = df["density_per_10k_med_new"].fillna(0)

    # ── 5. c_raw ──
    df["c_raw_new"] = (
        W_COMP_DENSITY * df["density_per_10k_med_new"]
        + W_COMP_RADIUS * df["n_doctors_within_radius_med_new"]
        + W_COMP_STATION * df["n_doctors_station_500m_med"].fillna(0)
        + W_COMP_SUBCLUSTER * df["n_doctors_subcluster_max_med_new"]
    )

    # ── 6. p_raw ──
    df["p_raw_new"] = df["catchment_pop_40plus_new"]

    # ── 7. 정규화 + score ──
    df["c_norm_new"] = percentile_rank_inverted(df["c_raw_new"])
    df["p_norm_new"] = percentile_rank(df["p_raw_new"])
    # t_norm 그대로
    df["score_new"] = (
        W_COMPETITION * df["c_norm_new"]
        + W_POPULATION * df["p_norm_new"]
        + W_COMMUTE * df["t_norm"]
    )

    df_sorted = df.sort_values("score_new", ascending=False).reset_index(drop=True)
    df_sorted["rank_new"] = df_sorted.index + 1

    top30_new = df_sorted.head(30).copy()
    top30_old = scores.sort_values("score", ascending=False).head(30)
    top30_old_set = set(top30_old["adm_cd"])
    top30_new_set = set(top30_new["adm_cd"])

    overlap = top30_old_set & top30_new_set
    newly_in = top30_new_set - top30_old_set
    dropped = top30_old_set - top30_new_set

    print()
    print("=" * 80)
    print("[ABLATION] 중심점: 인구 가중 → 1·2층 상가 가중")
    print("=" * 80)
    print(f"  Top30 overlap: {len(overlap)} / 30")
    print(f"  새로 진입: {len(newly_in)}, 탈락: {len(dropped)}")
    print()

    # 기존 동들의 rank 매핑
    old_rank_map = scores.sort_values("score", ascending=False).reset_index(drop=True)
    old_rank_map["old_rank"] = old_rank_map.index + 1
    old_rank_map = old_rank_map.set_index("adm_cd")["old_rank"]

    # ── 새 Top30 ──
    print("=" * 100)
    print("[새 Top30] 중심점 = 1·2층 상가 mean")
    print("=" * 100)
    top30_new["old_rank"] = top30_new["adm_cd"].map(old_rank_map).fillna(999).astype(int)
    cols = ["rank_new", "old_rank", "sgg", "adm_nm", "score_new",
            "c_norm_new", "p_norm_new", "t_norm",
            "n_doctors_500m_med_new", "n_doctors_1km_med_new",
            "n_doctors_within_radius_med_new",
            "catchment_pop_40plus_new"]
    print(top30_new[cols].rename(columns={
        "rank_new": "신rank", "old_rank": "구rank",
        "score_new": "score", "c_norm_new": "c", "p_norm_new": "p", "t_norm": "t",
        "n_doctors_500m_med_new": "500m의사",
        "n_doctors_1km_med_new": "1km의사",
        "n_doctors_within_radius_med_new": "1.5km의사",
        "catchment_pop_40plus_new": "40+catch",
    }).to_string(index=False))
    print()

    # ── 새로 진입 ──
    if newly_in:
        print("=" * 80)
        print(f"[새로 Top30 진입 — {len(newly_in)}개]")
        print("=" * 80)
        rows = df_sorted[df_sorted["adm_cd"].isin(newly_in)][
            ["rank_new", "adm_cd", "sgg", "adm_nm", "score_new"]].copy()
        rows["old_rank"] = rows["adm_cd"].map(old_rank_map).fillna(-1).astype(int)
        print(rows[["rank_new", "old_rank", "sgg", "adm_nm", "score_new"]].sort_values("rank_new").to_string(index=False))
        print()

    # ── 탈락 ──
    if dropped:
        print("=" * 80)
        print(f"[탈락 — {len(dropped)}개] 인구중심에선 Top30이었으나 상가중심에선 밀려남")
        print("=" * 80)
        rows = df_sorted[df_sorted["adm_cd"].isin(dropped)][
            ["rank_new", "adm_cd", "sgg", "adm_nm", "score_new"]].copy()
        rows["old_rank"] = rows["adm_cd"].map(old_rank_map).fillna(-1).astype(int)
        print(rows[["old_rank", "rank_new", "sgg", "adm_nm", "score_new"]].sort_values("old_rank").to_string(index=False))
        print()

    # ── 기존 Top30의 1.5km 의사 수 / catchment 변화 ──
    print("=" * 110)
    print("[기존 Top30] 인구중심 vs 상가중심 input 변화")
    print("=" * 110)
    old_top30_aug = top30_old.merge(
        df_sorted[["adm_cd", "rank_new",
                   "n_doctors_within_radius_med_new",
                   "catchment_pop_40plus_new"]],
        on="adm_cd",
    )
    old_top30_aug["Δ1.5km의사"] = (
        old_top30_aug["n_doctors_within_radius_med_new"] - old_top30_aug["n_doctors_within_radius_med"]
    )
    old_top30_aug["Δcatch40+"] = (
        old_top30_aug["catchment_pop_40plus_new"] - old_top30_aug["catchment_pop_40plus"]
    ).round(0).astype(int)
    show_cols = ["rank", "rank_new", "sgg", "adm_nm",
                 "n_doctors_within_radius_med", "n_doctors_within_radius_med_new", "Δ1.5km의사",
                 "catchment_pop_40plus", "catchment_pop_40plus_new", "Δcatch40+"]
    old_top30_aug["catchment_pop_40plus"] = old_top30_aug["catchment_pop_40plus"].round(0).astype(int)
    old_top30_aug["catchment_pop_40plus_new"] = old_top30_aug["catchment_pop_40plus_new"].round(0).astype(int)
    print(old_top30_aug[show_cols].rename(columns={
        "rank": "구r", "rank_new": "신r",
        "n_doctors_within_radius_med": "구1.5의사",
        "n_doctors_within_radius_med_new": "신1.5의사",
        "catchment_pop_40plus": "구catch40+",
        "catchment_pop_40plus_new": "신catch40+",
    }).to_string(index=False))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

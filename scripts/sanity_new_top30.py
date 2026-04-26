"""SANITY CHECK: 상가 가중 중심점 ablation의 신규 Top30 진입 동 검증.

새로 Top30에 들어온 동들이 진짜 좋은 입지인지, 아니면 데이터 artifact인지 판별.

검증 항목 (각 동별):
- 옛 좌표(인구 가중) vs 새 좌표(상가 가중) — 카카오맵 링크 포함
- 두 좌표 사이 거리(m), 방향
- 옛/새 좌표 기준 500m / 1km / 1.5km / 2km 내과 의사 수 (의사 분포 합리성)
- 옛/새 catchment_pop_40plus (인구 풀 변화)
- nearest_station, station_dist_m (역세권 컨텍스트)
- artifact 시그널: ⚠️ 표시
  - 새 1km 의사 = 0  → 상가 anchor가 의원 없는 곳 (의심)
  - 인구↔상가 거리 > 800m → 동이 두 클러스터로 쪼개짐
  - 새 catch 40+ < 50000 → Top30 정상 범위 미달
  - 새 density << 옛 density → 분모만 커진 효과 (실제 경쟁 약화 아닌 가능성)

비교 기준: 기존 Top30 동들의 동일 metric median (참고치).

사용:
    python -m scripts.sanity_new_top30 [--date 2026-04-22]
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


def catchment_pop(lon, lat, raster, radius_m):
    x, y = _TO_5179(lon, lat)
    buf_5179 = Point(x, y).buffer(radius_m, resolution=32)
    buf_wgs = shapely_transform(lambda a, b, z=None: _TO_4326(a, b), buf_5179)
    try:
        clipped, _ = rio_mask(raster, [buf_wgs.__geo_interface__],
                              crop=True, all_touched=True, filled=False)
    except ValueError:
        return 0.0
    arr = clipped[0]
    v = np.where(~arr.mask, arr.data, 0.0) if hasattr(arr, "mask") else arr
    v = np.where(np.isfinite(v) & (v > 0), v, 0.0)
    return float(v.sum())


def _doctors_in_radii(dong_xy_5179, cl_x, cl_y, drs, radii_m):
    """각 동(N개)에 대해 여러 반경의 내과 의사 수 (N x len(radii))."""
    dx = dong_xy_5179[:, 0:1] - cl_x[None, :]
    dy = dong_xy_5179[:, 1:2] - cl_y[None, :]
    d2 = dx * dx + dy * dy
    out = np.zeros((len(dong_xy_5179), len(radii_m)), dtype=int)
    for k, r in enumerate(radii_m):
        within = d2 <= (r ** 2)
        out[:, k] = (within * drs[None, :]).sum(axis=1)
    return out


def _direction(dlat, dlon):
    """대략 방위. dlat>0=북, dlon>0=동."""
    ns = "북" if dlat > 0 else "남"
    ew = "동" if dlon > 0 else "서"
    if abs(dlat) < abs(dlon) * 0.3:
        return ew
    if abs(dlon) < abs(dlat) * 0.3:
        return ns
    return ns + ew


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-22")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    log = logging.getLogger("sanity")

    scores = pd.read_parquet(DATA_SCORED / f"scores_{args.date}.parquet")
    shops = pd.read_parquet(DATA_CLEANED / "shops_by_dong.parquet")
    pop_centroid = pd.read_parquet(DATA_CACHE / "admin_centroid_pop.parquet")
    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")

    scores["adm_cd"] = scores["adm_cd"].astype(str)
    scores["adm_cd10"] = scores["adm_cd10"].astype(str)
    shops["adm_cd10"] = shops["adm_cd10"].astype(str)
    pop_centroid["adm_cd"] = pop_centroid["adm_cd"].astype(str)

    df = scores.merge(
        shops[["adm_cd10", "shops_lat_mean", "shops_lon_mean", "n_shops_floor12"]],
        on="adm_cd10", how="left",
    ).merge(
        pop_centroid[["adm_cd", "lat_pop", "lon_pop"]],
        on="adm_cd", how="left",
    )
    df["lat_new"] = df["shops_lat_mean"].fillna(df["lat_pop"])
    df["lon_new"] = df["shops_lon_mean"].fillna(df["lon_pop"])

    log.info("recomputing for ablation...")
    # 새 catchment
    with rasterio.open(WORLDPOP_PATH) as src:
        df["catch_new"] = [catchment_pop(lo, la, src, CATCHMENT_RADIUS_M)
                           for lo, la in zip(df["lon_new"], df["lat_new"])]
    df["catch40_new"] = df["catch_new"] * df["ratio_40plus"]

    # 좌표 5179 변환 (옛 / 새)
    pts_old = gpd.GeoSeries(
        [Point(lo, la) for lo, la in zip(df["lon_pop"], df["lat_pop"])], crs=EPSG_WGS84
    ).to_crs(EPSG_KOREA)
    pts_new = gpd.GeoSeries(
        [Point(lo, la) for lo, la in zip(df["lon_new"], df["lat_new"])], crs=EPSG_WGS84
    ).to_crs(EPSG_KOREA)
    old_xy = np.column_stack([[p.x for p in pts_old], [p.y for p in pts_old]]).astype("float32")
    new_xy = np.column_stack([[p.x for p in pts_new], [p.y for p in pts_new]]).astype("float32")

    # 클리닉 5179 + 내과 마스크
    cl_gdf = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(
            pd.to_numeric(clinics["XPos"]), pd.to_numeric(clinics["YPos"])
        ),
        crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    cl_x = cl_gdf.geometry.x.values.astype("float32")
    cl_y = cl_gdf.geometry.y.values.astype("float32")
    is_internal = clinics["yadmNm"].str.contains("내과", na=False).to_numpy()
    drs = pd.to_numeric(clinics.get("drTotCnt", 0), errors="coerce").fillna(0).astype(int).to_numpy()
    cl_x_med, cl_y_med, drs_med = cl_x[is_internal], cl_y[is_internal], drs[is_internal]

    radii = (500, 1000, 1500, 2000)
    docs_old = _doctors_in_radii(old_xy, cl_x_med, cl_y_med, drs_med, radii)
    docs_new = _doctors_in_radii(new_xy, cl_x_med, cl_y_med, drs_med, radii)
    for k, r in enumerate(radii):
        df[f"docs_old_{r}"] = docs_old[:, k]
        df[f"docs_new_{r}"] = docs_new[:, k]

    # density 새
    safe_pop = df["catch40_new"].replace(0, np.nan)
    df["density_new"] = (df["n_doctors_med"] / (safe_pop / 10_000)).fillna(0)

    # c_raw_new
    df["c_raw_new"] = (
        W_COMP_DENSITY * df["density_new"]
        + W_COMP_RADIUS * df["docs_new_1500"]
        + W_COMP_STATION * df["n_doctors_station_500m_med"].fillna(0)
    )
    df["p_raw_new"] = df["catch40_new"]
    df["c_norm_new"] = percentile_rank_inverted(df["c_raw_new"])
    df["p_norm_new"] = percentile_rank(df["p_raw_new"])
    df["score_new"] = (
        W_COMPETITION * df["c_norm_new"]
        + W_POPULATION * df["p_norm_new"]
        + W_COMMUTE * df["t_norm"]
    )
    # 좌표 이동 거리 (m)
    dx_m = new_xy[:, 0] - old_xy[:, 0]
    dy_m = new_xy[:, 1] - old_xy[:, 1]
    df["shift_m"] = np.sqrt(dx_m * dx_m + dy_m * dy_m).round(0).astype(int)
    df_sorted = df.sort_values("score_new", ascending=False).reset_index(drop=True)
    df_sorted["rank_new"] = df_sorted.index + 1

    top30_old_set = set(scores.sort_values("score", ascending=False).head(30)["adm_cd"])
    top30_new = df_sorted.head(30)
    newly_in = set(top30_new["adm_cd"]) - top30_old_set

    log.info(f"newly entered: {len(newly_in)}")

    # 기준치 (기존 Top30 metric의 median)
    base = scores[scores["adm_cd"].isin(top30_old_set)]
    median_docs_1km = base["n_clinic_1km"].median()  # 전체 의원이지만 참고용
    median_catch40 = base["catchment_pop_40plus"].median()

    print()
    print("=" * 100)
    print(f"[SANITY] 신규 Top30 진입 {len(newly_in)}개 동 검증")
    print(f"  기준치: 기존 Top30 catch_40+ median = {median_catch40:,.0f}")
    print("=" * 100)

    # 신규 동을 새 rank 순으로
    new_df = df_sorted[df_sorted["adm_cd"].isin(newly_in)].sort_values("rank_new")

    for _, row in new_df.iterrows():
        adm_cd = row["adm_cd"]
        old_rank = scores[scores["adm_cd"] == adm_cd]["rank"].iloc[0]
        new_rank = int(row["rank_new"])
        sgg = row["sgg"]
        nm = row["adm_nm"]
        score_new = row["score_new"]

        # 좌표
        lat_p, lon_p = row["lat_pop"], row["lon_pop"]
        lat_n, lon_n = row["lat_new"], row["lon_new"]
        shift_m = int(row["shift_m"])
        # 방향
        dlat = lat_n - lat_p
        dlon = lon_n - lon_p
        direction = _direction(dlat, dlon)

        # 의사 수 (전체)
        d_old = [int(row[f"docs_old_{r}"]) for r in radii]
        d_new = [int(row[f"docs_new_{r}"]) for r in radii]
        delta = [n - o for n, o in zip(d_new, d_old)]

        # catchment
        catch_old = float(row["catchment_pop_40plus"])
        catch_new = float(row["catch40_new"])
        catch_delta = catch_new - catch_old

        # density
        density_old = float(row.get("density_per_10k", 0))
        density_new = float(row["density_new"])

        # 역세권
        station = row.get("nearest_station", "?")
        st_dist = row.get("station_dist_m", None)
        st_docs = int(row.get("n_doctors_station_500m_med", 0) or 0)

        # artifact 시그널
        signals = []
        if d_new[1] == 0:
            signals.append(f"⚠️ 새 1km 의사=0 (상가 anchor가 의원 없는 곳)")
        if shift_m > 800:
            signals.append(f"⚠️ 좌표 이동 {shift_m}m > 800m (동이 두 클러스터)")
        if catch_new < 50000:
            signals.append(f"⚠️ 새 40+ catch {catch_new:,.0f} < 50k (Top30 정상 범위 미달)")
        if d_new[2] < 5 and d_new[3] < 30:
            signals.append(f"⚠️ 1.5km {d_new[2]}, 2km {d_new[3]} (의료사막 패턴)")
        if density_old > 0 and density_new < 0.3 * density_old:
            signals.append(f"⚠️ density {density_old:.2f} → {density_new:.2f} (분모만 커진 효과)")

        print()
        print(f"━━━ #{new_rank} ({sgg} {nm})  구rank={old_rank}, score={score_new:.3f}")
        print(f"  [좌표] 인구중심 ({lat_p:.5f}, {lon_p:.5f})  →  상가중심 ({lat_n:.5f}, {lon_n:.5f})")
        print(f"         이동: {shift_m}m {direction}")
        print(f"         카카오맵 인구중심: https://map.kakao.com/link/map/{nm}_pop,{lat_p},{lon_p}")
        print(f"         카카오맵 상가중심: https://map.kakao.com/link/map/{nm}_shop,{lat_n},{lon_n}")
        print(f"  [내과의사 수 (반경별)]   500m / 1km / 1.5km / 2km")
        print(f"     인구 중심:  {d_old[0]:>4} / {d_old[1]:>4} / {d_old[2]:>5} / {d_old[3]:>4}")
        print(f"     상가 중심:  {d_new[0]:>4} / {d_new[1]:>4} / {d_new[2]:>5} / {d_new[3]:>4}")
        print(f"     변화(Δ):   {delta[0]:>+4} / {delta[1]:>+4} / {delta[2]:>+5} / {delta[3]:>+4}")
        print(f"  [40+ catchment]  옛 {catch_old:>10,.0f}  →  새 {catch_new:>10,.0f}  (Δ {catch_delta:+,.0f})")
        print(f"  [density 1만명당]  옛 {density_old:.3f}  →  새 {density_new:.3f}")
        if station != "?":
            print(f"  [최근접역] {station} ({st_dist:.0f}m), 역500m 내과의사 {st_docs}명")
        print(f"  [상가 1·2층] {int(row.get('n_shops_floor12', 0)):,}개")
        if signals:
            print(f"  ⚠️ ARTIFACT 시그널:")
            for s in signals:
                print(f"     {s}")
        else:
            print(f"  ✅ 시그널 없음 (sanity OK)")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

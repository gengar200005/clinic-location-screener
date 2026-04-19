"""인구 가중 행정동 중심점 계산.

기하 중심점(geometric centroid)은 동 모양에 따라 산·하천·공원에 찍힐 수 있다
(centroid_mismatch_flag: Top 30 중 5개 케이스). 진짜 배후세대(아파트단지) 위치
가 아니므로 임장 좌표·반경 의원 카운트가 어긋난다.

해법: WorldPop 100m 격자 인구를 동 폴리곤으로 마스크하여 픽셀 인구 가중 평균 좌표.
   = (Σ pop_i · lon_i) / Σ pop_i, (Σ pop_i · lat_i) / Σ pop_i

폴백: 폴리곤 안에 인구 픽셀이 0개거나 합이 0 (공단·공원·산) → 기하 중심점 사용.

출력: data/cache/admin_centroid_pop.parquet (영구 커밋, 50KB)
- columns: adm_cd, lat_pop, lon_pop, pop_sum_in_polygon, pop_weighted (bool)
- pop_weighted=False는 폴백한 동

사용:
    python -m scoring.centroid_pop_weighted

전제:
- data/raw/admin_boundary/HangJeongDong_*.geojson (admin_boundary 스크래퍼)
- data/raw/worldpop/kor_ppp_2020.tif (population_grid 스크래퍼)
- data/cleaned/admin_centroid.parquet (기하 중심점 — 폴백용)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.mask import mask as rio_mask

from config.constants import DATA_CACHE, DATA_CLEANED, DATA_RAW, EPSG_WGS84

logger = logging.getLogger(__name__)

OUT_PATH = DATA_CACHE / "admin_centroid_pop.parquet"
WORLDPOP_PATH = DATA_RAW / "worldpop" / "kor_ppp_2020.tif"


def _latest_boundary() -> Path:
    files = sorted((DATA_RAW / "admin_boundary").glob("HangJeongDong_*.geojson"))
    if not files:
        raise FileNotFoundError(
            "admin_boundary GeoJSON 없음. `python -m scrapers.admin_boundary` 먼저."
        )
    return files[-1]


def _compute_centroid_for_polygon(
    polygon, raster, raster_transform
) -> tuple[float, float, float] | None:
    """폴리곤 안의 raster 픽셀들로부터 인구 가중 (lat, lon, pop_sum) 계산.

    None 반환 시 폴리곤 안에 유효 인구 데이터 없음 (호출자가 폴백).
    """
    try:
        clipped, clipped_transform = rio_mask(
            raster, [polygon.__geo_interface__], crop=True, all_touched=False, filled=False
        )
    except ValueError:
        return None

    arr = clipped[0]  # (H, W) masked array
    if hasattr(arr, "mask"):
        valid = ~arr.mask
        values = np.where(valid, arr.data, 0.0)
    else:
        values = arr

    # nodata · NaN · 음수 처리
    values = np.where(np.isfinite(values) & (values > 0), values, 0.0)
    pop_sum = float(values.sum())
    if pop_sum <= 0:
        return None

    h, w = values.shape
    rows, cols = np.indices((h, w))
    # 픽셀 중심 좌표 (lon, lat)
    xs, ys = rasterio.transform.xy(clipped_transform, rows.flatten(), cols.flatten(), offset="center")
    xs = np.asarray(xs).reshape(h, w)
    ys = np.asarray(ys).reshape(h, w)

    weighted_lon = float((values * xs).sum() / pop_sum)
    weighted_lat = float((values * ys).sum() / pop_sum)
    return weighted_lat, weighted_lon, pop_sum


def build() -> Path:
    DATA_CACHE.mkdir(parents=True, exist_ok=True)

    if not WORLDPOP_PATH.exists():
        raise FileNotFoundError(
            f"{WORLDPOP_PATH} 없음. `python -m scrapers.population_grid` 먼저."
        )

    boundary_path = _latest_boundary()
    logger.info("loading boundary: %s", boundary_path.name)
    gdf = gpd.read_file(boundary_path)
    if gdf.crs is None:
        gdf = gdf.set_crs(EPSG_WGS84)
    elif gdf.crs.to_epsg() != EPSG_WGS84:
        gdf = gdf.to_crs(EPSG_WGS84)

    # 대상 행정동만 (admin_centroid.parquet의 adm_cd 사용)
    geom_centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")
    target_codes = set(geom_centroid["adm_cd"].astype(str))
    gdf["adm_cd"] = gdf["adm_cd"].astype(str)
    gdf = gdf[gdf["adm_cd"].isin(target_codes)].copy()
    logger.info("target dongs: %d", len(gdf))

    # 폴백용 기하 중심점 lookup
    geom_lookup = geom_centroid.set_index("adm_cd")[["lat", "lon"]].to_dict("index")

    rows = []
    n_fallback = 0
    with rasterio.open(WORLDPOP_PATH) as src:
        # WorldPop CRS는 EPSG:4326, gdf도 같음 (재투영 불필요)
        if src.crs.to_epsg() != EPSG_WGS84:
            logger.warning("WorldPop CRS=%s, expected 4326 — 결과 부정확 가능", src.crs)

        for adm_cd, poly in zip(gdf["adm_cd"], gdf.geometry):
            result = _compute_centroid_for_polygon(poly, src, src.transform)
            if result is None:
                # 폴백: 기하 중심점
                fallback = geom_lookup.get(str(adm_cd))
                if fallback is None:
                    logger.warning("adm_cd=%s 폴백 좌표 없음 → 스킵", adm_cd)
                    continue
                rows.append({
                    "adm_cd": adm_cd,
                    "lat_pop": fallback["lat"],
                    "lon_pop": fallback["lon"],
                    "pop_sum_in_polygon": 0.0,
                    "pop_weighted": False,
                })
                n_fallback += 1
            else:
                lat, lon, pop_sum = result
                rows.append({
                    "adm_cd": adm_cd,
                    "lat_pop": lat,
                    "lon_pop": lon,
                    "pop_sum_in_polygon": pop_sum,
                    "pop_weighted": True,
                })

    df = pd.DataFrame(rows)
    df.to_parquet(OUT_PATH, index=False)
    logger.info(
        "saved %s (%d rows, %d 인구 가중, %d 폴백)",
        OUT_PATH, len(df), len(df) - n_fallback, n_fallback,
    )

    # 진단: 기하 중심점에서 얼마나 이동했는지
    diag = df.merge(
        geom_centroid[["adm_cd", "lat", "lon"]], on="adm_cd", how="left"
    )
    diag = diag[diag["pop_weighted"]].copy()
    # haversine 근사 (작은 거리이므로 평면 근사 충분)
    dlat = (diag["lat_pop"] - diag["lat"]) * 111_000  # 1° lat ≈ 111km
    dlon = (diag["lon_pop"] - diag["lon"]) * 111_000 * np.cos(np.radians(diag["lat"]))
    dist = np.sqrt(dlat * dlat + dlon * dlon)
    logger.info(
        "이동 거리 통계 (m): 평균 %.0f · 중앙 %.0f · 최대 %.0f · 500m+ 이동 %d개",
        dist.mean(), dist.median(), dist.max(), (dist > 500).sum(),
    )
    return OUT_PATH


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="인구 가중 행정동 중심점 계산")
    args = parser.parse_args()
    build()
    return 0


if __name__ == "__main__":
    sys.exit(main())

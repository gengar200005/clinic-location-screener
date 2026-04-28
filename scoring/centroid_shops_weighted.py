"""1·2층 상가 가중 행정동 중심점 + 그 좌표 기준 catchment 인구.

근거 (ADR-004):
인구 가중 중심점은 동의 인구 mean이지만, 환자가 실제로 가는 의원은 상가/역 belt에
모임. 인구 mean이 아파트 단지 한복판이면 그 좌표는 답사·반경 의원 카운트의 기준
점으로 무의미하다 (개원 자체가 불가능한 위치).

해법: 1·2층 상가(소상공인진흥공단 데이터)의 평균 좌표를 중심점으로 사용. 토지 분류상
실제로 1종 근생/2종 근생/상업지역에서 운영 중인 위치만 anchor.

검증 (scripts/ablation_centroid_shops.py + scripts/sanity_new_top30.py):
- Top30 25/30 유지, 5개 교체. 5개 중 4개 명확히 합리적, 1개(부천 신흥동)는 의료사막
  동 자체 특성. 탈락 5개(청량리·창1·3·장안2·고강본동)는 상가에서 보면 경쟁이 강해
  떨어지는 게 합리적.
- 관악 대학동(인구↔상가 1049m)처럼 두 클러스터 동에서 진짜 보정 효과 확인.

Fallback 순서:
- 1순위: shops_lat_mean / shops_lon_mean (1·2층 상가 평균)
- 2순위: lat_pop / lon_pop (admin_centroid_pop.parquet, 인구 가중)
- 3순위: 기하 centroid (admin_centroid.parquet)

출력: data/cache/admin_centroid_shops.parquet (영구 커밋)
- columns: adm_cd, lat_shops, lon_shops, anchor (str: "shops"|"pop"|"geom"),
           catchment_pop_1_5km,
           anchor_pop_dist_m (shops anchor와 인구 가중 중심점 거리, EPSG:5179)
             — 상암 같은 비주거 monoculture anchor 진단용. 큰 값일수록
               anchor가 거주 분포 밖 (오피스·공원·강 한가운데). pop fallback
               동은 0, geom fallback 동은 NaN.

사용:
    python -m scoring.centroid_shops_weighted
"""
from __future__ import annotations

import argparse
import logging
import sys

import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.mask import mask as rio_mask
from shapely.geometry import Point
from shapely.ops import transform as shapely_transform

from config.constants import (
    CATCHMENT_RADIUS_M,
    DATA_CACHE,
    DATA_CLEANED,
    DATA_RAW,
    EPSG_KOREA,
    EPSG_WGS84,
)

logger = logging.getLogger(__name__)

OUT_PATH = DATA_CACHE / "admin_centroid_shops.parquet"
WORLDPOP_PATH = DATA_RAW / "worldpop" / "kor_ppp_2020.tif"
SHOPS_PATH = DATA_CLEANED / "shops_by_dong.parquet"
POP_CENTROID_PATH = DATA_CACHE / "admin_centroid_pop.parquet"
GEOM_CENTROID_PATH = DATA_CLEANED / "admin_centroid.parquet"

_CATCHMENT_KM = CATCHMENT_RADIUS_M / 1000
CATCHMENT_COL = f"catchment_pop_{str(_CATCHMENT_KM).replace('.', '_')}km"

_TO_5179 = Transformer.from_crs(EPSG_WGS84, EPSG_KOREA, always_xy=True).transform
_TO_4326 = Transformer.from_crs(EPSG_KOREA, EPSG_WGS84, always_xy=True).transform


def _catchment_pop(lon: float, lat: float, raster, radius_m: float) -> float:
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
    v = np.where(~arr.mask, arr.data, 0.0) if hasattr(arr, "mask") else arr
    v = np.where(np.isfinite(v) & (v > 0), v, 0.0)
    return float(v.sum())


def build() -> pd.DataFrame:
    DATA_CACHE.mkdir(parents=True, exist_ok=True)
    if not WORLDPOP_PATH.exists():
        raise FileNotFoundError(f"{WORLDPOP_PATH} 없음.")
    if not SHOPS_PATH.exists():
        raise FileNotFoundError(
            f"{SHOPS_PATH} 없음. `python -m scrapers.commercial_shops` 먼저."
        )
    if not GEOM_CENTROID_PATH.exists():
        raise FileNotFoundError(f"{GEOM_CENTROID_PATH} 없음.")

    geom = pd.read_parquet(GEOM_CENTROID_PATH)
    geom["adm_cd"] = geom["adm_cd"].astype(str)
    if "adm_cd10" in geom.columns:
        geom["adm_cd10"] = geom["adm_cd10"].astype(str)
    else:
        geom["adm_cd10"] = geom["adm_cd"] + "00"

    shops = pd.read_parquet(SHOPS_PATH)
    shops["adm_cd10"] = shops["adm_cd10"].astype(str)

    # pop fallback (선택)
    if POP_CENTROID_PATH.exists():
        pop = pd.read_parquet(POP_CENTROID_PATH)
        pop["adm_cd"] = pop["adm_cd"].astype(str)
    else:
        pop = pd.DataFrame(columns=["adm_cd", "lat_pop", "lon_pop"])

    df = geom[["adm_cd", "adm_cd10", "lat", "lon"]].rename(
        columns={"lat": "lat_geom", "lon": "lon_geom"}
    )
    df = df.merge(
        shops[["adm_cd10", "shops_lat_mean", "shops_lon_mean"]],
        on="adm_cd10", how="left",
    )
    df = df.merge(
        pop[["adm_cd", "lat_pop", "lon_pop"]],
        on="adm_cd", how="left",
    )

    # 1·2·3순위 선택
    def pick(row):
        if pd.notna(row["shops_lat_mean"]):
            return row["shops_lat_mean"], row["shops_lon_mean"], "shops"
        if pd.notna(row.get("lat_pop")):
            return row["lat_pop"], row["lon_pop"], "pop"
        return row["lat_geom"], row["lon_geom"], "geom"

    picked = df.apply(pick, axis=1, result_type="expand")
    picked.columns = ["lat_shops", "lon_shops", "anchor"]
    df = pd.concat([df[["adm_cd", "lat_pop", "lon_pop"]], picked], axis=1)

    # catchment 계산
    logger.info("computing catchment for %d dongs...", len(df))
    cps = []
    with rasterio.open(WORLDPOP_PATH) as src:
        if src.crs.to_epsg() != EPSG_WGS84:
            logger.warning("WorldPop CRS=%s ≠ 4326", src.crs)
        for lon, lat in zip(df["lon_shops"], df["lat_shops"]):
            cps.append(_catchment_pop(lon, lat, src, CATCHMENT_RADIUS_M))
    df[CATCHMENT_COL] = cps

    # anchor와 인구 가중 중심점 거리 — 비주거 anchor 진단용
    df["anchor_pop_dist_m"] = np.nan
    if "lat_pop" in df.columns:
        has_pop = df["lat_pop"].notna()
        if has_pop.any():
            sub = df.loc[has_pop]
            xa, ya = _TO_5179(sub["lon_shops"].values, sub["lat_shops"].values)
            xp, yp = _TO_5179(sub["lon_pop"].values, sub["lat_pop"].values)
            df.loc[has_pop, "anchor_pop_dist_m"] = np.sqrt(
                (np.asarray(xa) - np.asarray(xp)) ** 2
                + (np.asarray(ya) - np.asarray(yp)) ** 2
            ).round(0)

    df.to_parquet(OUT_PATH, index=False)
    n_shops = (df["anchor"] == "shops").sum()
    n_pop = (df["anchor"] == "pop").sum()
    n_geom = (df["anchor"] == "geom").sum()
    logger.info(
        "saved %s — anchor: shops %d / pop %d / geom %d",
        OUT_PATH, n_shops, n_pop, n_geom,
    )
    cc = df[CATCHMENT_COL]
    logger.info(
        "%s: median %d, mean %d, min %d, max %d",
        CATCHMENT_COL, int(cc.median()), int(cc.mean()),
        int(cc.min()), int(cc.max()),
    )
    return df


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="1·2층 상가 가중 중심점")
    parser.parse_args()
    build()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""행정동 중심점 기준 확장 반경 지표.

500m만으로는 중심점-상권 괴리가 큰 동의 실상권을 포착 못 함 (홍은2동 500m=0
이지만 2km=124개 사례). 1km·2km 카운트를 추가로 계산해 "의료사막" vs
"중심점 에러" vs "신도시 상가 밀집형" 구분 가능하게 함.

지표:
- n_clinic_500m / _1km / _2km: 중심점 기준 반경 의원 수
- med_desert_flag: 1km ≤ 5 AND 2km ≤ 30 (진짜 의료사막 의심)
- centroid_mismatch_flag: 500m = 0 AND 2km ≥ 50 (중심점이 상권 밖 주거지)
- suburban_cluster_flag: 동내 ≥ 10 AND 1km ≤ 5 (신도시 상가 밀집형)
"""
from __future__ import annotations

import logging

import geopandas as gpd
import numpy as np
import pandas as pd

from config.constants import EPSG_KOREA, EPSG_WGS84

logger = logging.getLogger(__name__)


def compute_radius_counts(
    admin_centroid: pd.DataFrame,
    clinics_by_dong: pd.DataFrame,
    radii_m: tuple[int, ...] = (500, 1000, 2000),
) -> pd.DataFrame:
    """각 동 중심점 기준 여러 반경의 의원 수 집계.

    반환: [adm_cd, n_clinic_500m, n_clinic_1km, n_clinic_2km]
    """
    if "x_5179" not in clinics_by_dong.columns:
        gdf = gpd.GeoDataFrame(
            clinics_by_dong,
            geometry=gpd.points_from_xy(
                pd.to_numeric(clinics_by_dong["XPos"]),
                pd.to_numeric(clinics_by_dong["YPos"]),
            ),
            crs=EPSG_WGS84,
        ).to_crs(EPSG_KOREA)
        cl_x = gdf.geometry.x.values.astype("float32")
        cl_y = gdf.geometry.y.values.astype("float32")
    else:
        cl_x = clinics_by_dong["x_5179"].to_numpy(dtype="float32")
        cl_y = clinics_by_dong["y_5179"].to_numpy(dtype="float32")

    dong_x = admin_centroid["x_5179"].to_numpy(dtype="float32")
    dong_y = admin_centroid["y_5179"].to_numpy(dtype="float32")

    dx = dong_x[:, None] - cl_x[None, :]
    dy = dong_y[:, None] - cl_y[None, :]
    d2 = dx * dx + dy * dy  # (n_dong, n_clinic)

    result = pd.DataFrame({"adm_cd": admin_centroid["adm_cd"].values})
    for r in radii_m:
        col = f"n_clinic_{_radius_label(r)}"
        result[col] = (d2 <= (r ** 2)).sum(axis=1).astype(int)
    return result


def _radius_label(r: int) -> str:
    if r < 1000:
        return f"{r}m"
    if r % 1000 == 0:
        return f"{r // 1000}km"
    return f"{r}m"


def add_coverage_flags(df: pd.DataFrame) -> pd.DataFrame:
    """반경 컬럼 기반 해석 플래그 추가.

    Requires columns: n_clinic (동내), n_clinic_500m, n_clinic_1km, n_clinic_2km.
    """
    out = df.copy()
    # 진짜 의료사막: 도보권 전체가 비어있음
    out["med_desert_flag"] = (
        (out["n_clinic_1km"] <= 5) & (out["n_clinic_2km"] <= 30)
    )
    # 중심점 에러: 반경 바로 옆은 비었는데 2km 넓혀보면 의료권 있음
    out["centroid_mismatch_flag"] = (
        (out["n_clinic_500m"] == 0) & (out["n_clinic_2km"] >= 50)
    )
    # 신도시 상가 밀집: 동내 많지만 1km 밖은 비어있음
    out["suburban_cluster_flag"] = (
        (out["n_clinic"] >= 10) & (out["n_clinic_1km"] <= 5)
    )
    return out


def compute_for_dongs(
    admin_centroid: pd.DataFrame,
    clinics_by_dong: pd.DataFrame,
) -> pd.DataFrame:
    """admin_centroid + clinics_by_dong → 반경 지표 DataFrame (플래그 제외)."""
    return compute_radius_counts(admin_centroid, clinics_by_dong)

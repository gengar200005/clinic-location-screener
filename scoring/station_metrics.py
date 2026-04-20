"""역세권 보정 지표.

행정동 중심점과 실제 의료상권 중심이 불일치하는 경우가 많음 (예: 이촌1동
centroid는 공원 쪽, 실제 상권은 이촌역). 가장 가까운 지하철·광역철도역을
"진짜 상권 앵커"로 사용해서 다음 3개 컬럼 추가:

- nearest_station : 최근접역 이름
- station_dist_m  : 중심점에서 최근접역까지 거리(m)
- n_clinic_station_500m : 그 역 반경 500m 내 의원 수

주의: 현재 C_raw(경쟁 점수)에는 반영 X. 참고용 메타 컬럼. 튜닝 근거 쌓이면
가중치 재설계 후 C_raw에 편입.
"""
from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from config.constants import DATA_CACHE, EPSG_KOREA, EPSG_WGS84

logger = logging.getLogger(__name__)

STATION_CACHE = DATA_CACHE / "subway_stations.parquet"
STATION_RADIUS_M = 500


def load_stations(path: Path | None = None) -> pd.DataFrame:
    path = path or STATION_CACHE
    if not path.exists():
        raise FileNotFoundError(
            f"역 캐시 없음: {path}. `python -m scrapers.subway_stations` 먼저."
        )
    return pd.read_parquet(path)


def compute_station_clinic_counts(
    stations: pd.DataFrame,
    clinics_by_dong: pd.DataFrame,
    radius_m: int = STATION_RADIUS_M,
) -> pd.DataFrame:
    """각 역에서 반경 radius_m 내 의원 수 + 내과 의사 수.

    반환: stations + ['n_clinic_station', 'n_doctors_station_med']
      - n_clinic_station: 전체 의원 수 (display용, 기존 호환)
      - n_doctors_station_med: 내과 의원의 의사 수 합 (c_raw 페널티 입력)
    """
    # 의원 좌표를 EPSG:5179로
    if "x_5179" not in clinics_by_dong.columns:
        gdf = gpd.GeoDataFrame(
            clinics_by_dong,
            geometry=gpd.points_from_xy(
                pd.to_numeric(clinics_by_dong["XPos"]),
                pd.to_numeric(clinics_by_dong["YPos"]),
            ),
            crs=EPSG_WGS84,
        ).to_crs(EPSG_KOREA)
        cl_x = gdf.geometry.x.values
        cl_y = gdf.geometry.y.values
    else:
        cl_x = clinics_by_dong["x_5179"].to_numpy(dtype="float32")
        cl_y = clinics_by_dong["y_5179"].to_numpy(dtype="float32")

    st_x = stations["x_5179"].to_numpy(dtype="float32")
    st_y = stations["y_5179"].to_numpy(dtype="float32")

    # 623 x ~6,700 = 4.2M pairs
    dx = st_x[:, None] - cl_x[None, :]
    dy = st_y[:, None] - cl_y[None, :]
    within = (dx * dx + dy * dy) <= (radius_m ** 2)
    n_clinic = within.sum(axis=1).astype(int)

    # 내과 의사 수 합 (c_raw 페널티 입력 — 점수 모델은 의사 수 가중)
    is_internal = clinics_by_dong["yadmNm"].str.contains("내과", na=False).to_numpy()
    drs = pd.to_numeric(
        clinics_by_dong.get("drTotCnt", 0), errors="coerce"
    ).fillna(0).astype(int).to_numpy()
    drs_med = (drs * is_internal).astype("float32")  # 내과 아니면 0
    n_doctors_med = (within * drs_med[None, :]).sum(axis=1).astype(int)

    out = stations.copy()
    out["n_clinic_station"] = n_clinic
    out["n_doctors_station_med"] = n_doctors_med
    return out


def compute_nearest_station(
    admin_centroid: pd.DataFrame,
    stations_with_count: pd.DataFrame,
) -> pd.DataFrame:
    """각 행정동 → 최근접역 맵.

    반환: [adm_cd, nearest_station, station_dist_m,
           n_clinic_station_500m, n_doctors_station_500m_med]
    """
    dong_x = admin_centroid["x_5179"].to_numpy(dtype="float32")
    dong_y = admin_centroid["y_5179"].to_numpy(dtype="float32")
    st_x = stations_with_count["x_5179"].to_numpy(dtype="float32")
    st_y = stations_with_count["y_5179"].to_numpy(dtype="float32")

    dx = dong_x[:, None] - st_x[None, :]
    dy = dong_y[:, None] - st_y[None, :]
    d2 = dx * dx + dy * dy  # (n_dong, n_station)
    nearest_idx = d2.argmin(axis=1)
    nearest_dist_m = np.sqrt(d2[np.arange(len(admin_centroid)), nearest_idx]).astype(int)

    out = pd.DataFrame({
        "adm_cd": admin_centroid["adm_cd"].values,
        "nearest_station": stations_with_count["name"].values[nearest_idx],
        "station_dist_m": nearest_dist_m,
        "n_clinic_station_500m": stations_with_count["n_clinic_station"].values[nearest_idx],
        "n_doctors_station_500m_med": (
            stations_with_count["n_doctors_station_med"].values[nearest_idx]
            if "n_doctors_station_med" in stations_with_count.columns
            else 0
        ),
    })
    return out


def compute_for_dongs(
    admin_centroid: pd.DataFrame,
    clinics_by_dong: pd.DataFrame,
) -> pd.DataFrame:
    """Top-level: admin_centroid + clinics_by_dong → 역 지표 DataFrame."""
    stations = load_stations()
    logger.info("stations: %d", len(stations))
    stations = compute_station_clinic_counts(stations, clinics_by_dong)
    return compute_nearest_station(admin_centroid, stations)

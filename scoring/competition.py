"""경쟁 지표 C 계산.

C_raw = w_density · (N / (P/10k)) + w_radius · N_radius + w_station · N_station_500m

- 밀도 항 (W_COMP_DENSITY): 인구 1만명당 의사 수
- 반경 항 (W_COMP_RADIUS): 1.5km 내 의사 수
- 역세권 페널티 (W_COMP_STATION, 2026-04-21 추가):
    최근접역 500m 내 내과 의사 수. 동 centroid가 의료상권 중심과 어긋날 때
    역세권 밀집을 c_raw가 놓치는 문제 보정. 가중치 0.2× (보수적).

인구 데이터가 아직 없을 때는 `compute_radius_only()` 로 두 번째 항만 계산 가능
(3주차 인구 스크래퍼 완성 전 간이 검증용).

정규화·방향 처리는 scoring/pipeline.py에서 일괄 수행.
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from config.constants import (
    COMPETITION_RADIUS_M,
    DATA_CLEANED,
    W_COMP_DENSITY,
    W_COMP_RADIUS,
    W_COMP_STATION,
    W_COMP_SUBCLUSTER,
)

logger = logging.getLogger(__name__)


def count_clinics_per_dong(
    clinics_by_dong: pd.DataFrame,
    internal_keyword: str | None = None,
    sum_doctors: bool = False,
) -> pd.DataFrame:
    """클리닉 공간조인 결과 → 행정동별 개수 집계.

    입력: scoring.spatial_join.join_clinics_to_dong 산출 parquet
    internal_keyword: 'yadmNm'에 이 키워드 포함된 의원만 카운트 (예: '내과').
    sum_doctors: True면 drTotCnt 합도 같이 반환 (n_doctors 컬럼).
    출력: columns = [adm_cd, adm_nm, sido, sgg, n_clinic, n_clinic_gi(, n_doctors)]
    """
    df = clinics_by_dong
    if internal_keyword:
        mask = df["yadmNm"].str.contains(internal_keyword, na=False)
        df = df[mask]
    # is_gi: 병원명에 "소화기" 포함 태깅 (scrapers/hira_clinic.py에서 계산)
    if sum_doctors:
        df = df.copy()
        df["_dr"] = pd.to_numeric(df.get("drTotCnt", 0), errors="coerce").fillna(0).astype(int)
        agg_kwargs = {
            "n_clinic": ("yadmNm", "count"),
            "n_doctors": ("_dr", "sum"),
        }
    else:
        agg_kwargs = {"n_clinic": ("yadmNm", "count")}
    if "is_gi" in df.columns:
        agg_kwargs["n_clinic_gi"] = ("is_gi", "sum")
    grouped = df.groupby(["adm_cd", "sido", "sgg", "adm_nm"]).agg(**agg_kwargs).reset_index()
    if "n_clinic_gi" not in grouped.columns:
        grouped["n_clinic_gi"] = 0
    return grouped


def count_clinics_within_radius(
    clinics_by_dong: pd.DataFrame,
    admin_centroid: pd.DataFrame,
    radius_m: int = COMPETITION_RADIUS_M,
    internal_keyword: str | None = None,
    sum_doctors: bool = False,
) -> pd.DataFrame:
    """각 행정동 중심점에서 반경 `radius_m` 이내 클리닉 수.

    - 중심점과 클리닉 모두 EPSG:5179 좌표로 euclidean 거리 계산
    - clinics_by_dong에는 XPos/YPos(EPSG:4326) 있지만 5179 좌표는 없음 →
      필요 시 여기서 변환 대신 단순 거리 환산(위경도 1도 ≈ 111km)은 부정확.
      올바른 방법: clinics_by_dong 작성 시에도 5179 좌표 추가하거나,
      admin_centroid와 clinics를 모두 EPSG:5179로 투영하여 결합.

    구현: admin_centroid의 (x_5179, y_5179)와 clinics의 5179 변환을
    모두 넘파이로 벡터화. 653 × 6,669 = 약 435만 페어 — 메모리 허용.

    internal_keyword: 'yadmNm'에 이 키워드 포함된 의원만 카운트 (예: '내과').

    출력: columns = [adm_cd, n_within_radius]
    """
    if internal_keyword:
        mask = clinics_by_dong["yadmNm"].str.contains(internal_keyword, na=False)
        clinics_by_dong = clinics_by_dong[mask].copy()

    # clinics에 5179 좌표 부여 (없으면 on-the-fly 변환)
    if "x_5179" not in clinics_by_dong.columns:
        import geopandas as gpd
        from config.constants import EPSG_KOREA, EPSG_WGS84

        gdf = gpd.GeoDataFrame(
            clinics_by_dong,
            geometry=gpd.points_from_xy(
                pd.to_numeric(clinics_by_dong["XPos"]),
                pd.to_numeric(clinics_by_dong["YPos"]),
            ),
            crs=EPSG_WGS84,
        ).to_crs(EPSG_KOREA)
        clinics_by_dong = clinics_by_dong.copy()
        clinics_by_dong["x_5179"] = gdf.geometry.x.values
        clinics_by_dong["y_5179"] = gdf.geometry.y.values

    dong_x = admin_centroid["x_5179"].to_numpy(dtype="float32")
    dong_y = admin_centroid["y_5179"].to_numpy(dtype="float32")
    clinic_x = clinics_by_dong["x_5179"].to_numpy(dtype="float32")
    clinic_y = clinics_by_dong["y_5179"].to_numpy(dtype="float32")

    # 거리 행렬 (n_dong, n_clinic) — 653×6,669 = 약 435만 페어, float32로 ~17MB
    dx = dong_x[:, None] - clinic_x[None, :]
    dy = dong_y[:, None] - clinic_y[None, :]
    within_mask = (dx * dx + dy * dy) <= (radius_m ** 2)
    n_within = within_mask.sum(axis=1)

    out = pd.DataFrame({
        "adm_cd": admin_centroid["adm_cd"].values,
        "n_within_radius": n_within,
    })
    if sum_doctors:
        dr = pd.to_numeric(
            clinics_by_dong.get("drTotCnt", 0), errors="coerce"
        ).fillna(0).astype(int).to_numpy(dtype="float32")
        # within_mask: (n_dong, n_clinic). 각 동에 대해 마스크된 의원의 dr 합.
        out["n_doctors_within"] = (within_mask * dr[None, :]).sum(axis=1).astype(int)
    return out


def compute_subcluster_max_doctors(
    admin_centroid: pd.DataFrame,
    clinics_by_dong: pd.DataFrame,
    radius_m: int = 1500,
    cluster_radius_m: int = 500,
) -> pd.DataFrame:
    """동 내 가장 밀집된 subcluster의 내과 의사 수.

    각 동 centroid 반경 radius_m 안의 모든 내과 의원 위치를 후보 anchor로 삼아,
    각 anchor 기준 cluster_radius_m 안 내과 의사 수를 계산 → max.

    "동 centroid가 의료상권 mean이라 역세권 밀집을 묻는 문제"의 정밀 보정.
    사용자 지시상 격자 분할 안이지만, 격자 경계 효과를 피하려고 의원 위치 기반
    sliding disk로 구현 (DBSCAN 단순 버전 — eps=cluster_radius_m, minPts=1).

    반환: [adm_cd, n_doctors_subcluster_max_med, n_clinics_subcluster_max_med]
    """
    if "x_5179" not in clinics_by_dong.columns:
        import geopandas as gpd
        from config.constants import EPSG_KOREA, EPSG_WGS84

        gdf = gpd.GeoDataFrame(
            clinics_by_dong,
            geometry=gpd.points_from_xy(
                pd.to_numeric(clinics_by_dong["XPos"]),
                pd.to_numeric(clinics_by_dong["YPos"]),
            ),
            crs=EPSG_WGS84,
        ).to_crs(EPSG_KOREA)
        clinics_by_dong = clinics_by_dong.copy()
        clinics_by_dong["x_5179"] = gdf.geometry.x.values
        clinics_by_dong["y_5179"] = gdf.geometry.y.values

    is_internal = clinics_by_dong["yadmNm"].str.contains("내과", na=False)
    cl_med = clinics_by_dong[is_internal]
    cl_x = cl_med["x_5179"].to_numpy(dtype="float32")
    cl_y = cl_med["y_5179"].to_numpy(dtype="float32")
    drs = pd.to_numeric(
        cl_med.get("drTotCnt", 0), errors="coerce"
    ).fillna(0).astype(int).to_numpy()

    dong_x = admin_centroid["x_5179"].to_numpy(dtype="float32")
    dong_y = admin_centroid["y_5179"].to_numpy(dtype="float32")

    r2_outer = float(radius_m) ** 2
    r2_cluster = float(cluster_radius_m) ** 2

    n_dong = len(admin_centroid)
    max_drs = np.zeros(n_dong, dtype=int)
    max_clin = np.zeros(n_dong, dtype=int)

    for i in range(n_dong):
        dx = cl_x - dong_x[i]
        dy = cl_y - dong_y[i]
        in_outer = (dx * dx + dy * dy) <= r2_outer
        if not in_outer.any():
            continue
        cx = cl_x[in_outer]
        cy = cl_y[in_outer]
        cd = drs[in_outer]
        # n_cand x n_cand 페어 거리 (n_cand 보통 수십, 메모리 OK)
        ax = cx[:, None] - cx[None, :]
        ay = cy[:, None] - cy[None, :]
        within = (ax * ax + ay * ay) <= r2_cluster
        max_drs[i] = int((within * cd[None, :]).sum(axis=1).max())
        max_clin[i] = int(within.sum(axis=1).max())

    return pd.DataFrame({
        "adm_cd": admin_centroid["adm_cd"].values,
        "n_doctors_subcluster_max_med": max_drs,
        "n_clinics_subcluster_max_med": max_clin,
    })


def compute_competition_raw(
    n_by_dong: pd.DataFrame,
    within_radius: pd.DataFrame,
    population: pd.DataFrame | None = None,
    station_penalty: pd.DataFrame | None = None,
    subcluster_penalty: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """C_raw = w_density·(N/P_10k) + w_radius·N_radius
              + w_station·N_station + w_subcluster·N_subcluster_max

    - population: adm_cd + population 컬럼. None이면 밀도 항 제외 (간이 모드).
    - station_penalty: adm_cd + n_doctors_station_500m_med (선택).
        역 캐시 없으면 None → 항 0.
    - subcluster_penalty: adm_cd + n_doctors_subcluster_max_med (선택).
        compute_subcluster_max_doctors 결과. W_COMP_SUBCLUSTER 기본 0이면 영향 없음.
    """
    df = n_by_dong.merge(within_radius, on="adm_cd", how="left")
    df["n_within_radius"] = df["n_within_radius"].fillna(0)

    # 역세권 페널티 머지 (없으면 0)
    if station_penalty is not None:
        df = df.merge(
            station_penalty[["adm_cd", "n_doctors_station_500m_med"]],
            on="adm_cd", how="left",
        )
        df["n_doctors_station_500m_med"] = df["n_doctors_station_500m_med"].fillna(0)
    else:
        df["n_doctors_station_500m_med"] = 0

    # subcluster max 페널티 머지 (없으면 0)
    if subcluster_penalty is not None:
        df = df.merge(
            subcluster_penalty[["adm_cd", "n_doctors_subcluster_max_med"]],
            on="adm_cd", how="left",
        )
        df["n_doctors_subcluster_max_med"] = df["n_doctors_subcluster_max_med"].fillna(0)
    else:
        df["n_doctors_subcluster_max_med"] = 0

    if population is not None:
        df = df.merge(population[["adm_cd", "population"]], on="adm_cd", how="left")
        safe_pop = df["population"].replace(0, np.nan)
        df["density_per_10k"] = df["n_clinic"] / (safe_pop / 10_000)
        df["density_per_10k"] = df["density_per_10k"].fillna(0)
        df["c_raw"] = (
            W_COMP_DENSITY * df["density_per_10k"]
            + W_COMP_RADIUS * df["n_within_radius"]
            + W_COMP_STATION * df["n_doctors_station_500m_med"]
            + W_COMP_SUBCLUSTER * df["n_doctors_subcluster_max_med"]
        )
    else:
        logger.warning("population 없음 — 반경 항만 사용 (간이 모드)")
        df["density_per_10k"] = np.nan
        df["c_raw"] = (
            df["n_within_radius"].astype(float)
            + W_COMP_STATION * df["n_doctors_station_500m_med"]
            + W_COMP_SUBCLUSTER * df["n_doctors_subcluster_max_med"]
        )

    return df


def main() -> int:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Competition score (간이 모드)")
    parser.add_argument("--clinics-by-dong", type=Path,
                        default=DATA_CLEANED / "clinics_by_dong.parquet")
    parser.add_argument("--admin-centroid", type=Path,
                        default=DATA_CLEANED / "admin_centroid.parquet")
    args = parser.parse_args()

    clinics_by_dong = pd.read_parquet(args.clinics_by_dong)
    admin_centroid = pd.read_parquet(args.admin_centroid)

    n_by_dong = count_clinics_per_dong(clinics_by_dong)
    within = count_clinics_within_radius(clinics_by_dong, admin_centroid)

    # 모든 대상 동(653)을 기준으로 — 의원 0개 동은 n_clinic=0으로 유지
    full = admin_centroid[["adm_cd", "sido", "sgg", "adm_nm"]].merge(
        n_by_dong[["adm_cd", "n_clinic", "n_clinic_gi"]], on="adm_cd", how="left"
    )
    full[["n_clinic", "n_clinic_gi"]] = full[["n_clinic", "n_clinic_gi"]].fillna(0).astype(int)

    result = compute_competition_raw(full, within, population=None)

    from scoring.normalize import percentile_rank_inverted
    result["c_norm"] = percentile_rank_inverted(result["c_raw"])

    logger.info("쉬운 동(c_norm 상위 10, 경쟁 약함 = 개원 매력):")
    top = result.nlargest(10, "c_norm")[["adm_nm", "n_clinic", "n_within_radius", "c_raw", "c_norm"]]
    print(top.to_string(index=False))
    print()
    logger.info("어려운 동(c_norm 하위 10, 경쟁 강함):")
    bot = result.nsmallest(10, "c_norm")[["adm_nm", "n_clinic", "n_within_radius", "c_raw", "c_norm"]]
    print(bot.to_string(index=False))
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

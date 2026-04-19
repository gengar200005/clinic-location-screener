"""Top 30 동별 상세 JSON 생성 — Leaflet 인터랙티브 맵용.

출력: web/data/detail/{adm_cd}.json
스키마:
{
    "adm_cd": "11030630",
    "name": "서울특별시 용산구 이촌1동",
    "rank": 16,
    "score": 0.7752,
    "center": {"lat": 37.522, "lon": 126.972},
    "boundary": {GeoJSON Feature},
    "station": {"name": "이촌", "lat": ..., "lon": ..., "dist_m": 880,
                "n_clinic_500m": 9},
    "metrics": {"c_norm": ..., "p_norm": ..., "t_norm": ...,
                "pop_total": ..., "pop_40plus": ..., "ratio_40plus": ...,
                "t_raw": ..., "n_clinic": ..., "n_clinic_500m": ...,
                "n_clinic_1km": ..., "n_clinic_2km": ...,
                "med_desert": bool, "centroid_mismatch": bool, "suburban": bool},
    "clinics": [  // 반경 1km, 거리 오름차순
        {"name": ..., "lat": ..., "lon": ..., "dist": m,
         "kind": "의원", "is_gi": false, "drs": 2,
         "addr": "...", "tel": "...", "estb_year": "2010"},
        ...
    ]
}

사용:
    python -m publishers.web_export [--all]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from config.constants import (
    DATA_CLEANED,
    DATA_RAW,
    DATA_SCORED,
    EPSG_KOREA,
    EPSG_WGS84,
    ROOT,
)

logger = logging.getLogger(__name__)

WEB_DETAIL_DIR = ROOT / "web" / "data" / "detail"
RADIUS_M = 1000


def _latest_top30() -> Path:
    files = sorted(DATA_SCORED.glob("top30_*.parquet"))
    if not files:
        raise FileNotFoundError(f"{DATA_SCORED}/top30_*.parquet 없음")
    return files[-1]


def _latest_boundary() -> Path:
    files = sorted((DATA_RAW / "admin_boundary").glob("HangJeongDong_*.geojson"))
    if not files:
        raise FileNotFoundError("admin_boundary geojson 없음")
    return files[-1]


def load_all():
    top30 = pd.read_parquet(_latest_top30())
    centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")
    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")

    # 의원 EPSG:5179 좌표
    gdf = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(
            pd.to_numeric(clinics["XPos"]), pd.to_numeric(clinics["YPos"])
        ),
        crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    clinics = clinics.copy()
    clinics["x_5179"] = gdf.geometry.x.values
    clinics["y_5179"] = gdf.geometry.y.values

    # 지하철역 (옵션)
    from scoring.station_metrics import load_stations
    try:
        stations = load_stations()
    except FileNotFoundError:
        stations = None

    # 행정동 경계 GeoJSON (전체)
    boundary_gdf = gpd.read_file(_latest_boundary())
    if boundary_gdf.crs is None:
        boundary_gdf = boundary_gdf.set_crs(EPSG_WGS84)
    else:
        boundary_gdf = boundary_gdf.to_crs(EPSG_WGS84)

    return top30, centroid, clinics, stations, boundary_gdf


def _clinic_entry(cl: pd.Series, dist_m: int) -> dict:
    estb = str(cl.get("estbDd", "") or "")
    estb_year = estb[:4] if len(estb) >= 4 and estb.isdigit() else None
    return {
        "name": str(cl.get("yadmNm", "")),
        "lat": float(cl.get("YPos", 0)),
        "lon": float(cl.get("XPos", 0)),
        "dist": int(dist_m),
        "kind": str(cl.get("clCdNm", "")),
        "is_gi": bool(cl.get("is_gi", False)),
        "drs": int(cl.get("drTotCnt")) if pd.notna(cl.get("drTotCnt")) else None,
        "addr": str(cl.get("addr", "") or ""),
        "tel": str(cl.get("telno", "") or ""),
        "estb_year": estb_year,
    }


def build_detail_json(
    row: pd.Series,
    centroid: pd.DataFrame,
    clinics: pd.DataFrame,
    stations: pd.DataFrame | None,
    boundary_gdf: gpd.GeoDataFrame,
) -> dict:
    adm_cd = str(row["adm_cd"])
    cent = centroid[centroid["adm_cd"].astype(str) == adm_cd].iloc[0]
    cx, cy = float(cent["x_5179"]), float(cent["y_5179"])
    center_lat, center_lon = float(cent["lat"]), float(cent["lon"])

    # 반경 1km 의원 필터 + 거리 계산
    dx = clinics["x_5179"].to_numpy() - cx
    dy = clinics["y_5179"].to_numpy() - cy
    dist = np.sqrt(dx * dx + dy * dy)
    mask = dist <= RADIUS_M
    near = clinics[mask].copy()
    near["_dist"] = dist[mask].astype(int)
    near = near.sort_values("_dist")

    clinic_list = [_clinic_entry(cl, cl["_dist"]) for _, cl in near.iterrows()]

    # 경계 폴리곤 (GeoJSON Feature)
    boundary_feat = None
    # admin_boundary.geojson의 adm_cd는 string 8자리와 일치해야 함
    match = boundary_gdf[boundary_gdf["adm_cd"].astype(str) == adm_cd]
    if not match.empty:
        # 간단화 (Douglas-Peucker tol=0.0001 ≈ 10m)
        simplified = match.copy()
        simplified["geometry"] = simplified.geometry.simplify(0.0001, preserve_topology=True)
        boundary_feat = json.loads(simplified.to_json())["features"][0]

    # 역 정보
    station_info = None
    if stations is not None and pd.notna(row.get("nearest_station")):
        sta = stations[stations["name"] == row["nearest_station"]]
        if not sta.empty:
            station_info = {
                "name": str(row["nearest_station"]),
                "lat": float(sta.iloc[0]["lat"]),
                "lon": float(sta.iloc[0]["lon"]),
                "dist_m": int(row["station_dist_m"]),
                "n_clinic_500m": int(row.get("n_clinic_station_500m", 0)),
            }

    out = {
        "adm_cd": adm_cd,
        "name": str(row["adm_nm"]),
        "rank": int(row["rank"]),
        "score": round(float(row["score"]), 4),
        "center": {"lat": center_lat, "lon": center_lon},
        "boundary": boundary_feat,
        "station": station_info,
        "metrics": {
            "c_norm": round(float(row["c_norm"]), 4),
            "p_norm": round(float(row["p_norm"]), 4),
            "t_norm": round(float(row["t_norm"]), 4),
            "pop_total": int(row["pop_total"]),
            "pop_40plus": int(row["pop_40plus"]),
            "ratio_40plus": round(float(row["ratio_40plus"]), 4),
            "t_raw": int(row["t_raw"]),
            "n_clinic": int(row["n_clinic"]),
            "n_clinic_500m": int(row.get("n_clinic_500m", row.get("n_within_radius", 0))),
            "n_clinic_1km": int(row.get("n_clinic_1km", 0)),
            "n_clinic_2km": int(row.get("n_clinic_2km", 0)),
            "med_desert": bool(row.get("med_desert_flag", False)),
            "centroid_mismatch": bool(row.get("centroid_mismatch_flag", False)),
            "suburban": bool(row.get("suburban_cluster_flag", False)),
        },
        "clinics": clinic_list,
    }
    return out


def run(export_all: bool = False) -> int:
    WEB_DETAIL_DIR.mkdir(parents=True, exist_ok=True)
    top30, centroid, clinics, stations, boundary_gdf = load_all()

    if export_all:
        # 전체 653 동 (PWA용, 나중에)
        from scoring.population import load_kosis_population, merge_population
        # 이 경로는 일단 TODO, 지금은 Top 30만
        logger.warning("--all 모드 아직 미구현. Top 30만 내보냄.")

    count = 0
    for _, row in top30.iterrows():
        data = build_detail_json(row, centroid, clinics, stations, boundary_gdf)
        out = WEB_DETAIL_DIR / f"{row['adm_cd']}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        count += 1
        logger.info(
            "  [%2d] %s → %s (%d clinics)",
            row["rank"], row["adm_nm"], out.name, len(data["clinics"]),
        )
    logger.info("wrote %d JSON files to %s", count, WEB_DETAIL_DIR)
    return count


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Top 30 동별 상세 JSON 생성")
    parser.add_argument("--all", action="store_true", help="전체 653동 (아직 미구현)")
    args = parser.parse_args()
    run(export_all=args.all)
    return 0


if __name__ == "__main__":
    sys.exit(main())

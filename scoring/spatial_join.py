"""행정동 경계 → 중심점 계산 + 클리닉 공간조인.

- build_admin_centroid: GeoJSON → 필터 → representative_point → parquet
- join_clinics_to_dong: HIRA 클리닉 좌표 → 대상 행정동 폴리곤 매핑 → parquet

중심점은 representative_point() 사용: polygon.centroid가 concave 폴리곤에서
외부 점을 반환할 수 있는 문제 회피. 반드시 폴리곤 내부 점 보장.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

from config.constants import (
    DATA_CLEANED,
    DATA_RAW,
    EPSG_KOREA,
    EPSG_WGS84,
    ROOT,
)

logger = logging.getLogger(__name__)

TARGET_REGIONS_YAML = ROOT / "config" / "target_regions.yaml"

# 표준화된 출력 컬럼명
STD_COLS = {"code": "adm_cd", "sido": "sido", "sgg": "sgg", "name": "adm_nm"}


def load_target_regions() -> dict[str, list[str]]:
    with open(TARGET_REGIONS_YAML, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return {"서울특별시": cfg.get("seoul", []), "경기도": cfg.get("gyeonggi", [])}


def _detect_columns(gdf: gpd.GeoDataFrame) -> dict[str, str]:
    cols = {c.lower(): c for c in gdf.columns}
    resolved = {
        "sido": cols.get("sidonm") or cols.get("sido_nm") or cols.get("sido"),
        "sgg": cols.get("sggnm") or cols.get("sgg_nm") or cols.get("sgg"),
        "name": cols.get("adm_nm") or cols.get("admdong") or cols.get("dong"),
        "code": cols.get("adm_cd") or cols.get("code"),
        "code10": cols.get("adm_cd2"),  # 10자리 — KOSIS 인구 테이블과 매핑용
    }
    if not all([resolved[k] for k in ("sido", "sgg", "name", "code")]):
        raise RuntimeError(
            f"예상 컬럼 누락. 실제 컬럼: {list(gdf.columns)}. "
            "vuski/admdongkor 스키마 변경 가능성."
        )
    return resolved


def _filter_target(gdf: gpd.GeoDataFrame, cmap: dict[str, str]) -> gpd.GeoDataFrame:
    """target_regions.yaml에 지정된 시도·시군구만 남긴다.

    주의: 성남·고양·부천·안양은 구가 있어 sggnm이 "성남시 분당구" 형식.
    startswith 매칭으로 일반시·광역시 모두 포착.
    """
    targets = load_target_regions()
    mask = False
    for sido, sgg_list in targets.items():
        sido_mask = gdf[cmap["sido"]] == sido
        sgg_mask = gdf[cmap["sgg"]].str.startswith(tuple(sgg_list))
        mask = mask | (sido_mask & sgg_mask)
    return gdf[mask].copy().reset_index(drop=True)


def _load_target_polygons(geojson_path: Path) -> tuple[gpd.GeoDataFrame, dict[str, str]]:
    gdf = gpd.read_file(geojson_path)
    gdf = gdf.set_crs(EPSG_WGS84) if gdf.crs is None else gdf.to_crs(EPSG_WGS84)
    cmap = _detect_columns(gdf)
    filtered = _filter_target(gdf, cmap)
    logger.info("filtered %d / %d dongs", len(filtered), len(gdf))
    return filtered, cmap


def _find_latest_geojson() -> Path | None:
    candidates = sorted((DATA_RAW / "admin_boundary").glob("HangJeongDong_*.geojson"))
    return candidates[-1] if candidates else None


def apply_pop_weighted_centroid(centroid: pd.DataFrame) -> pd.DataFrame:
    """data/cache/admin_centroid_pop.parquet 있으면 lat/lon 교체 + 5179 재계산.

    centroid_mismatch_flag 동의 임장 좌표·반경 의원 카운트 정확도 향상.
    `catchment_pop_*km` 컬럼이 있으면 함께 merge (P_raw · density 분모용).
    cache 없으면 그대로 반환.
    """
    from config.constants import DATA_CACHE, EPSG_WGS84
    pop_path = DATA_CACHE / "admin_centroid_pop.parquet"
    if not pop_path.exists():
        logger.info("admin_centroid_pop.parquet 없음 → 기하 중심점 사용")
        return centroid

    pop = pd.read_parquet(pop_path)
    pop["adm_cd"] = pop["adm_cd"].astype(str)
    out = centroid.copy()
    out["adm_cd"] = out["adm_cd"].astype(str)
    before_n = len(out)

    merge_cols = ["adm_cd", "lat_pop", "lon_pop", "pop_weighted"]
    catchment_cols = [c for c in pop.columns if c.startswith("catchment_pop_")]
    merge_cols += catchment_cols

    out = out.merge(pop[merge_cols], on="adm_cd", how="left")
    matched = out["lat_pop"].notna().sum()
    logger.info(
        "pop-weighted 적용: %d / %d 동 (catchment 컬럼: %s)",
        matched, before_n, catchment_cols or "없음",
    )

    # lat/lon 교체 (NaN인 동은 기존 유지)
    out["lat"] = out["lat_pop"].where(out["lat_pop"].notna(), out["lat"])
    out["lon"] = out["lon_pop"].where(out["lon_pop"].notna(), out["lon"])

    # 5179 좌표 재계산
    gdf = gpd.GeoDataFrame(
        out, geometry=gpd.points_from_xy(out["lon"], out["lat"]), crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    out["x_5179"] = gdf.geometry.x.values
    out["y_5179"] = gdf.geometry.y.values

    # catchment 컬럼은 유지, 나머지 보조 컬럼만 정리
    out = out.drop(columns=["lat_pop", "lon_pop", "pop_weighted"])
    return out


def apply_shops_weighted_centroid(centroid: pd.DataFrame) -> pd.DataFrame:
    """data/cache/admin_centroid_shops.parquet 있으면 lat/lon 교체 + 5179 재계산.

    1·2층 상가 평균 좌표를 중심점으로 사용 (ADR-004). 인구 가중 mean이 의료상권과
    어긋나는 동에서 답사 좌표·반경 의원 카운트가 더 정확.

    `catchment_pop_*km` 컬럼이 있으면 함께 merge (P_raw · density 분모용).
    cache 없으면 인구 가중 폴백 (apply_pop_weighted_centroid 호출).
    """
    from config.constants import DATA_CACHE, EPSG_WGS84
    shops_path = DATA_CACHE / "admin_centroid_shops.parquet"
    if not shops_path.exists():
        logger.warning(
            "admin_centroid_shops.parquet 없음 → 인구 가중 폴백 "
            "(`python -m scoring.centroid_shops_weighted` 으로 생성)"
        )
        return apply_pop_weighted_centroid(centroid)

    shops = pd.read_parquet(shops_path)
    shops["adm_cd"] = shops["adm_cd"].astype(str)
    out = centroid.copy()
    out["adm_cd"] = out["adm_cd"].astype(str)
    before_n = len(out)

    merge_cols = ["adm_cd", "lat_shops", "lon_shops", "anchor"]
    catchment_cols = [c for c in shops.columns if c.startswith("catchment_pop_")]
    merge_cols += catchment_cols

    out = out.merge(shops[merge_cols], on="adm_cd", how="left")
    matched = out["lat_shops"].notna().sum()
    n_shops = (out["anchor"] == "shops").sum()
    n_pop = (out["anchor"] == "pop").sum()
    n_geom = (out["anchor"] == "geom").sum()
    logger.info(
        "shops-weighted 적용: %d / %d 동 (anchor: shops %d / pop %d / geom %d)",
        matched, before_n, n_shops, n_pop, n_geom,
    )

    # lat/lon 교체 (NaN인 동은 기존 유지)
    out["lat"] = out["lat_shops"].where(out["lat_shops"].notna(), out["lat"])
    out["lon"] = out["lon_shops"].where(out["lon_shops"].notna(), out["lon"])

    # 5179 좌표 재계산
    gdf = gpd.GeoDataFrame(
        out, geometry=gpd.points_from_xy(out["lon"], out["lat"]), crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    out["x_5179"] = gdf.geometry.x.values
    out["y_5179"] = gdf.geometry.y.values

    # catchment 컬럼은 유지, 나머지 보조 컬럼만 정리
    out = out.drop(columns=["lat_shops", "lon_shops", "anchor"])
    return out


def apply_centroid_overlay(centroid: pd.DataFrame) -> pd.DataFrame:
    """CENTROID_MODE에 따라 적절한 overlay 함수 호출.

    "shops": apply_shops_weighted_centroid
    "pop":   apply_pop_weighted_centroid
    "geom":  no-op (기하 centroid 그대로)
    """
    from config.constants import CENTROID_MODE
    if CENTROID_MODE == "shops":
        return apply_shops_weighted_centroid(centroid)
    if CENTROID_MODE == "pop":
        return apply_pop_weighted_centroid(centroid)
    if CENTROID_MODE == "geom":
        logger.info("CENTROID_MODE=geom → overlay 없음 (기하 중심점)")
        return centroid
    raise ValueError(f"unknown CENTROID_MODE={CENTROID_MODE!r}")


def build_admin_centroid(
    geojson_path: Path,
    out_path: Path | None = None,
) -> gpd.GeoDataFrame:
    if out_path is None:
        out_path = DATA_CLEANED / "admin_centroid.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("loading %s", geojson_path)
    filtered, cmap = _load_target_polygons(geojson_path)

    filtered["centroid"] = filtered.geometry.representative_point()
    filtered["lon"] = filtered["centroid"].x
    filtered["lat"] = filtered["centroid"].y

    proj = filtered.set_geometry("centroid").to_crs(EPSG_KOREA)
    filtered["x_5179"] = proj.geometry.x
    filtered["y_5179"] = proj.geometry.y

    out_cols = [cmap["code"], cmap["sido"], cmap["sgg"], cmap["name"],
                "lon", "lat", "x_5179", "y_5179"]
    rename_map = {
        cmap["code"]: STD_COLS["code"],
        cmap["sido"]: STD_COLS["sido"],
        cmap["sgg"]: STD_COLS["sgg"],
        cmap["name"]: STD_COLS["name"],
    }
    if cmap.get("code10"):
        out_cols.insert(1, cmap["code10"])
        rename_map[cmap["code10"]] = "adm_cd10"
    out_df = filtered[out_cols].rename(columns=rename_map)
    out_df.to_parquet(out_path, index=False)
    logger.info("saved %s (%d rows)", out_path, len(out_df))
    return filtered


def join_clinics_to_dong(
    clinics_parquet: Path,
    geojson_path: Path,
    out_path: Path | None = None,
) -> gpd.GeoDataFrame:
    """HIRA 클리닉 좌표를 대상 행정동 폴리곤에 매핑."""
    if out_path is None:
        out_path = DATA_CLEANED / "clinics_by_dong.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info("loading clinics %s", clinics_parquet)
    clinics = pd.read_parquet(clinics_parquet)
    n_raw = len(clinics)

    # 좌표 결측 제외 + HIRA 응답의 XPos/YPos는 경도/위도 순서
    clinics = clinics.dropna(subset=["XPos", "YPos"]).copy()
    logger.info("clinics with coords: %d / %d", len(clinics), n_raw)

    gdf_clinics = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(clinics["XPos"], clinics["YPos"]),
        crs=EPSG_WGS84,
    )

    logger.info("loading polygons %s", geojson_path)
    dongs, cmap = _load_target_polygons(geojson_path)

    # 공간조인: 클리닉 점이 어느 동 폴리곤 within인지
    cols_to_keep = ["geometry", cmap["code"], cmap["sido"], cmap["sgg"], cmap["name"]]
    joined = gpd.sjoin(
        gdf_clinics,
        dongs[cols_to_keep],
        how="inner",
        predicate="within",
    )
    logger.info("joined %d clinics → dongs (dropped %d outside target)",
                len(joined), len(gdf_clinics) - len(joined))

    joined = joined.drop(columns=["geometry", "index_right"]).rename(columns={
        cmap["code"]: STD_COLS["code"],
        cmap["sido"]: STD_COLS["sido"],
        cmap["sgg"]: STD_COLS["sgg"],
        cmap["name"]: STD_COLS["name"],
    })

    joined.to_parquet(out_path, index=False)
    logger.info("saved %s (%d rows)", out_path, len(joined))

    # 요약: 동별 클리닉 수 상위 10
    top = joined.groupby(STD_COLS["name"]).size().sort_values(ascending=False).head(10)
    logger.info("top 10 dongs by clinic count:\n%s", top.to_string())
    return joined


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Spatial join utilities")
    sub = parser.add_subparsers(dest="cmd", required=False)

    p_cent = sub.add_parser("centroid", help="행정동 중심점 parquet 생성")
    p_cent.add_argument("--geojson", type=Path, default=None)

    p_join = sub.add_parser("join-clinics", help="HIRA 클리닉 → 행정동 공간조인")
    p_join.add_argument("--clinics", type=Path, required=True)
    p_join.add_argument("--geojson", type=Path, default=None)

    args = parser.parse_args()

    # 하위 명령 미지정 시 centroid (기존 동작 유지)
    cmd = args.cmd or "centroid"

    geojson = getattr(args, "geojson", None) or _find_latest_geojson()
    if geojson is None:
        logger.error("GeoJSON 없음. `python -m scrapers.admin_boundary` 먼저.")
        return 1

    if cmd == "centroid":
        build_admin_centroid(geojson)
    elif cmd == "join-clinics":
        join_clinics_to_dong(args.clinics, geojson)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""웹 데이터 생성 — Leaflet 인터랙티브 맵용.

출력 3종:
- web/data/detail/{adm_cd}.json   Top 30 동별 상세 (의원 리스트 포함)
- web/data/heatmap.json           전체 동 점수 + 메타 (PWA 메인 페이지)
- web/data/boundaries.geojson     전체 동 경계 (simplify, choropleth용)

스키마 (detail/{adm_cd}.json):
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
    python -m publishers.web_export
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
import yaml

from config.constants import (
    DATA_CLEANED,
    DATA_RAW,
    DATA_SCORED,
    EPSG_KOREA,
    EPSG_WGS84,
    ROOT,
)

logger = logging.getLogger(__name__)

WEB_DATA_DIR = ROOT / "web" / "data"
WEB_DETAIL_DIR = WEB_DATA_DIR / "detail"
WEB_HEATMAP_PATH = WEB_DATA_DIR / "heatmap.json"
WEB_BOUNDARIES_PATH = WEB_DATA_DIR / "boundaries.geojson"
WEB_NARROW_PATH = WEB_DATA_DIR / "narrow_lists.json"
RADIUS_M = 1000
NEW_TOWNS_YAML = ROOT / "config" / "new_towns.yaml"
T_RAW_MAX = 50  # 자차 통근 컷 (이상은 narrow_lists에서 제외)


def load_new_towns() -> dict:
    if not NEW_TOWNS_YAML.exists():
        return {}
    with open(NEW_TOWNS_YAML, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def tag_new_town(adm_nm: str, sgg: str, towns: dict) -> str:
    text = f"{sgg} {adm_nm}"
    for tag, kws in towns.items():
        for kw in kws:
            if kw in text:
                return tag
    return ""

# 경계 단순화 tolerance (도 단위 ≈ 11m at lat 37). PLAN: ~1-2MB 목표.
# 0.0005 ≈ 50m: 동 모양 식별 가능 + 파일 1MB 내외.
BOUNDARY_SIMPLIFY_TOL = 0.0005


def _latest_top30() -> Path:
    files = sorted(DATA_SCORED.glob("top30_*.parquet"))
    if not files:
        raise FileNotFoundError(f"{DATA_SCORED}/top30_*.parquet 없음")
    return files[-1]


def _top_n_from_scores(scores: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    """top30 parquet 대신 scores.parquet에서 직접 Top N 추출 (rank 기준)."""
    return scores[scores["rank"] <= n].copy().sort_values("rank").reset_index(drop=True)


def _latest_scores() -> Path:
    files = sorted(DATA_SCORED.glob("scores_*.parquet"))
    if not files:
        raise FileNotFoundError(f"{DATA_SCORED}/scores_*.parquet 없음")
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

    # 답사 준비 외부 링크 (검색어 = adm_nm 자체)
    nm_q = str(row["adm_nm"]).replace(" ", "+")
    survey_links = {
        "kakao_map": f"https://map.kakao.com/?q={nm_q}",
        "naver_map": f"https://map.naver.com/v5/search/{nm_q}",
        "naver_estate": (
            f"https://land.naver.com/sales?ms={center_lat},{center_lon},16"
            f"&filter=PWR&a=SG&b=A1&e=RETAIL"
        ),
    }

    out = {
        "adm_cd": adm_cd,
        "name": str(row["adm_nm"]),
        "rank": int(row["rank"]),
        "rank_sido": int(row.get("rank_sido", -1)),
        "sido": str(row["sido"]),
        "sgg": str(row["sgg"]),
        "score": round(float(row["score"]), 4),
        "town_tag": str(row.get("town_tag", "") or ""),
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
            "catchment_pop_1_5km": (
                int(row["catchment_pop_1_5km"])
                if pd.notna(row.get("catchment_pop_1_5km")) else None
            ),
            "catchment_pop_40plus": (
                int(row["catchment_pop_40plus"])
                if pd.notna(row.get("catchment_pop_40plus")) else None
            ),
            "density_per_10k": (
                round(float(row["density_per_10k"]), 3)
                if pd.notna(row.get("density_per_10k")) else None
            ),
            "t_raw": int(row["t_raw"]),
            "t_transit": (
                int(row["t_transit"]) if pd.notna(row.get("t_transit")) else None
            ),
            "n_clinic": int(row["n_clinic"]),
            "n_clinic_med": int(row.get("n_clinic_med", 0)),
            "n_doctors_med": int(row.get("n_doctors_med", 0)),
            "n_clinic_gi": int(row.get("n_clinic_gi", 0)),
            "n_clinic_500m": int(row.get("n_clinic_500m", 0)),
            "n_clinic_1km": int(row.get("n_clinic_1km", 0)),
            "n_clinic_2km": int(row.get("n_clinic_2km", 0)),
            "n_within_radius_med": int(row.get("n_within_radius_med", 0)),
            "n_within_radius_all": int(row.get("n_within_radius_all", 0)),
            "n_doctors_within_radius_med": int(row.get("n_doctors_within_radius_med", 0)),
            "med_desert": bool(row.get("med_desert_flag", False)),
            "centroid_mismatch": bool(row.get("centroid_mismatch_flag", False)),
            "suburban": bool(row.get("suburban_cluster_flag", False)),
        },
        "survey_links": survey_links,
        "clinics": clinic_list,
    }
    return out


def export_heatmap(
    scores: pd.DataFrame, centroid: pd.DataFrame, scores_path: Path,
) -> Path:
    """전체 동 점수 + 좌표 → heatmap.json (PWA 메인 페이지용)."""
    # 좌표 머지 (centroid는 lat/lon 보유)
    cent_lite = centroid[["adm_cd", "lat", "lon"]].copy()
    cent_lite["adm_cd"] = cent_lite["adm_cd"].astype(str)
    df = scores.copy()
    df["adm_cd"] = df["adm_cd"].astype(str)
    df = df.merge(cent_lite, on="adm_cd", how="left")

    # adm_nm "서울특별시 용산구 이촌1동" → 동명만 (마지막 토큰)
    df["short"] = df["adm_nm"].apply(lambda s: str(s).split(" ")[-1])

    # 신도시 태그 + 시도순위 (이미 scores에 있으면 사용, 없으면 계산)
    if "town_tag" not in df.columns:
        towns = load_new_towns()
        df["town_tag"] = df.apply(
            lambda r: tag_new_town(r["adm_nm"], r["sgg"], towns), axis=1
        )
    if "rank_sido" not in df.columns:
        df["rank_sido"] = (
            df.groupby("sido")["score"].rank(method="min", ascending=False).astype(int)
        )

    dongs = []
    for _, r in df.iterrows():
        dongs.append({
            "adm_cd": str(r["adm_cd"]),
            "name": str(r["adm_nm"]),
            "short": str(r["short"]),
            "sido": str(r["sido"]),
            "sgg": str(r["sgg"]),
            "rank": int(r["rank"]),
            "rank_sido": int(r["rank_sido"]),
            "score": round(float(r["score"]), 4),
            "lat": round(float(r["lat"]), 6) if pd.notna(r["lat"]) else None,
            "lon": round(float(r["lon"]), 6) if pd.notna(r["lon"]) else None,
            "c": round(float(r["c_norm"]), 4),
            "p": round(float(r["p_norm"]), 4),
            "t": round(float(r["t_norm"]), 4),
            "n_clinic": int(r.get("n_clinic", 0)),
            "n_clinic_med": int(r.get("n_clinic_med", 0)),
            "n_doctors_med": int(r.get("n_doctors_med", 0)),
            "n_clinic_500m": int(r.get("n_clinic_500m", 0)),
            "pop_total": int(r.get("pop_total", 0)) if pd.notna(r.get("pop_total")) else None,
            "pop_40plus": int(r.get("pop_40plus", 0)) if pd.notna(r.get("pop_40plus")) else None,
            "t_raw": int(r["t_raw"]) if pd.notna(r["t_raw"]) else None,
            "med_desert": bool(r.get("med_desert_flag", False)),
            "suburban": bool(r.get("suburban_cluster_flag", False)),
            "town_tag": str(r.get("town_tag", "") or ""),
        })

    # 메타: 날짜는 파일명에서 추출 (scores_YYYY-MM-DD.parquet)
    gen_date = scores_path.stem.replace("scores_", "")
    sido_top30 = (
        df[df["rank"] <= 30]["sido"].value_counts().to_dict()
    )

    payload = {
        "generated_at": gen_date,
        "weights": {"c": 0.4, "p": 0.4, "t": 0.2},
        "stats": {
            "n_dongs": int(len(df)),
            "n_top30_by_sido": {str(k): int(v) for k, v in sido_top30.items()},
        },
        "dongs": dongs,
    }
    with open(WEB_HEATMAP_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = WEB_HEATMAP_PATH.stat().st_size / 1024
    logger.info("heatmap.json: %d dongs, %.1f KB", len(dongs), size_kb)
    return WEB_HEATMAP_PATH


def export_boundaries(
    boundary_gdf: gpd.GeoDataFrame,
    scores: pd.DataFrame,
    centroid: pd.DataFrame,
) -> Path:
    """대상 동 경계 → boundaries.geojson (simplified, score 속성 포함).

    centroid의 adm_cd 집합(서울+경기 9시 = 653)으로 먼저 필터.
    이후 simplify, score 머지. feature.id = adm_cd.
    """
    target_codes = set(centroid["adm_cd"].astype(str).tolist())

    score_lite = scores[["adm_cd", "score", "rank", "sido", "sgg", "adm_nm"]].copy()
    score_lite["adm_cd"] = score_lite["adm_cd"].astype(str)

    gdf = boundary_gdf.copy()
    gdf["adm_cd"] = gdf["adm_cd"].astype(str)
    # 대상 동만 필터 (전국 3558 → 653)
    before = len(gdf)
    gdf = gdf[gdf["adm_cd"].isin(target_codes)].copy()
    logger.info("boundary filter: %d → %d (target dongs)", before, len(gdf))
    # 스코어링 제외된 동(인구<500 등)도 boundary는 표시 → left merge
    gdf = gdf.merge(score_lite, on="adm_cd", how="left", suffixes=("", "_s"))

    # 단순화 (Douglas-Peucker)
    gdf["geometry"] = gdf.geometry.simplify(BOUNDARY_SIMPLIFY_TOL, preserve_topology=True)

    # 속성 정리 — 필수만
    keep_cols = ["adm_cd", "score", "rank", "geometry"]
    # adm_nm은 boundary 원본에도 있으므로 _s 접미가 붙은 score 머지본을 우선
    gdf["adm_nm_out"] = gdf.get("adm_nm_s", gdf.get("adm_nm"))
    gdf = gdf[["adm_cd", "adm_nm_out", "score", "rank", "geometry"]].rename(
        columns={"adm_nm_out": "adm_nm"}
    )

    # score NaN → null로 직렬화
    gdf["score"] = gdf["score"].astype("object").where(gdf["score"].notna(), None)
    gdf["rank"] = gdf["rank"].astype("object").where(gdf["rank"].notna(), None)

    geojson_str = gdf.to_json(drop_id=True)
    # feature.id = adm_cd 부여 (Leaflet에서 편함)
    feat_obj = json.loads(geojson_str)
    for f in feat_obj["features"]:
        f["id"] = f["properties"]["adm_cd"]
    with open(WEB_BOUNDARIES_PATH, "w", encoding="utf-8") as f:
        json.dump(feat_obj, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = WEB_BOUNDARIES_PATH.stat().st_size / 1024
    logger.info("boundaries.geojson: %d features, %.1f KB", len(feat_obj["features"]), size_kb)
    return WEB_BOUNDARIES_PATH


def run(top_n: int = 50) -> int:
    WEB_DETAIL_DIR.mkdir(parents=True, exist_ok=True)
    _, centroid, clinics, stations, boundary_gdf = load_all()

    # 점수 + 신도시 태그 + 시도순위 enrich
    scores_path = _latest_scores()
    scores = pd.read_parquet(scores_path)
    towns = load_new_towns()
    scores["town_tag"] = scores.apply(
        lambda r: tag_new_town(r["adm_nm"], r["sgg"], towns), axis=1
    )
    scores["rank_sido"] = (
        scores.groupby("sido")["score"].rank(method="min", ascending=False).astype(int)
    )

    # 1. Top N detail JSON
    top_df = _top_n_from_scores(scores, n=top_n)
    count = 0
    for _, row in top_df.iterrows():
        data = build_detail_json(row, centroid, clinics, stations, boundary_gdf)
        out = WEB_DETAIL_DIR / f"{row['adm_cd']}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        count += 1
        logger.info(
            "  [%2d] %s → %s (%d clinics, tag=%s, sido_rank=%d)",
            row["rank"], row["adm_nm"], out.name, len(data["clinics"]),
            row["town_tag"] or "-", int(row["rank_sido"]),
        )
    logger.info("wrote %d detail JSON files", count)

    # 2. heatmap.json (전체 동 점수 + 좌표 + 메타)
    export_heatmap(scores, centroid, scores_path)

    # 3. boundaries.geojson (choropleth)
    export_boundaries(boundary_gdf, scores, centroid)

    # 4. narrow_lists.json (5개 기준 Top10)
    export_narrow_lists(scores)

    return count


def export_narrow_lists(scores: pd.DataFrame) -> Path:
    """5개 기준 Top10 묶음 JSON. 메인 페이지의 '기준' 탭이 사용.

    기준: score / commute / new_town / desert / low_density
    """
    df = scores.copy()
    df["adm_cd"] = df["adm_cd"].astype(str)
    df = df[df["t_raw"] <= T_RAW_MAX].copy()

    def _pack(sub: pd.DataFrame) -> list[dict]:
        rows = []
        for _, r in sub.iterrows():
            rows.append({
                "adm_cd": str(r["adm_cd"]),
                "name": str(r["adm_nm"]),
                "short": str(r["adm_nm"]).split(" ")[-1],
                "sido": str(r["sido"]),
                "sgg": str(r["sgg"]),
                "rank": int(r["rank"]),
                "rank_sido": int(r.get("rank_sido", 0)),
                "score": round(float(r["score"]), 4),
                "score_adj": round(float(r.get("score_adj", r["score"])), 4),
                "t_raw": int(r["t_raw"]) if pd.notna(r["t_raw"]) else None,
                "n_clinic": int(r.get("n_clinic", 0)),
                "town_tag": str(r.get("town_tag", "") or ""),
            })
        return rows

    lists: dict[str, list[dict]] = {}

    # score
    s = df.sort_values("score", ascending=False).head(10).copy()
    s["score_adj"] = s["score"]
    lists["score"] = _pack(s)

    # commute (t_norm 가중 강화)
    d2 = df.copy()
    d2["score_adj"] = d2["score"] * (1 - 0.3 * (1 - d2["t_norm"]))
    lists["commute"] = _pack(d2.sort_values("score_adj", ascending=False).head(10))

    # new_town
    nt = df[df["town_tag"] != ""].sort_values("score", ascending=False).head(10).copy()
    nt["score_adj"] = nt["score"]
    lists["new_town"] = _pack(nt)

    # med_desert
    md = df[df["med_desert_flag"] == True].sort_values("score", ascending=False).head(10).copy()
    md["score_adj"] = md["score"]
    lists["desert"] = _pack(md)

    # low_density (인구 충분 + density 낮음)
    p_med = df["p_raw"].median()
    ld = df[df["p_raw"] >= p_med].sort_values("density_per_10k", ascending=True).head(10).copy()
    ld["score_adj"] = ld["score"]
    lists["low_density"] = _pack(ld)

    payload = {
        "criteria": {
            "score": {"label": "종합 점수", "desc": "C+P+T 가중합"},
            "commute": {"label": "통근 우선", "desc": "score × t_norm 가중"},
            "new_town": {"label": "신도시", "desc": "1·2·3기 신도시·재개발"},
            "desert": {"label": "의료사막", "desc": "med_desert 플래그"},
            "low_density": {"label": "인구당 의원 적음", "desc": "잠재 미충족 수요"},
        },
        "lists": lists,
    }
    with open(WEB_NARROW_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
    size_kb = WEB_NARROW_PATH.stat().st_size / 1024
    logger.info("narrow_lists.json: 5 criteria × 10 dongs, %.1f KB", size_kb)
    return WEB_NARROW_PATH


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="웹 데이터 생성 (detail + heatmap + boundaries)")
    parser.parse_args()
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""OSM Overpass API로 수도권 지하철·광역철도 역 좌표 수집.

행정동 중심점은 상권 중심이 아닐 수 있음 (이촌1동 centroid가 공원쪽,
실제 상권은 이촌역). 역 좌표를 앵커로 추가해서 "역세권 경쟁도" 계산.

출력: data/cache/subway_stations.parquet (영구 커밋)
- columns: name, lat, lon, x_5179, y_5179
- 중복 제거 (같은 역 여러 플랫폼 노드가 OSM에 있을 수 있음)

Query: 수도권 (37.0~37.85°N, 126.3~127.4°E) 범위의
- railway=station + subway=yes (지하철)
- station=subway (지하철 태깅 변형)
- railway=station + train=yes (광역철도, 공항철도·경의중앙·수인분당 등)

무료 API. 요청당 ~1MB, 1~3초.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_CACHE, EPSG_KOREA, EPSG_WGS84

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
CACHE_PATH = DATA_CACHE / "subway_stations.parquet"

# 수도권 범위
BBOX = (37.0, 126.3, 37.85, 127.4)  # S, W, N, E

QUERY = f"""
[out:json][timeout:60];
(
  node["railway"="station"]["subway"="yes"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  node["station"="subway"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  node["railway"="station"]["train"="yes"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
  node["railway"="station"]["light_rail"="yes"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
);
out;
"""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=3, min=3, max=30),
    reraise=True,
)
def _fetch_overpass() -> list[dict]:
    logger.info("Overpass API 호출 (bbox=%s)", BBOX)
    resp = requests.post(OVERPASS_URL, data=QUERY.encode("utf-8"), timeout=90)
    resp.raise_for_status()
    data = resp.json()
    elements = data.get("elements") or []
    logger.info("  받은 노드: %d", len(elements))
    return elements


def _dedup_by_name_and_location(df: pd.DataFrame, dedup_radius_m: int = 150) -> pd.DataFrame:
    """OSM엔 같은 역이 플랫폼·출구별로 여러 노드로 존재.
    같은 이름이면 하나만, 이름 다르더라도 150m 내면 중복으로 간주."""
    if df.empty:
        return df
    # 1차: 이름 기준 deduplication (좌표 평균)
    by_name = df.groupby("name", as_index=False).agg(
        lat=("lat", "mean"),
        lon=("lon", "mean"),
    )
    # 2차: 이름 다르더라도 근접 노드 dedup (EPSG:5179 투영 후)
    gdf = gpd.GeoDataFrame(
        by_name,
        geometry=gpd.points_from_xy(by_name.lon, by_name.lat),
        crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    by_name["x_5179"] = gdf.geometry.x.values
    by_name["y_5179"] = gdf.geometry.y.values

    # Greedy dedup
    keep = [True] * len(by_name)
    xs = by_name["x_5179"].values
    ys = by_name["y_5179"].values
    r2 = dedup_radius_m ** 2
    for i in range(len(by_name)):
        if not keep[i]:
            continue
        for j in range(i + 1, len(by_name)):
            if not keep[j]:
                continue
            if (xs[i] - xs[j]) ** 2 + (ys[i] - ys[j]) ** 2 <= r2:
                # 짧은 이름 우선 (보통 기본 역명)
                if len(by_name.iloc[j]["name"]) < len(by_name.iloc[i]["name"]):
                    keep[i] = False
                    break
                else:
                    keep[j] = False

    return by_name[keep].reset_index(drop=True)


def fetch_stations(force: bool = False) -> Path:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if CACHE_PATH.exists() and not force:
        logger.info("skip: %s exists", CACHE_PATH)
        return CACHE_PATH

    elements = _fetch_overpass()
    rows = []
    for e in elements:
        tags = e.get("tags") or {}
        name = tags.get("name:ko") or tags.get("name") or ""
        if not name:
            continue
        # 역 아닌 것 제거 (버스정류장·지점 등 혼입 방지)
        rail = tags.get("railway") or tags.get("station") or ""
        if rail not in ("station", "subway"):
            if tags.get("station") != "subway" and tags.get("railway") != "station":
                continue
        rows.append({
            "name": name.strip(),
            "lat": e.get("lat"),
            "lon": e.get("lon"),
        })

    df = pd.DataFrame(rows)
    logger.info("raw stations: %d", len(df))
    df = _dedup_by_name_and_location(df)
    logger.info("deduped stations: %d", len(df))

    df[["name", "lat", "lon", "x_5179", "y_5179"]].to_parquet(CACHE_PATH, index=False)
    logger.info("saved %s", CACHE_PATH)

    # 요약
    logger.info("샘플 (처음 10): %s", df["name"].head(10).tolist())
    return CACHE_PATH


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="수도권 지하철·광역철도역 좌표 수집 (OSM)")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    fetch_stations(force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

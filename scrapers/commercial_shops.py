"""소상공인시장진흥공단 상가(상권)정보 → 동별 1·2층 상가 cluster.

입력: data/raw/commercial/소상공인시장진흥공단_*_서울_*.csv 등
필터: context_sido(서울·경기·인천) + 1·2층 + 좌표 유효
출력: data/cleaned/shops_by_dong.parquet
  · 동별: n_shops_total, n_shops_floor12, lat_mean, lon_mean, hull_wkt
  · 답사 anchor (1종 근생 매물 가능 영역) 정밀도 ↑

v7: 후보 동 판정(통근 필터)이 집계 이후 단계라 시도 전체를 집계한다.

사용:
    python -m scrapers.commercial_shops
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
from shapely.geometry import MultiPoint

from config.constants import DATA_CLEANED, DATA_RAW
from config.regions import context_sido

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("commercial")

SRC_DIR = DATA_RAW / "commercial"
OUT_PATH = DATA_CLEANED / "shops_by_dong.parquet"

USE_COLS = ["시도명", "시군구명", "행정동코드", "행정동명",
            "경도", "위도", "층정보", "상권업종대분류명"]


# 상가 CSV 파일명 태그 (소상공인시장진흥공단_상가(상권)정보_{태그}_YYYYMM.csv)
SIDO_FILE_TAG = {"서울특별시": "서울", "경기도": "경기", "인천광역시": "인천"}


def parse_floor(v) -> int | None:
    """층정보 → int. '1', '1.0', '지하1층' 등 처리."""
    if pd.isna(v):
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        s = str(v)
        if "지하" in s or "B" in s.upper():
            return -1
        # 숫자만 추출 시도
        import re
        m = re.search(r"-?\d+", s)
        return int(m.group()) if m else None


def main():
    sidos = context_sido()
    files = []
    for sido in sidos:
        found = sorted(SRC_DIR.glob(f"*_{SIDO_FILE_TAG[sido]}_*.csv"))
        if not found:
            raise FileNotFoundError(f"{sido} 상가 CSV 없음 ({SRC_DIR})")
        files.append(found[-1])
    log.info("source files: %s", [f.name for f in files])

    pieces = []
    for f in files:
        log.info("loading %s", f.name)
        df = pd.read_csv(f, usecols=USE_COLS, dtype=str, low_memory=False)
        # 좌표 숫자 변환
        df["경도"] = pd.to_numeric(df["경도"], errors="coerce")
        df["위도"] = pd.to_numeric(df["위도"], errors="coerce")
        log.info("  raw rows: %d", len(df))

        df = df[df["시도명"].isin(sidos)].copy()
        log.info("  after sido filter: %d", len(df))

        # 좌표 유효성
        df = df[df["경도"].notna() & df["위도"].notna()]
        log.info("  with coords: %d", len(df))

        # 층 파싱
        df["floor"] = df["층정보"].apply(parse_floor)
        pieces.append(df)

    all_df = pd.concat(pieces, ignore_index=True)
    log.info("all shops in target: %d", len(all_df))

    # 행정동코드 8자리 → "00" 붙여 우리 adm_cd10 (10자리) 매칭
    all_df = all_df.rename(columns={"행정동코드": "_admcd8"})
    all_df["adm_cd10"] = all_df["_admcd8"].astype(str) + "00"

    # 1·2층만 (의원 적합)
    floor12 = all_df[all_df["floor"].isin([1, 2])].copy()
    log.info("floor 1-2 only: %d / %d", len(floor12), len(all_df))

    # 동별 집계
    n_total_by_dong = all_df.groupby("adm_cd10").size()
    rows = []
    for adm_cd10, sub in floor12.groupby("adm_cd10"):
        n_total = n_total_by_dong.get(adm_cd10, 0)
        lat_mean = float(sub["위도"].mean())
        lon_mean = float(sub["경도"].mean())
        # convex hull (3개 이상)
        hull_wkt = None
        if len(sub) >= 3:
            mp = MultiPoint(list(zip(sub["경도"], sub["위도"])))
            hull = mp.convex_hull
            if hull.geom_type == "Polygon":
                hull_wkt = hull.wkt
        rows.append({
            "adm_cd10": str(adm_cd10),
            "adm_nm": str(sub["행정동명"].iloc[0]),
            "sgg": str(sub["시군구명"].iloc[0]),
            "sido": str(sub["시도명"].iloc[0]),
            "n_shops_total": int(n_total),
            "n_shops_floor12": int(len(sub)),
            "shops_lat_mean": round(lat_mean, 6),
            "shops_lon_mean": round(lon_mean, 6),
            "shops_hull_wkt": hull_wkt,
        })
    out = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_PATH, index=False)
    log.info("saved %s (%d dongs)", OUT_PATH, len(out))
    log.info("sample:")
    print(out.head(5)[["adm_cd10", "sgg", "adm_nm", "n_shops_total",
                       "n_shops_floor12", "shops_lat_mean", "shops_lon_mean"]].to_string(index=False))


if __name__ == "__main__":
    sys.exit(main())

"""행정안전부 지방행정 인허가 — 의원 (전국, 개업·폐업 이력 포함).

data.go.kr 15154874 「행정안전부_건강_의원 조회서비스」 (2026-04 localdata.go.kr 이관).
- endpoint: https://apis.data.go.kr/1741000/clinics/info (serviceKey = HIRA_KEY와 동일 data.go.kr 키)
- 페이지당 최대 100건 → 전국 ~12.6만 건 ≈ 1,260회 호출
- 좌표 CRD_INFO_X/Y: EPSG:5174 (보정계수 없는 Bessel 중부원점TM) → WGS84 변환
- 주요 컬럼: BPLC_NM(사업장명), LCPMT_YMD(인허가일), CLSBIZ_YMD(폐업일),
  SALS_STTS_NM(영업/정상·폐업 등), MDEXM_SBJCT_CN_NM(진료과목), ROAD_NM_ADDR/LOTNO_ADDR

용도: v7 생존율 지표 (scoring/survival.py). 폐업 의원은 HIRA 현황 API에 없으므로 이 데이터가 유일.

출력: data/raw/mois/mois_clinics_{date}.parquet

실행:
    python -m scrapers.mois_clinics --date 2026-10-12
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv
from pyproj import Transformer
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

URL = "https://apis.data.go.kr/1741000/clinics/info"
PAGE_SIZE = 100          # API 상한
WORKERS = 4
SRC_EPSG = 5174


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30), reraise=True)
def _fetch_page(key: str, page: int) -> tuple[list[dict], int]:
    resp = requests.get(
        URL,
        params={"serviceKey": key, "pageNo": page, "numOfRows": PAGE_SIZE, "type": "json"},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()["response"]
    header = data.get("header") or {}
    if str(header.get("resultCode", "0")) not in ("0", "00"):
        raise RuntimeError(f"MOIS [{header.get('resultCode')}] {header.get('resultMsg')}")
    body = data["body"]
    items = (body.get("items") or {}).get("item") or []
    if isinstance(items, dict):
        items = [items]
    return items, int(body.get("totalCount") or 0)


def fetch(date_str: str, force: bool = False):
    out_dir = DATA_RAW / "mois"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"mois_clinics_{date_str}.parquet"
    if out.exists() and not force:
        logger.info("skip: %s exists", out)
        return out

    load_dotenv()
    key = os.environ.get("HIRA_KEY")
    if not key:
        raise RuntimeError(".env HIRA_KEY(data.go.kr 키) 미설정")

    first, total = _fetch_page(key, 1)
    n_pages = -(-total // PAGE_SIZE)
    logger.info("totalCount=%d → %d pages", total, n_pages)

    rows = list(first)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for i, (items, _) in enumerate(ex.map(lambda p: _fetch_page(key, p), range(2, n_pages + 1)), 2):
            rows.extend(items)
            if i % 200 == 0:
                logger.info("  %d / %d pages", i, n_pages)

    df = pd.DataFrame(rows)
    if len(df) < total * 0.99:
        raise RuntimeError(f"수집 누락: {len(df)} / {total}")

    x = pd.to_numeric(df["CRD_INFO_X"], errors="coerce")
    y = pd.to_numeric(df["CRD_INFO_Y"], errors="coerce")
    lon, lat = Transformer.from_crs(SRC_EPSG, 4326, always_xy=True).transform(x.values, y.values)
    df["lon"], df["lat"] = lon, lat
    df.loc[x.isna() | y.isna(), ["lon", "lat"]] = None
    for col in ("LCPMT_YMD", "CLSBIZ_YMD"):
        df[col] = pd.to_datetime(df[col].replace("", None), errors="coerce")

    df.to_parquet(out, index=False)
    logger.info(
        "saved %s (%d rows, 좌표 결측 %d, 상태 %s)",
        out, len(df), int(df["lon"].isna().sum()),
        df["SALS_STTS_NM"].value_counts().to_dict(),
    )
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="행안부 의원 인허가 전국 수집")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    fetch(args.date, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())


# ─── 좌표 결측 보충 (Kakao 주소 검색) ───────────────────────────────
# 폐업 의원은 34%가 좌표 결측(2026-10-12, 서울·경기·인천 내과) → 반경 기반 폐업 카운트 과소.
# 도로명/지번 주소로 지오코딩. 캐시: data/cache/mois_geocode.parquet (MNG_NO 키, 영구 커밋).

KAKAO_ADDR_URL = "https://dapi.kakao.com/v2/local/search/address.json"
GEOCODE_CACHE = DATA_RAW.parent / "cache" / "mois_geocode.parquet"


def _clean_addr(addr: str) -> str:
    """'…로 26, 지하1층 B101호 (마포동)' → '…로 26'. 지번 뒤 건물명도 제거."""
    import re
    a = str(addr or "").split(",")[0].split("(")[0].strip()
    m = re.match(r"^(.*?\d+(?:-\d+)?)(?:\s|$)", a)
    return m.group(1) if m else a


def _geocode_one(key: str, addr: str) -> tuple[float | None, float | None]:
    if not addr:
        return None, None
    resp = requests.get(
        KAKAO_ADDR_URL,
        headers={"Authorization": f"KakaoAK {key}"},
        params={"query": addr, "analyze_type": "similar"},
        timeout=15,
    )
    if resp.status_code in (401, 403, 429):
        raise RuntimeError(f"Kakao {resp.status_code}: {resp.text[:150]}")
    docs = resp.json().get("documents") or []
    if not docs:
        return None, None
    return float(docs[0]["x"]), float(docs[0]["y"])


def fill_missing_coords(df: pd.DataFrame) -> pd.DataFrame:
    """lon/lat 결측 행을 도로명 → 지번 주소 순으로 지오코딩. geocoded 컬럼 추가."""
    load_dotenv()
    key = os.environ.get("KAKAO_KEY")
    cache = (pd.read_parquet(GEOCODE_CACHE) if GEOCODE_CACHE.exists()
             else pd.DataFrame(columns=["MNG_NO", "g_lon", "g_lat"]))
    todo = df[df["lon"].isna() & ~df["MNG_NO"].isin(cache["MNG_NO"])]
    if len(todo) and key:
        def run(r):
            for raw in (r.ROAD_NM_ADDR, r.LOTNO_ADDR):
                lon, lat = _geocode_one(key, _clean_addr(raw))
                if lon is not None:
                    return {"MNG_NO": r.MNG_NO, "g_lon": lon, "g_lat": lat}
            return {"MNG_NO": r.MNG_NO, "g_lon": None, "g_lat": None}
        with ThreadPoolExecutor(max_workers=4) as ex:
            new = list(ex.map(run, todo.itertuples(index=False)))
        cache = pd.concat([cache, pd.DataFrame(new)], ignore_index=True)
        GEOCODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
        cache.to_parquet(GEOCODE_CACHE, index=False)
        logger.info("geocode: %d건 시도, 성공 %d", len(new), sum(n["g_lon"] is not None for n in new))
    out = df.merge(cache, on="MNG_NO", how="left")
    out["geocoded"] = out["lon"].isna() & out["g_lon"].notna()
    out["lon"] = out["lon"].fillna(out["g_lon"])
    out["lat"] = out["lat"].fillna(out["g_lat"])
    return out.drop(columns=["g_lon", "g_lat"])

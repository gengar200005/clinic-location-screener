"""HIRA 병의원 정보서비스 수집.

Endpoint: apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList
진료과목코드 01(내과) + 종별코드 31(의원) 필터.
시도: 서울 110000, 경기 410000 (경기는 이후 공간조인으로 대상 9시만 필터).

페이징: numOfRows=1000, pageNo=1..N. 마지막 페이지는 totalCount 기반 판단.
재시도: 네트워크 오류 시 지수 백오프.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import xml.etree.ElementTree as ET

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

BASE_URL = "https://apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList"

# HIRA 시도코드 (행정안전부 법정동 코드와 상이).
# 확인됨: 서울=110000, 경기=310000. 410000은 HIRA에서 세종특별자치시를 반환.
SIDO_CODES = {
    "서울": "110000",
    "경기": "310000",
}

# 내과 전체. 소화기내과는 HIRA에서 별도 코드 없음 → 병원명 태깅으로 별도 처리.
DGSBJT_INTERNAL_MEDICINE = "01"
CL_CLINIC = "31"  # 의원

NUM_OF_ROWS = 1000
REQUEST_TIMEOUT = 30


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _fetch_page(service_key: str, sido_cd: str, page_no: int) -> tuple[list[dict], int]:
    """HIRA API 1페이지 호출. (items, totalCount) 반환. XML 응답 파싱."""
    params = {
        "serviceKey": service_key,  # data.go.kr 표준: 소문자 s
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
        "sidoCd": sido_cd,
        "clCd": CL_CLINIC,
        "dgsbjtCd": DGSBJT_INTERNAL_MEDICINE,
    }
    resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)

    # 헤더 체크
    result_code = root.findtext(".//resultCode")
    if result_code not in (None, "00", "0"):
        result_msg = root.findtext(".//resultMsg") or "unknown"
        raise RuntimeError(f"HIRA 응답 오류 [{result_code}]: {result_msg}")

    total = int(root.findtext(".//totalCount") or 0)

    items = []
    for item in root.findall(".//items/item"):
        row = {child.tag: child.text for child in item}
        items.append(row)

    return items, total


def fetch_region(service_key: str, sido_name: str, sido_cd: str) -> pd.DataFrame:
    logger.info("fetching HIRA %s (sidoCd=%s)", sido_name, sido_cd)

    rows, total = _fetch_page(service_key, sido_cd, 1)
    logger.info("  totalCount=%d", total)

    total_pages = (total + NUM_OF_ROWS - 1) // NUM_OF_ROWS
    for page_no in range(2, total_pages + 1):
        page_rows, _ = _fetch_page(service_key, sido_cd, page_no)
        rows.extend(page_rows)
        logger.info("  page %d/%d: +%d rows", page_no, total_pages, len(page_rows))

    df = pd.DataFrame(rows)
    df["sido_query"] = sido_name
    logger.info("  collected %d rows for %s", len(df), sido_name)
    return df


def fetch_all(date_str: str, force: bool = False, test_mode: bool = False) -> Path:
    load_dotenv()
    service_key = os.environ.get("HIRA_KEY")
    if not service_key:
        raise RuntimeError(".env의 HIRA_KEY 미설정. data.go.kr에서 발급 후 기입.")

    out_dir = DATA_RAW / "hira"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"hira_{date_str}.parquet"

    if out.exists() and not force:
        logger.info("skip: %s already exists", out)
        return out

    frames = []
    for sido_name, sido_cd in SIDO_CODES.items():
        if test_mode:
            logger.info("[TEST] %s: 1페이지만 수집", sido_name)
            rows, total = _fetch_page(service_key, sido_cd, 1)
            logger.info("[TEST] totalCount=%d, got %d rows", total, len(rows))
            df = pd.DataFrame(rows)
            df["sido_query"] = sido_name
        else:
            df = fetch_region(service_key, sido_name, sido_cd)
        frames.append(df)

    merged = pd.concat(frames, ignore_index=True)

    # 좌표 숫자화 (HIRA는 string으로 반환)
    for col in ("XPos", "YPos"):
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

    # 소화기내과 태깅 (병원명에 "소화기" 포함)
    if "yadmNm" in merged.columns:
        merged["is_gi"] = merged["yadmNm"].str.contains("소화기", na=False)

    merged.to_parquet(out, index=False)
    logger.info("saved %s (%d rows, coords missing: %d)",
                out, len(merged),
                merged["XPos"].isna().sum() if "XPos" in merged.columns else -1)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="HIRA 병의원 정보 수집")
    parser.add_argument("--date", default=date.today().isoformat(), help="YYYY-MM-DD")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--test", action="store_true", help="각 시도 1페이지만 (빠른 검증)")
    args = parser.parse_args()

    fetch_all(args.date, force=args.force, test_mode=args.test)
    return 0


if __name__ == "__main__":
    sys.exit(main())

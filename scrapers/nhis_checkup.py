"""국민건강보험공단 국가건강검진기관 — 지역별 검진기관 (위암·대장암 검진 여부 포함).

data.go.kr 15154419 「국민건강보험공단_검진기관 찾기 조회」.
- 실제 endpoint: https://apis.data.go.kr/B550928/HmcSearchService/getRegnHmcList
  (상세 페이지에 표기된 openapigw.nhis.or.kr 주소는 "key parameter error" — 페이지 내장 swagger의 host가 정답)
- serviceKey = data.go.kr 키 (HIRA_KEY와 동일)
- siDoCd: 행안부 2자리 (11 서울 · 41 경기 · 28 인천)
- 주요 필드: hmcNo, hmcNm, ykindnm(의원/병원/종합병원…), cxVl/cyVl(WGS84 경도/위도),
  stmcaExmdChrgTypeCd(위암), ccExmdChrgTypeCd(대장암) — 1=실시
  위암검진은 위내시경 또는 위장조영 중 택1이라 "위암 검진기관"이 곧 내시경 가능은 아님.

출력: data/raw/nhis/nhis_checkup_{date}.parquet

실행:
    python -m scrapers.nhis_checkup --date 2026-10-13
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

URL = "https://apis.data.go.kr/B550928/HmcSearchService/getRegnHmcList"
SIDO_CODES = {"서울특별시": "11", "경기도": "41", "인천광역시": "28"}
PAGE_SIZE = 1000


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=2, min=2, max=30), reraise=True)
def _fetch_page(key: str, sido_cd: str, page: int) -> tuple[list[dict], int]:
    resp = requests.get(
        URL, params={"serviceKey": key, "siDoCd": sido_cd, "pageNo": page, "numOfRows": PAGE_SIZE},
        timeout=60,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    code = root.findtext(".//resultCode")
    if code not in ("00", "0"):
        raise RuntimeError(f"NHIS [{code}] {root.findtext('.//resultMsg')}")
    items = [{c.tag: c.text for c in it} for it in root.iter("item")]
    return items, int(root.findtext(".//totalCount") or 0)


def fetch(date_str: str, force: bool = False):
    out_dir = DATA_RAW / "nhis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"nhis_checkup_{date_str}.parquet"
    if out.exists() and not force:
        logger.info("skip: %s exists", out)
        return out
    load_dotenv()
    key = os.environ.get("HIRA_KEY")
    if not key:
        raise RuntimeError(".env HIRA_KEY(data.go.kr 키) 미설정")

    frames = []
    for sido, cd in SIDO_CODES.items():
        rows, total = _fetch_page(key, cd, 1)
        for page in range(2, -(-total // PAGE_SIZE) + 1):
            rows += _fetch_page(key, cd, page)[0]
        if len(rows) < total:
            raise RuntimeError(f"{sido} 수집 누락 {len(rows)}/{total}")
        logger.info("%s: %d / %d", sido, len(rows), total)
        frames.append(pd.DataFrame(rows).assign(sido=sido))

    df = pd.concat(frames, ignore_index=True).drop_duplicates("hmcNo")
    df["lon"] = pd.to_numeric(df["cxVl"], errors="coerce")
    df["lat"] = pd.to_numeric(df["cyVl"], errors="coerce")
    df["stomach"] = df["stmcaExmdChrgTypeCd"].eq("1")
    df["colon"] = df["ccExmdChrgTypeCd"].eq("1")
    df.to_parquet(out, index=False)
    logger.info(
        "saved %s (%d기관, 위암 %d, 대장암 %d, 좌표결측 %d) 종별 %s",
        out, len(df), int(df["stomach"].sum()), int(df["colon"].sum()),
        int(df["lon"].isna().sum()), df["ykindnm"].value_counts().to_dict(),
    )
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="건보공단 검진기관 (서울·경기·인천)")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    fetch(args.date, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

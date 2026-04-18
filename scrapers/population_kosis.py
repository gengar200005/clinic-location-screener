"""KOSIS 오픈API: 행정동별 주민등록 인구 수집.

통계표: 행정구역(읍면동)별/5세별 주민등록인구 (orgId=101, tblId=DT_1B04005N)
- C1 = 행정구역 코드 (10자리, vuski adm_cd2와 동일)
- C2 = 5세별 연령 (0=전체 합계)
- ITM_ID=T2 = 총인구수

MVP: objL2='0' (연령 계)만 수집 → 총인구. 40대+ 비율은 Post-MVP.
단일 호출 (최신 1개월), 약 3,910 rows. 40,000 제한 대비 여유.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

BASE_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
ORG_ID = "101"
TBL_ID = "DT_1B04005N"


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _fetch(api_key: str) -> list[dict]:
    params = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": ORG_ID,
        "tblId": TBL_ID,
        "itmId": "T2",      # 총인구수
        "objL1": "ALL",      # 모든 행정구역 (시도/시군구/읍면동 혼재)
        "objL2": "0",        # 연령 계
        "format": "json",
        "jsonVD": "Y",
        "prdSe": "M",
        "newEstPrdCnt": "1",
    }
    resp = requests.get(BASE_URL, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "err" in data:
        raise RuntimeError(f"KOSIS error [{data['err']}]: {data.get('errMsg')}")
    return data


def fetch_population(date_str: str, force: bool = False) -> Path:
    load_dotenv()
    api_key = os.environ.get("KOSIS_KEY")
    if not api_key:
        raise RuntimeError(".env의 KOSIS_KEY 미설정.")

    out_dir = DATA_RAW / "population"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"kosis_pop_{date_str}.parquet"

    if out.exists() and not force:
        logger.info("skip: %s exists", out)
        return out

    logger.info("fetching KOSIS DT_1B04005N")
    rows = _fetch(api_key)
    df = pd.DataFrame(rows)
    logger.info("  received %d rows, PRD_DE=%s", len(df),
                sorted(df["PRD_DE"].unique()) if "PRD_DE" in df.columns else "-")

    # 10자리(읍면동) 행만 필터. 시도 2자리, 시군구 5자리는 제외.
    df = df[df["C1"].str.len() == 10].copy()
    df["DT"] = pd.to_numeric(df["DT"], errors="coerce")
    df = df.rename(columns={"C1": "adm_cd10", "C1_NM": "dong_nm", "DT": "population"})

    keep = ["adm_cd10", "dong_nm", "population", "PRD_DE"]
    df[keep].to_parquet(out, index=False)
    logger.info("saved %s (%d dongs, latest PRD_DE=%s)",
                out, len(df), df["PRD_DE"].max())
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="KOSIS 행정동 주민등록 인구")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    fetch_population(args.date, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

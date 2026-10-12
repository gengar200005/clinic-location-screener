"""KOSIS 오픈API: 행정동별 주민등록 인구 수집.

통계표: 행정구역(읍면동)별/5세별 주민등록인구 (orgId=101, tblId=DT_1B04005N)
- C1 = 행정구역 코드 (10자리, vuski adm_cd2와 동일)
- C2 = 5세별 연령 ("0"=전체 합계, 45=40-44, 50=45-49, ..., 105=100+)
- ITM_ID=T2 = 총인구수

출력:
- kosis_pop_{date}.parquet      총인구 (C2='0')
- kosis_pop_age_{date}.parquet  연령대별 + 50~69세 합산 (v7 타깃 연령)

API 한도: 요청당 40,000행. objL2 콤마 미지원 → 밴드별 개별 호출 (4회).
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

# C2 코드 맵 (5세 단위, "45"=40~44세 시작)
# v7 (2026-10-12): 타깃 연령 40+ → 50~69세 (사용자 결정 — 가온내과 50~70대 검진 타깃).
AGE_CODE_5069 = ["55", "60", "65", "70"]   # 50~54, 55~59, 60~64, 65~69세 (4밴드)


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    reraise=True,
)
def _fetch_raw(api_key: str, obj_l2: str, prd_de: str | None = None) -> list[dict]:
    """KOSIS API 호출. obj_l2는 단일 코드. prd_de(YYYYMM) 지정 시 해당 월, 없으면 최신."""
    params = {
        "method": "getList",
        "apiKey": api_key,
        "orgId": ORG_ID,
        "tblId": TBL_ID,
        "itmId": "T2",
        "objL1": "ALL",
        "objL2": obj_l2,
        "format": "json",
        "jsonVD": "Y",
        "prdSe": "M",
    }
    if prd_de:
        params.update(startPrdDe=prd_de, endPrdDe=prd_de)
    else:
        params["newEstPrdCnt"] = "1"
    resp = requests.get(BASE_URL, params=params, timeout=90)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "err" in data:
        raise RuntimeError(f"KOSIS error [{data['err']}]: {data.get('errMsg')}")
    return data


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    """10자리(읍면동) 행만 남기고 DT를 숫자로."""
    df = df[df["C1"].str.len() == 10].copy()
    df["DT"] = pd.to_numeric(df["DT"], errors="coerce")
    return df


def fetch_population(date_str: str, force: bool = False) -> Path:
    """총인구 (C2='0')만 수집. 기존 호환."""
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

    logger.info("fetching KOSIS DT_1B04005N (총인구)")
    rows = _fetch_raw(api_key, "0")
    df = pd.DataFrame(rows)
    logger.info("  received %d rows, PRD_DE=%s", len(df),
                sorted(df["PRD_DE"].unique()) if "PRD_DE" in df.columns else "-")

    df = _clean(df)
    df = df.rename(columns={"C1": "adm_cd10", "C1_NM": "dong_nm", "DT": "population"})
    df[["adm_cd10", "dong_nm", "population", "PRD_DE"]].to_parquet(out, index=False)
    logger.info("saved %s (%d dongs, latest PRD_DE=%s)",
                out, len(df), df["PRD_DE"].max())
    return out


def fetch_population_age(date_str: str, force: bool = False) -> Path:
    """연령대별 인구 수집 + 50~69세 합산.

    출력 컬럼: adm_cd10, pop_total, pop_5069, ratio_5069
    """
    load_dotenv()
    api_key = os.environ.get("KOSIS_KEY")
    if not api_key:
        raise RuntimeError(".env의 KOSIS_KEY 미설정.")

    out_dir = DATA_RAW / "population"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"kosis_pop_age_{date_str}.parquet"

    if out.exists() and not force:
        logger.info("skip: %s exists", out)
        return out

    # KOSIS objL2는 콤마 리스트 미지원(err 21). 코드별 개별 호출 (13회, ~30초).
    frames = []
    for i, code in enumerate(AGE_CODE_5069, 1):
        logger.info("  [%d/%d] C2=%s fetching...", i, len(AGE_CODE_5069), code)
        f = pd.DataFrame(_fetch_raw(api_key, code))
        f = _clean(f)
        frames.append(f)
    age = pd.concat(frames, ignore_index=True)
    logger.info("  total age-band rows: %d (expected ~%d)",
                len(age), 3622 * len(AGE_CODE_5069))
    pop_5069 = age.groupby("C1")["DT"].sum().rename("pop_5069").reset_index()
    pop_5069 = pop_5069.rename(columns={"C1": "adm_cd10"})

    # 총인구 로드 (없으면 동시 수집)
    total_path = DATA_RAW / "population" / f"kosis_pop_{date_str}.parquet"
    if not total_path.exists():
        fetch_population(date_str, force=False)
    total = pd.read_parquet(total_path)[["adm_cd10", "population"]].rename(
        columns={"population": "pop_total"}
    )

    # 병합 + 비율
    merged = total.merge(pop_5069, on="adm_cd10", how="left")
    merged["pop_5069"] = merged["pop_5069"].fillna(0).astype(int)
    merged["pop_total"] = merged["pop_total"].astype(int)
    merged["ratio_5069"] = (
        merged["pop_5069"] / merged["pop_total"].where(merged["pop_total"] > 0)
    ).round(4)

    merged.to_parquet(out, index=False)
    logger.info(
        "saved %s (%d dongs, total median=%d, 50~69 median=%d, ratio median=%.3f)",
        out, len(merged),
        int(merged["pop_total"].median()),
        int(merged["pop_5069"].median()),
        merged["ratio_5069"].median(),
    )
    return out


def fetch_population_age_at(prd_de: str, force: bool = False) -> Path:
    """특정 월(YYYYMM)의 총인구 + 50~69세 + 동 이름 (추세 비교용).

    출력: kosis_pop_age_at_{prd_de}.parquet [adm_cd10, dong_nm, pop_total, pop_5069, PRD_DE]
    """
    load_dotenv()
    api_key = os.environ.get("KOSIS_KEY")
    if not api_key:
        raise RuntimeError(".env의 KOSIS_KEY 미설정.")
    out = DATA_RAW / "population" / f"kosis_pop_age_at_{prd_de}.parquet"
    if out.exists() and not force:
        logger.info("skip: %s exists", out)
        return out

    total = _clean(pd.DataFrame(_fetch_raw(api_key, "0", prd_de)))
    frames = [_clean(pd.DataFrame(_fetch_raw(api_key, code, prd_de))) for code in AGE_CODE_5069]
    age = pd.concat(frames, ignore_index=True).groupby("C1")["DT"].sum().rename("pop_5069")
    df = total.rename(columns={"C1": "adm_cd10", "C1_NM": "dong_nm", "DT": "pop_total"})
    df = df.merge(age, left_on="adm_cd10", right_index=True, how="left")
    df = df[["adm_cd10", "dong_nm", "pop_total", "pop_5069", "PRD_DE"]]
    df.to_parquet(out, index=False)
    logger.info("saved %s (%d dongs, PRD_DE=%s)", out, len(df), df["PRD_DE"].unique().tolist())
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="KOSIS 행정동 주민등록 인구 + 연령대")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--age", action="store_true",
                        help="50~69세 포함 연령대별 수집 (권장). 기본은 총인구만.")
    args = parser.parse_args()

    if args.age:
        fetch_population(args.date, force=args.force)
        fetch_population_age(args.date, force=args.force)
    else:
        fetch_population(args.date, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

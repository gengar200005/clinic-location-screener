"""인구 지표 P 계산.

**P_raw = catchment_pop_1_5km × ratio_40plus** (기본, catchment 기반)
   · catchment: 중심점 반경 1.5km WorldPop 인구 합 (행정동 경계 무시)
   · ratio_40plus: 동 단위 KOSIS 연령 비율 (동 수준 근사)
**P_raw = pop_40plus** (폴백, catchment 없을 때)

근거: 소화기내과 유효 환자풀은 40+.
- 20대 대부분 급성 1회성. 40+부터 만성질환 빈도 급증 (GI·HTN·DM·검진)
- 국가검진(위·대장내시경) 대상 50+에서 수요 집중

배후세대 보정: 행정동 경계는 인위적. "작은 동 + 인접 대단지" 케이스(행신2동 등)가
동 단위 pop_40plus로는 과소평가됨. catchment 1.5km는 실상권 규모로, 동 경계 밖의
배후세대까지 포착. 연령 비율은 동 단위 통계를 그대로 적용 (근사).

데이터:
- 기본(권장): data/raw/population/kosis_pop_age_{date}.parquet — 총인구 + 40+ 합
             + data/cache/admin_centroid_pop.parquet 의 catchment 컬럼
- 폴백: data/raw/population/kosis_pop_{date}.parquet — 총인구만 (P_raw=총인구)

KOSIS는 10자리 행정구역 코드(adm_cd10)로 제공. admin_centroid의 `adm_cd10` 컬럼이 브리지.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.constants import DATA_RAW, MIN_POPULATION

logger = logging.getLogger(__name__)


def load_kosis_population(parquet_path: Path | None = None) -> pd.DataFrame:
    """KOSIS 수집 결과 → (adm_cd10, pop_total, pop_40plus, ratio_40plus) 테이블.

    우선순위:
    1. kosis_pop_age_*.parquet — 연령대 포함 (40+ 가능)
    2. kosis_pop_*.parquet — 총인구만 (폴백, pop_40plus=NaN)

    parquet_path 지정 시 해당 파일만 사용.
    """
    pop_dir = DATA_RAW / "population"

    if parquet_path is not None:
        df = pd.read_parquet(parquet_path)
    else:
        age_files = sorted(pop_dir.glob("kosis_pop_age_*.parquet"))
        if age_files:
            df = pd.read_parquet(age_files[-1])
            logger.info("population source: %s (연령대 포함)", age_files[-1].name)
        else:
            totals = sorted(pop_dir.glob("kosis_pop_*.parquet"))
            totals = [p for p in totals if "age" not in p.name]
            if not totals:
                raise FileNotFoundError(
                    "KOSIS 인구 parquet 없음. "
                    "`python -m scrapers.population_kosis --age` 먼저."
                )
            df = pd.read_parquet(totals[-1])
            df = df.rename(columns={"population": "pop_total"})
            df["pop_40plus"] = pd.NA
            df["ratio_40plus"] = pd.NA
            logger.warning("40+ 데이터 없음 — pop_total만 사용. "
                           "`python -m scrapers.population_kosis --age`로 40+ 수집 권장.")

    # 표준 컬럼 반환
    cols = ["adm_cd10", "pop_total"]
    if "pop_40plus" in df.columns:
        cols += ["pop_40plus", "ratio_40plus"]
    return df[cols].copy()


def _find_catchment_col(df: pd.DataFrame) -> str | None:
    """dong_table/merged에서 catchment_pop_*km 컬럼 이름 탐색. 없으면 None."""
    for c in df.columns:
        if c.startswith("catchment_pop_") and c.endswith("km"):
            return c
    return None


def merge_population(
    dong_table: pd.DataFrame,
    pop: pd.DataFrame,
) -> pd.DataFrame:
    """행정동 테이블에 인구 병합 + MIN_POPULATION 필터.

    P_raw 정책 (우선순위):
    1. catchment_pop_*km 컬럼 + ratio_40plus 있으면
       → P_raw = catchment_pop × ratio_40plus (배후세대 기반 40+ 풀)
    2. pop_40plus 있으면 → P_raw = pop_40plus (동 단위 폴백)
    3. 그 외 → P_raw = pop_total
    """
    merged = dong_table.merge(pop, on="adm_cd10", how="left")
    missing = merged["pop_total"].isna().sum()
    if missing:
        logger.warning("인구 매칭 실패: %d 동", missing)

    # MIN_POPULATION은 동 단위 총인구 기준 (공단·공원 동 배제)
    before = len(merged)
    merged = merged[merged["pop_total"].fillna(0) >= MIN_POPULATION].copy()
    logger.info("population filter (>= %d): %d / %d", MIN_POPULATION, len(merged), before)

    # P_raw 설정
    catchment_col = _find_catchment_col(merged)
    has_catchment = (
        catchment_col is not None
        and merged[catchment_col].notna().any()
    )
    has_ratio = (
        "ratio_40plus" in merged.columns
        and merged["ratio_40plus"].notna().any()
    )
    has_40plus = (
        "pop_40plus" in merged.columns
        and merged["pop_40plus"].notna().any()
    )

    if has_catchment and has_ratio:
        catch = merged[catchment_col].astype(float)
        ratio = merged["ratio_40plus"].astype(float)
        # catchment 또는 ratio NaN인 행은 동 단위 pop_40plus 로 폴백
        p_catchment = catch * ratio
        merged["catchment_pop_40plus"] = p_catchment
        if has_40plus:
            merged["p_raw"] = p_catchment.where(
                p_catchment.notna() & (p_catchment > 0),
                merged["pop_40plus"].astype(float),
            )
        else:
            merged["p_raw"] = p_catchment.fillna(0.0)
        logger.info(
            "P_raw = %s × ratio_40plus (중앙값=%d, 동 pop_40plus 중앙값=%d)",
            catchment_col,
            int(merged["p_raw"].median()),
            int(merged["pop_40plus"].median()) if has_40plus else -1,
        )
    elif has_40plus:
        merged["p_raw"] = merged["pop_40plus"].astype(float)
        logger.info(
            "P_raw = pop_40plus (catchment 없음 — 동 단위 폴백, 중앙값=%d)",
            int(merged["pop_40plus"].median()),
        )
    else:
        merged["p_raw"] = merged["pop_total"].astype(float)
        logger.warning("P_raw = pop_total (40+ 데이터 없음)")

    return merged

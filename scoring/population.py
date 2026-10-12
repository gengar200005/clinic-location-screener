"""인구 지표 P 계산.

**P_raw = catchment_pop_1_5km × ratio_5069** (기본, catchment 기반)
   · catchment: 중심점 반경 1.5km WorldPop 인구 합 (행정동 경계 무시)
   · ratio_5069: 동 단위 KOSIS 연령 비율 (동 수준 근사)
**P_raw = pop_5069** (폴백, catchment 없을 때)

근거 (v7, 2026-10-12): 타깃 50~69세. 가온내과는 50~70대 위·대장 내시경 검진 중심.
- v6까지는 40+ (만성질환 빈도 급증 연령). 사용자 결정으로 50~69세로 좁힘.
- 국가검진(위·대장내시경) 수요가 50대 이후 집중.

배후세대 보정: 행정동 경계는 인위적. "작은 동 + 인접 대단지" 케이스(행신2동 등)가
동 단위 pop_5069로는 과소평가됨. catchment 1.5km는 실상권 규모로, 동 경계 밖의
배후세대까지 포착. 연령 비율은 동 단위 통계를 그대로 적용 (근사).

데이터:
- 기본(권장): data/raw/population/kosis_pop_age_{date}.parquet — 총인구 + 50~69세 합
             + data/cache/admin_centroid_pop.parquet 의 catchment 컬럼
- 폴백: data/raw/population/kosis_pop_{date}.parquet — 총인구만 (P_raw=총인구)

KOSIS는 10자리 행정구역 코드(adm_cd10)로 제공. admin_centroid의 `adm_cd10` 컬럼이 브리지.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from config.constants import DATA_RAW, MIN_POPULATION

logger = logging.getLogger(__name__)


def load_kosis_population(parquet_path: Path | None = None) -> pd.DataFrame:
    """KOSIS 수집 결과 → (adm_cd10, pop_total, pop_5069, ratio_5069) 테이블.

    우선순위:
    1. kosis_pop_age_*.parquet — 연령대 포함 (50~69세)
    2. kosis_pop_*.parquet — 총인구만 (폴백, pop_5069=NaN)

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
            df["pop_5069"] = pd.NA
            df["ratio_5069"] = pd.NA
            logger.warning("50~69세 데이터 없음 — pop_total만 사용. "
                           "`python -m scrapers.population_kosis --age`로 연령대 수집 권장.")

    # 동 이름 (코드 crosswalk용) — 같은 날짜의 총인구 파일에서
    if "dong_nm" not in df.columns and parquet_path is None and age_files:
        total_path = pop_dir / age_files[-1].name.replace("kosis_pop_age_", "kosis_pop_")
        if total_path.exists():
            names = pd.read_parquet(total_path)[["adm_cd10", "dong_nm"]]
            df = df.merge(names, on="adm_cd10", how="left")

    # 표준 컬럼 반환
    cols = ["adm_cd10", "pop_total"]
    if "pop_5069" in df.columns:
        cols += ["pop_5069", "ratio_5069"]
    if "dong_nm" in df.columns:
        cols += ["dong_nm"]
    return df[cols].copy()


def _find_catchment_col(df: pd.DataFrame) -> str | None:
    """dong_table/merged에서 catchment_pop_*km 컬럼 이름 탐색. 없으면 None."""
    for c in df.columns:
        if c.startswith("catchment_pop_") and c.endswith("km"):
            return c
    return None


def _norm_dong_name(s: str) -> str:
    return re.sub(r"[·.\s]", "", str(s))


def crosswalk_codes(dong_table: pd.DataFrame, pop: pd.DataFrame) -> pd.Series:
    """경계 adm_cd10 → KOSIS adm_cd10 매핑 (코드 불일치 동은 같은 시도 안에서 동 이름으로).

    배경 (2026-10-12): 인천 행정체제 개편(2026-07, 중구·동구 → 제물포구·영종구,
    서구 → 서구·검단구)으로 KOSIS(2026-09~)는 새 구 코드, 경계 GeoJSON(ver20260201)은
    옛 코드. 동 이름은 유지되어 이름 매칭이 1:1 (인천 48동 검증).
    이름이 시도 안에서 유일하지 않거나 못 찾으면 원래 코드 유지 → 인구 결측으로 보고.
    """
    codes = dong_table["adm_cd10"].astype(str)
    if "dong_nm" not in pop.columns:
        return codes
    kosis = pop[["adm_cd10", "dong_nm"]].astype(str)
    missing = ~codes.isin(set(kosis["adm_cd10"]))
    if not missing.any():
        return codes
    pool = kosis[~kosis["adm_cd10"].isin(set(codes))].copy()
    pool["sido2"] = pool["adm_cd10"].str[:2]
    pool["key"] = pool["dong_nm"].map(_norm_dong_name)
    pool = pool.drop_duplicates(["sido2", "key"], keep=False)  # 시도 내 동명 중복은 매칭 안 함
    lookup = pool.set_index(["sido2", "key"])["adm_cd10"]

    out = codes.copy()
    names = dong_table["adm_nm"].astype(str).str.split().str[-1].map(_norm_dong_name)
    n_hit = 0
    for idx in codes[missing].index:
        hit = lookup.get((codes[idx][:2], names[idx]))
        if hit is not None:
            out[idx] = hit
            n_hit += 1
    logger.info("인구 코드 crosswalk (동 이름): %d / %d 동 매칭", n_hit, int(missing.sum()))
    return out


def merge_population(
    dong_table: pd.DataFrame,
    pop: pd.DataFrame,
) -> pd.DataFrame:
    """행정동 테이블에 인구 병합 + MIN_POPULATION 필터.

    P_raw 정책 (우선순위):
    1. catchment_pop_*km 컬럼 + ratio_5069 있으면
       → P_raw = catchment_pop × ratio_5069 (배후세대 기반 50~69세 풀)
    2. pop_5069 있으면 → P_raw = pop_5069 (동 단위 폴백)
    3. 그 외 → P_raw = pop_total
    """
    dong_table = dong_table.copy()
    dong_table["adm_cd10_pop"] = crosswalk_codes(dong_table, pop)
    pop = pop.drop(columns=["dong_nm"], errors="ignore").rename(columns={"adm_cd10": "adm_cd10_pop"})
    merged = dong_table.merge(pop, on="adm_cd10_pop", how="left")
    missing = merged["pop_total"].isna()
    if missing.any():
        logger.warning(
            "인구 매칭 실패: %d 동 → 근거 불충분으로 제외: %s",
            int(missing.sum()), merged.loc[missing, "adm_nm"].tolist()[:20],
        )

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
        "ratio_5069" in merged.columns
        and merged["ratio_5069"].notna().any()
    )
    has_target_age = (
        "pop_5069" in merged.columns
        and merged["pop_5069"].notna().any()
    )

    if has_catchment and has_ratio:
        catch = merged[catchment_col].astype(float)
        ratio = merged["ratio_5069"].astype(float)
        # catchment 또는 ratio NaN인 행은 동 단위 pop_5069 로 폴백
        p_catchment = catch * ratio
        merged["catchment_pop_5069"] = p_catchment
        if has_target_age:
            merged["p_raw"] = p_catchment.where(
                p_catchment.notna() & (p_catchment > 0),
                merged["pop_5069"].astype(float),
            )
        else:
            merged["p_raw"] = p_catchment.fillna(0.0)
        logger.info(
            "P_raw = %s × ratio_5069 (중앙값=%d, 동 pop_5069 중앙값=%d)",
            catchment_col,
            int(merged["p_raw"].median()),
            int(merged["pop_5069"].median()) if has_target_age else -1,
        )
    elif has_target_age:
        merged["p_raw"] = merged["pop_5069"].astype(float)
        logger.info(
            "P_raw = pop_5069 (catchment 없음 — 동 단위 폴백, 중앙값=%d)",
            int(merged["pop_5069"].median()),
        )
    else:
        merged["p_raw"] = merged["pop_total"].astype(float)
        logger.warning("P_raw = pop_total (50~69세 데이터 없음)")

    return merged

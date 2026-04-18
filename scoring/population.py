"""인구 지표 P 계산.

MVP: P_raw = 총인구 (KOSIS DT_1B04005N, objL2='0' 전체 연령).
Post-MVP: 40대+ 비율 추가 → P_raw = 0.6 · P_total + 0.4 · HouseholdRatio_40+

KOSIS는 10자리 행정구역 코드(adm_cd10)로 제공. vuski `adm_cd2`와 동일.
admin_centroid에 저장된 `adm_cd10` 컬럼이 브리지 역할.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.constants import DATA_RAW, MIN_POPULATION

logger = logging.getLogger(__name__)


def load_kosis_population(parquet_path: Path | None = None) -> pd.DataFrame:
    """KOSIS 수집 결과 → (adm_cd10, population) 테이블.

    parquet_path가 None이면 data/raw/population/ 에서 최신 파일 자동 선택.
    """
    if parquet_path is None:
        candidates = sorted((DATA_RAW / "population").glob("kosis_pop_*.parquet"))
        if not candidates:
            raise FileNotFoundError(
                "KOSIS 인구 parquet 없음. `python -m scrapers.population_kosis` 먼저."
            )
        parquet_path = candidates[-1]

    df = pd.read_parquet(parquet_path)
    return df[["adm_cd10", "population"]].copy()


def merge_population(
    dong_table: pd.DataFrame,
    pop: pd.DataFrame,
) -> pd.DataFrame:
    """행정동 테이블(adm_cd + adm_cd10)에 인구 병합.

    MIN_POPULATION 미만은 제외 (공단·공원 등 비거주 동).
    """
    merged = dong_table.merge(pop, on="adm_cd10", how="left")
    missing = merged["population"].isna().sum()
    if missing:
        logger.warning("인구 매칭 실패: %d 동", missing)

    before = len(merged)
    merged = merged[merged["population"].fillna(0) >= MIN_POPULATION].copy()
    logger.info("population filter (>= %d): %d / %d", MIN_POPULATION, len(merged), before)
    return merged

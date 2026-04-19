"""통근 지표 T 계산.

T_raw = ODSay 대중교통 소요시간(분), 이촌역 원점 → 동 중심점.
낮을수록 좋음 → percentile_rank_inverted 적용 (weighted_sum.py에서).

ODSay 실패 동은 COMMUTE_FALLBACK_MIN(999)로 캐시되어 자동 최하위.
실패가 많으면 scrapers.odsay_transit을 --force로 재시도.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.constants import COMMUTE_FALLBACK_MIN, DATA_CACHE

logger = logging.getLogger(__name__)

CACHE_PATH = DATA_CACHE / "odsay_commute.parquet"


def load_commute(parquet_path: Path | None = None) -> pd.DataFrame:
    """캐시 → (adm_cd, minutes) 테이블."""
    path = parquet_path or CACHE_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"ODSay 캐시 없음: {path}. `python -m scrapers.odsay_transit` 먼저."
        )
    df = pd.read_parquet(path)
    df["adm_cd"] = df["adm_cd"].astype(str)
    return df[["adm_cd", "minutes"]].copy()


def merge_commute(dong_table: pd.DataFrame, commute: pd.DataFrame) -> pd.DataFrame:
    """행정동 테이블에 소요시간 병합. 미매칭은 fallback 값 채움."""
    dong_table = dong_table.copy()
    dong_table["adm_cd"] = dong_table["adm_cd"].astype(str)
    merged = dong_table.merge(commute, on="adm_cd", how="left")

    missing = merged["minutes"].isna().sum()
    if missing:
        logger.warning("commute 매칭 실패: %d 동 → fallback(%d)",
                       missing, COMMUTE_FALLBACK_MIN)
        merged["minutes"] = merged["minutes"].fillna(COMMUTE_FALLBACK_MIN).astype(int)
    else:
        merged["minutes"] = merged["minutes"].astype(int)

    merged = merged.rename(columns={"minutes": "t_raw"})
    return merged

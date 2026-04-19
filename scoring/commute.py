"""통근 지표 T 계산.

T_raw = 자차 소요시간(분), 이촌역 원점 → 동 중심점.
낮을수록 좋음 → percentile_rank_inverted 적용 (weighted_sum.py에서).

우선순위:
1. **Kakao Mobility** (data/cache/kakao_car.parquet) — 평일 07:30 누적통계 ⭐ 기본
2. ODSay (data/cache/odsay_commute.parquet) — 대중교통, fallback

의원 출근은 자차가 주력이라 Kakao를 primary로 사용. ODSay 캐시는 히스토릭 용도로
보존(PLAN.md 영구 캐시 원칙) 및 대중교통 비교 필요 시 활용.

실패 동은 COMMUTE_FALLBACK_MIN(999)로 캐시되어 자동 최하위.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from config.constants import COMMUTE_FALLBACK_MIN, DATA_CACHE

logger = logging.getLogger(__name__)

KAKAO_CACHE = DATA_CACHE / "kakao_car.parquet"
ODSAY_CACHE = DATA_CACHE / "odsay_commute.parquet"


def load_commute(
    parquet_path: Path | None = None,
    source: str = "auto",
) -> pd.DataFrame:
    """캐시 → (adm_cd, minutes) 테이블.

    source:
    - "auto" (기본): Kakao 우선, 없으면 ODSay
    - "kakao": Kakao만
    - "odsay": ODSay만
    - 직접 경로: parquet_path 지정
    """
    if parquet_path is not None:
        path = parquet_path
    elif source == "kakao":
        path = KAKAO_CACHE
    elif source == "odsay":
        path = ODSAY_CACHE
    elif source == "auto":
        if KAKAO_CACHE.exists():
            path = KAKAO_CACHE
            logger.info("commute source = Kakao Mobility (자차 누적통계)")
        elif ODSAY_CACHE.exists():
            path = ODSAY_CACHE
            logger.info("commute source = ODSay (대중교통, Kakao 캐시 없음)")
        else:
            raise FileNotFoundError(
                "Kakao·ODSay 캐시 모두 없음. "
                "`python -m scrapers.kakao_car` 또는 `python -m scrapers.odsay_transit` 먼저."
            )
    else:
        raise ValueError(f"unknown source: {source}")

    if not path.exists():
        raise FileNotFoundError(f"캐시 없음: {path}")

    df = pd.read_parquet(path)
    df["adm_cd"] = df["adm_cd"].astype(str)

    # Kakao 캐시는 동일 adm_cd에 여러 departure_time 스냅샷 가능 → 최신 하나만.
    if "departure_time" in df.columns:
        df = df.sort_values("departure_time").drop_duplicates("adm_cd", keep="last")

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

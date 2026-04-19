"""3개 지표 정규화 + 가중합.

Score_i = W_COMPETITION · C_norm
        + W_POPULATION  · P_norm
        + W_COMMUTE     · T_norm

정규화 방향 (docs/PLAN.md §3.4):
- C_norm: 경쟁 낮을수록 좋음 → percentile_rank_inverted(c_raw)
- P_norm: 인구 많을수록 좋음 → percentile_rank(p_raw)
- T_norm: 소요시간 짧을수록 좋음 → percentile_rank_inverted(t_raw)

MIN_POPULATION 미만 동은 미리 제외되어 들어와야 함 (scoring.population에서).
"""
from __future__ import annotations

import logging

import pandas as pd

from config.constants import (
    W_COMMUTE,
    W_COMPETITION,
    W_POPULATION,
)
from scoring.normalize import percentile_rank, percentile_rank_inverted

logger = logging.getLogger(__name__)


def compute_final_scores(df: pd.DataFrame) -> pd.DataFrame:
    """c_raw, p_raw, t_raw → 정규화 + 가중합 → 최종 Score.

    입력: columns 최소 [adm_cd, adm_nm, c_raw, p_raw, t_raw] (+ 부가 컬럼들)
    출력: 원본 + [c_norm, p_norm, t_norm, score], score 내림차순 정렬
    """
    required = {"c_raw", "p_raw", "t_raw"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    out = df.copy()
    out["c_norm"] = percentile_rank_inverted(out["c_raw"])
    out["p_norm"] = percentile_rank(out["p_raw"])
    out["t_norm"] = percentile_rank_inverted(out["t_raw"])

    out["score"] = (
        W_COMPETITION * out["c_norm"]
        + W_POPULATION * out["p_norm"]
        + W_COMMUTE * out["t_norm"]
    )

    out = out.sort_values("score", ascending=False).reset_index(drop=True)
    out["rank"] = out.index + 1

    # 합산 sanity: W_* 합이 1 아니면 경고
    total_w = W_COMPETITION + W_POPULATION + W_COMMUTE
    if abs(total_w - 1.0) > 1e-6:
        logger.warning("가중치 합 %.4f ≠ 1.0 — config/constants.py 확인", total_w)

    return out


def top_n(scored: pd.DataFrame, n: int = 30) -> pd.DataFrame:
    """Top N 행만 추출. 기본 30."""
    return scored.head(n).copy()

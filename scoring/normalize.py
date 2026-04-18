"""Percentile rank 정규화.

Score 구성 요소(경쟁·인구·통근)를 [0,1] 범위로 표준화한다.
방향 처리는 호출자 책임: 낮을수록 좋은 지표는 호출 전 또는 후에 `1 - x` 적용.

근거 (docs/PLAN.md §3.4):
- 이상치 강건: rank 기반이므로 극단값 영향 없음
- 해석 용이: "상위 N%" 직관
- 분포 가정 없음: 로그정규분포(인구·의원수)에도 안전
"""
from __future__ import annotations

import pandas as pd


def percentile_rank(s: pd.Series) -> pd.Series:
    """Series → [0,1] percentile rank. 동점은 평균 rank.

    rank_pct(x_i) = #{j : x_j <= x_i} / N
    pd.Series.rank(pct=True)는 위 정의와 동일 (method='average' 기본).
    """
    return s.rank(pct=True, method="average")


def percentile_rank_inverted(s: pd.Series) -> pd.Series:
    """낮을수록 좋은 지표용: 1 - percentile_rank."""
    return 1.0 - percentile_rank(s)

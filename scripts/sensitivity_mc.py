"""v7 가중치 몬테카를로 민감도 — 가중치 ±30% 무작위화 N회, 후보별 순위 분포.

무작위화 대상 (각각 독립 Uniform(1−SPREAD, 1+SPREAD) 배수):
- 상위: W_COMPETITION, W_POPULATION (합 1로 재정규화)
- 경쟁 서브: W_COMP_DENSITY, W_COMP_RADIUS, W_COMP_STATION
  (W_COMP_SUBCLUSTER=0은 0 유지, W_GI_MULTIPLIER는 원천 카운트 재계산이 필요해 제외)

c_raw는 scores parquet의 구성 컬럼으로 재구성 (재구성 오차 0 검증 후 실행).
정규화는 파이프라인과 동일하게 후보 집합 내 percentile rank.

가정 의존 플래그 (기준 순위 ≤ FLAG_RANK_MAX 후보만 평가):
- top30_freq ∈ [0.2, 0.8]   → Top30 진입 여부가 가중치에 따라 갈림
- or rank_p95 − rank_p5 ≥ 20 → 순위 변동 폭 큼

실행:
    python -m scripts.sensitivity_mc --date 2026-10-12 --n 2000
출력:
    data/scored/sensitivity_{date}.parquet
"""
from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

from scoring.normalize import percentile_rank
from config.constants import (
    DATA_SCORED,
    W_COMP_DENSITY,
    W_COMP_RADIUS,
    W_COMP_STATION,
    W_COMP_SUBCLUSTER,
    W_COMPETITION,
    W_POPULATION,
)

SPREAD = 0.30
TOP_N = 30
FLAG_RANK_MAX = 60
FLAG_FREQ_BAND = (0.2, 0.8)
FLAG_RANGE = 20


def _pct_rank(a: np.ndarray) -> np.ndarray:
    """파이프라인과 동일한 정규화 (scoring.normalize.percentile_rank, 동점 평균)."""
    return percentile_rank(pd.Series(a)).to_numpy()


def c_raw_from(s: pd.DataFrame, wd: float, wr: float, ws: float) -> np.ndarray:
    return (
        wd * s["density_per_10k"].fillna(0).to_numpy()
        + wr * s["n_doctors_within_radius_med"].fillna(0).to_numpy()
        + ws * s["n_doctors_station_500m_med"].fillna(0).to_numpy()
        + W_COMP_SUBCLUSTER * s["n_doctors_subcluster_max_med"].fillna(0).to_numpy()
    )


def run(date_str: str, n: int, seed: int = 42) -> pd.DataFrame:
    s = pd.read_parquet(DATA_SCORED / f"scores_{date_str}.parquet").reset_index(drop=True)
    recon = c_raw_from(s, W_COMP_DENSITY, W_COMP_RADIUS, W_COMP_STATION)
    err = float(np.abs(recon - s["c_raw"].to_numpy()).max())
    if err > 1e-6:
        raise RuntimeError(f"c_raw 재구성 불일치 (max err {err}) — 산식 변경 확인")

    p_norm = _pct_rank(s["p_raw"].to_numpy())
    base_score = W_COMPETITION * (1 - _pct_rank(recon)) + W_POPULATION * p_norm
    if not np.allclose(base_score, s["score"].to_numpy()):
        raise RuntimeError("기본 가중치 점수가 파이프라인 score와 불일치")
    rng = np.random.default_rng(seed)
    ranks = np.empty((n, len(s)), dtype=np.int32)
    for k in range(n):
        m = rng.uniform(1 - SPREAD, 1 + SPREAD, size=5)
        wc, wp = W_COMPETITION * m[0], W_POPULATION * m[1]
        wc, wp = wc / (wc + wp), wp / (wc + wp)
        c_norm = 1 - _pct_rank(c_raw_from(s, W_COMP_DENSITY * m[2], W_COMP_RADIUS * m[3], W_COMP_STATION * m[4]))
        score = wc * c_norm + wp * p_norm
        order = np.argsort(-score, kind="stable")
        ranks[k, order] = np.arange(1, len(s) + 1)

    out = s[["adm_cd", "sido", "sgg", "adm_nm", "rank"]].rename(columns={"rank": "rank_base"}).copy()
    out["rank_median"] = np.median(ranks, axis=0)
    out["rank_p5"] = np.percentile(ranks, 5, axis=0)
    out["rank_p95"] = np.percentile(ranks, 95, axis=0)
    out["rank_range_90"] = out["rank_p95"] - out["rank_p5"]
    out["top30_freq"] = (ranks <= TOP_N).mean(axis=0)
    eligible = out["rank_base"] <= FLAG_RANK_MAX
    out["assumption_dependent"] = eligible & (
        out["top30_freq"].between(*FLAG_FREQ_BAND) | (out["rank_range_90"] >= FLAG_RANGE)
    )
    out["n_sim"] = n
    out = out.sort_values("rank_median").reset_index(drop=True)
    out.to_parquet(DATA_SCORED / f"sensitivity_{date_str}.parquet", index=False)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="가중치 ±30% 몬테카를로 민감도")
    parser.add_argument("--date", required=True)
    parser.add_argument("--n", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    out = run(args.date, args.n, args.seed)

    stable = out[(out["top30_freq"] >= 0.95)]
    print(f"시뮬레이션 {args.n}회, 후보 {len(out)}동")
    print(f"Top30 안정 (≥95% 진입): {len(stable)}동 | 가정 의존: {int(out['assumption_dependent'].sum())}동")
    cols = ["rank_base", "adm_nm", "rank_median", "rank_p5", "rank_p95", "top30_freq", "assumption_dependent"]
    print(out[out["rank_base"] <= 40].sort_values("rank_base")[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

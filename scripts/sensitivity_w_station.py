"""W_COMP_STATION sensitivity 분석.

저장된 scores_YYYY-MM-DD.parquet 의 c_raw 입력 컬럼을 사용해
W_COMP_STATION 만 0.0 ~ 0.5 로 흔들고 Top30 변화를 비교한다.

전체 파이프라인 재실행 없이 c_raw 재구성 → c_norm 재계산 → score 재계산만 수행.

사용:
    python -m scripts.sensitivity_w_station [--date 2026-04-22]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from config.constants import (
    DATA_SCORED,
    W_COMMUTE,
    W_COMP_DENSITY,
    W_COMP_RADIUS,
    W_COMP_SUBCLUSTER,
    W_COMPETITION,
    W_POPULATION,
)
from scoring.normalize import percentile_rank_inverted


W_GRID = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
BASELINE_W = 0.2  # 현재 운영값


def recompute(df: pd.DataFrame, w_station: float) -> pd.DataFrame:
    """주어진 W_COMP_STATION 값으로 c_raw → c_norm → score 재계산."""
    out = df.copy()
    out["c_raw_new"] = (
        W_COMP_DENSITY * out["density_per_10k_med"].fillna(0)
        + W_COMP_RADIUS * out["n_doctors_within_radius_med"].fillna(0)
        + w_station * out["n_doctors_station_500m_med"].fillna(0)
        + W_COMP_SUBCLUSTER * out["n_doctors_subcluster_max_med"].fillna(0)
    )
    out["c_norm_new"] = percentile_rank_inverted(out["c_raw_new"])
    out["score_new"] = (
        W_COMPETITION * out["c_norm_new"]
        + W_POPULATION * out["p_norm"]
        + W_COMMUTE * out["t_norm"]
    )
    out = out.sort_values("score_new", ascending=False).reset_index(drop=True)
    out["rank_new"] = out.index + 1
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2026-04-22")
    parser.add_argument("--n", type=int, default=30, help="Top N (default 30)")
    args = parser.parse_args()

    src = DATA_SCORED / f"scores_{args.date}.parquet"
    if not src.exists():
        raise FileNotFoundError(src)
    df = pd.read_parquet(src)
    print(f"loaded {src.name} — {len(df)} dongs\n")

    # 각 W값별 Top N adm_cd 와 (rank, score) 매핑 저장
    topN_by_w: dict[float, pd.DataFrame] = {}
    rank_by_w: dict[float, pd.Series] = {}  # adm_cd → rank
    for w in W_GRID:
        scored = recompute(df, w)
        topN = scored.head(args.n)[
            ["rank_new", "adm_cd", "sido", "sgg", "adm_nm",
             "score_new", "c_norm_new", "p_norm", "t_norm",
             "n_doctors_station_500m_med", "n_doctors_med", "n_doctors_within_radius_med"]
        ].copy()
        topN_by_w[w] = topN
        rank_by_w[w] = scored.set_index("adm_cd")["rank_new"]

    base_set = set(topN_by_w[BASELINE_W]["adm_cd"])

    # ── 1. baseline(W=0.2) 대비 시나리오별 in/out ──
    print("=" * 76)
    print(f"[1] Top{args.n} 멤버십 변화 (baseline = W={BASELINE_W})")
    print("=" * 76)
    print(f"{'W':>5} | {'in_top':>6} | {'overlap':>7} | {'newly_in':>9} | {'dropped_out':>11}")
    print("-" * 76)
    for w in W_GRID:
        cur = set(topN_by_w[w]["adm_cd"])
        overlap = len(cur & base_set)
        newly_in = cur - base_set
        dropped = base_set - cur
        marker = "  ← 현재" if w == BASELINE_W else ""
        print(f"{w:>5.2f} | {len(cur):>6} | {overlap:>7} | {len(newly_in):>9} | {len(dropped):>11}{marker}")
    print()

    # ── 2. Stable core: 모든 W에서 Top N에 들어있는 adm_cd ──
    sets = [set(topN_by_w[w]["adm_cd"]) for w in W_GRID]
    stable = set.intersection(*sets)
    print("=" * 76)
    print(f"[2] Stable core — 모든 W ({W_GRID[0]}~{W_GRID[-1]})에서 Top{args.n} 유지")
    print("=" * 76)
    print(f"안정 코어 동 수: {len(stable)} / {args.n}\n")
    if stable:
        # 표시용으로 baseline 기준 정렬
        base_top = topN_by_w[BASELINE_W]
        stable_df = base_top[base_top["adm_cd"].isin(stable)][
            ["rank_new", "sido", "sgg", "adm_nm", "score_new",
             "n_doctors_station_500m_med"]
        ].rename(columns={"rank_new": "rank@W=0.2", "score_new": "score@W=0.2",
                          "n_doctors_station_500m_med": "역500m_의사"})
        print(stable_df.to_string(index=False))
    print()

    # ── 3. Sensitive: W에 따라 들락날락하는 동 (어떤 W에선 in, 다른 W에선 out) ──
    union = set.union(*sets)
    sensitive = union - stable
    print("=" * 76)
    print(f"[3] Sensitive — W값에 따라 Top{args.n} 들락날락 ({len(sensitive)}개 동)")
    print("=" * 76)
    if sensitive:
        rows = []
        for adm_cd in sensitive:
            row = {"adm_nm": None, "sgg": None,
                   "역500m_의사": None}
            in_w = []
            for w in W_GRID:
                in_top = adm_cd in topN_by_w[w]["adm_cd"].values
                in_w.append("●" if in_top else "·")
                if in_top and row["adm_nm"] is None:
                    r = topN_by_w[w][topN_by_w[w]["adm_cd"] == adm_cd].iloc[0]
                    row["adm_nm"] = r["adm_nm"]
                    row["sgg"] = r["sgg"]
                    row["역500m_의사"] = int(r["n_doctors_station_500m_med"])
            row["멤버십"] = "".join(in_w)
            row["adm_cd"] = adm_cd
            # 전체 그리드에서 평균 rank (Top안에 못 들면 그 W의 rank 그대로)
            ranks = [int(rank_by_w[w].loc[adm_cd]) for w in W_GRID]
            row["rank_min"] = min(ranks)
            row["rank_max"] = max(ranks)
            rows.append(row)
        sens_df = pd.DataFrame(rows).sort_values("rank_min")
        # 컬럼 순서
        sens_df = sens_df[["sgg", "adm_nm", f"멤버십({'/'.join(str(w) for w in W_GRID)})".replace("멤버십(0.0/0.1/0.2/0.3/0.4/0.5)", "멤버십"),
                           "역500m_의사", "rank_min", "rank_max"]] if "멤버십" in sens_df.columns else sens_df
        # 헤더 라벨 명시
        sens_df = sens_df.rename(columns={"멤버십": f"in_top@{','.join(str(w) for w in W_GRID)}"})
        print(sens_df.to_string(index=False))
    print()

    # ── 4. Rank shift: 극단 W=0.0 vs W=0.5 비교 (전체 동 기준) ──
    rank_lo = rank_by_w[0.0]
    rank_hi = rank_by_w[0.5]
    rank_base = rank_by_w[BASELINE_W]
    shift_extreme = (rank_hi - rank_lo).abs()
    shift_from_base_lo = (rank_base - rank_lo).abs()
    shift_from_base_hi = (rank_base - rank_hi).abs()
    print("=" * 76)
    print("[4] 전체 동 rank shift 분포 (절대값)")
    print("=" * 76)
    print(f"  W=0.0 ↔ W=0.5  : median {shift_extreme.median():.0f}, p90 {shift_extreme.quantile(0.9):.0f}, max {shift_extreme.max():.0f}")
    print(f"  W=0.2 ↔ W=0.0  : median {shift_from_base_lo.median():.0f}, p90 {shift_from_base_lo.quantile(0.9):.0f}, max {shift_from_base_lo.max():.0f}")
    print(f"  W=0.2 ↔ W=0.5  : median {shift_from_base_hi.median():.0f}, p90 {shift_from_base_hi.quantile(0.9):.0f}, max {shift_from_base_hi.max():.0f}")
    print()

    # Top30 안에서만 본 rank shift
    base_top_set = set(topN_by_w[BASELINE_W]["adm_cd"])
    shift_in_base_top = (rank_base.loc[list(base_top_set)] - rank_hi.loc[list(base_top_set)]).abs()
    print(f"  Top{args.n}@W=0.2 동들이 W=0.5로 갔을 때 rank shift:")
    print(f"    median {shift_in_base_top.median():.0f}, p90 {shift_in_base_top.quantile(0.9):.0f}, max {shift_in_base_top.max():.0f}")
    print()

    # ── 5. baseline Top30의 W값별 rank 변화 (어떤 동이 W에 민감한지) ──
    print("=" * 76)
    print(f"[5] Baseline(W=0.2) Top{args.n} 동들의 W값별 rank")
    print("=" * 76)
    base_top = topN_by_w[BASELINE_W][["rank_new", "sgg", "adm_nm", "adm_cd",
                                        "n_doctors_station_500m_med"]].copy()
    base_top.columns = ["rank@0.2", "sgg", "adm_nm", "adm_cd", "역500m_의사"]
    for w in W_GRID:
        base_top[f"r@{w}"] = [int(rank_by_w[w].loc[c]) for c in base_top["adm_cd"]]
    base_top["max-min"] = base_top[[f"r@{w}" for w in W_GRID]].max(axis=1) - base_top[[f"r@{w}" for w in W_GRID]].min(axis=1)
    cols = ["rank@0.2", "sgg", "adm_nm", "역500m_의사"] + [f"r@{w}" for w in W_GRID] + ["max-min"]
    print(base_top[cols].to_string(index=False))
    print()

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

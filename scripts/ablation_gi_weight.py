"""ABLATION: W_GI_MULTIPLIER ∈ {1.0, 1.5, 2.0, 2.5} Top30 비교.

각 가중치로 pipeline.run을 호출하여 Top30 변동을 비교.
GI 의원(위·대장 내시경 둘 다 보유) 의사수에 W배 적용 시 score 변동 분석.

기준 (W=1.0): GI 가중 없음 - 기존 c_raw와 동일.
실험: W=1.5, 2.0, 2.5 - GI 의사수가 일반 내과의 1.5~2.5배 가중.

산출:
- 각 W의 Top30 set, 신규 진입/탈락 동, score 변동
- 강남 삼성동·대치동(내과 多·GI 多) 같은 검증 동의 rank 변화

사용:
    python -m scripts.ablation_gi_weight [--date 2026-04-27]

주의: 매 실행 시 data/scored/scores_<date>.parquet이 W 마지막 값으로 덮어써짐.
스크립트 종료 시 W=W_GI_MULTIPLIER(기본값)로 다시 실행해서 정상 상태 복원.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date

import pandas as pd

from config.constants import W_GI_MULTIPLIER
from scoring.pipeline import run as pipeline_run

WEIGHTS = [1.0, 1.5, 2.0, 2.5]

# 검증 동: 강남 GI 의원 多 vs 노원 GI 의원 적음 (대비)
VERIFICATION_DONGS = ["역삼1동", "역삼2동", "삼성1동", "대치1동",
                      "월계2동", "중계1동", "삼선동"]


def main() -> int:
    import io
    parser = argparse.ArgumentParser(
        description="W_GI_MULTIPLIER ablation",
    )
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--out", type=str, default=None,
                        help="결과 텍스트 출력 파일 (utf-8). 미지정 시 stdout만.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(message)s")

    buf = io.StringIO()
    def emit(line: str = "") -> None:
        buf.write(line + "\n")
        try:
            print(line)
        except UnicodeEncodeError:
            pass   # cp949 콘솔에서 한글 깨지면 stdout 스킵, 파일만 사용

    emit(f"\n{'='*90}")
    emit(f"[ABLATION] W_GI_MULTIPLIER in {WEIGHTS} (date={args.date})")
    emit(f"{'='*90}\n")

    results: dict[float, pd.DataFrame] = {}
    for w in WEIGHTS:
        print(f"  pipeline.run(W_GI={w}) ...", flush=True)
        scores_path, _ = pipeline_run(args.date, gi_multiplier=w)
        df = pd.read_parquet(scores_path).copy()
        df["adm_cd"] = df["adm_cd"].astype(str)
        results[w] = df

    # 정상 상태 복원: 기본 W로 한 번 더 실행
    if W_GI_MULTIPLIER not in WEIGHTS:
        print(f"  [복원] pipeline.run(W_GI={W_GI_MULTIPLIER}) ...", flush=True)
        pipeline_run(args.date, gi_multiplier=W_GI_MULTIPLIER)

    base_w = 1.0
    base = results[base_w].set_index("adm_cd")
    base_top30_set = set(base.nsmallest(30, "rank").index)

    # ── Top30 변동 요약 ──
    emit(f"\n{'='*90}")
    emit(f"[Top30 변동] W=1.0 대비")
    emit(f"{'='*90}")
    emit(f"  {'W':>5s} | {'overlap':>8s} | {'newly_in':>9s} | {'dropped':>8s} | {'top1':<25s}")
    for w in WEIGHTS:
        df = results[w]
        top30 = set(df.nsmallest(30, "rank")["adm_cd"])
        overlap = len(base_top30_set & top30)
        newly = len(top30 - base_top30_set)
        dropped = len(base_top30_set - top30)
        top1 = df[df["rank"] == 1].iloc[0]
        top1_label = f"{top1['sgg']} {top1['adm_nm']}"
        emit(f"  {w:>5.1f} | {overlap:>8d} | {newly:>9d} | {dropped:>8d} | {top1_label:<25s}")

    # ── 검증 동들의 W별 rank ──
    emit(f"\n{'='*90}")
    emit(f"[검증 동 rank 변동] W가 클수록 GI 多 동은 rank 떨어짐 (페널티 강화)")
    emit(f"{'='*90}")
    header = f"  {'sgg/adm_nm':<28s}" + " ".join(f"{f'W={w}':>7s}" for w in WEIGHTS)
    emit(header)
    for kw in VERIFICATION_DONGS:
        for adm_cd, row in base.iterrows():
            if kw in row["adm_nm"]:
                ranks = []
                for w in WEIGHTS:
                    r = results[w].set_index("adm_cd").loc[adm_cd, "rank"]
                    ranks.append(int(r))
                # 추가 정보: n_doctors_med, n_doctors_med_weighted (W=2.0 기준)
                base_w_df = results[max(WEIGHTS)].set_index("adm_cd").loc[adm_cd]
                n_med = int(base_w_df.get("n_doctors_med", 0))
                n_med_w = int(base_w_df.get("n_doctors_med_weighted", 0))
                ratio = (n_med_w / n_med) if n_med else 1.0
                line = f"  {row['sgg']:<8s} {row['adm_nm']:<18s}" + " ".join(f"{r:>7d}" for r in ranks)
                line += f"   |  의사 {n_med} -> {n_med_w} (x{ratio:.2f})"
                emit(line)

    # ── W별 신규 진입/탈락 동 (W=2.0 기준 상세) ──
    pivot_w = 2.0
    if pivot_w in results:
        df = results[pivot_w]
        top30 = df.nsmallest(30, "rank")
        top30_set = set(top30["adm_cd"])
        newly_in = top30_set - base_top30_set
        dropped = base_top30_set - top30_set

        if newly_in:
            emit(f"\n{'='*90}")
            emit(f"[W=2.0에서 새로 Top30 진입 - {len(newly_in)}개]")
            emit(f"{'='*90}")
            rows = df[df["adm_cd"].isin(newly_in)].sort_values("rank")
            base_rank = base["rank"]
            for _, r in rows.iterrows():
                old_r = int(base_rank.loc[r["adm_cd"]])
                emit(f"  W=1.0 rank {old_r:>3d} -> W=2.0 rank {int(r['rank']):>3d}  "
                     f"{r['sgg']} {r['adm_nm']}  (score {r['score']:.3f})")

        if dropped:
            emit(f"\n{'='*90}")
            emit(f"[W=2.0에서 Top30 탈락 - {len(dropped)}개]")
            emit(f"{'='*90}")
            rows = df[df["adm_cd"].isin(dropped)].sort_values("rank")
            base_rank = base["rank"]
            for _, r in rows.iterrows():
                old_r = int(base_rank.loc[r["adm_cd"]])
                emit(f"  W=1.0 rank {old_r:>3d} -> W=2.0 rank {int(r['rank']):>3d}  "
                     f"{r['sgg']} {r['adm_nm']}  (score {r['score']:.3f})")

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(buf.getvalue())

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())

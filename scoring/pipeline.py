"""End-to-end 스코어링 오케스트레이션.

입력:
- data/cleaned/admin_centroid.parquet       (653 행정동)
- data/cleaned/clinics_by_dong.parquet      (HIRA 공간조인)
- data/raw/population/kosis_pop_*.parquet   (최신)
- data/cache/odsay_commute.parquet          (ODSay 캐시)

출력:
- data/scored/scores_YYYY-MM-DD.parquet     (전체 동, 정규화·점수·순위 포함)
- data/scored/top30_YYYY-MM-DD.parquet      (상위 30)

실행:
    python -m scoring.pipeline [--date YYYY-MM-DD]

중간 데이터가 없으면 명확한 에러 메시지로 어느 스크래퍼를 돌려야 하는지 안내한다.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from config.constants import DATA_CLEANED, DATA_SCORED
from scoring.commute import load_commute, merge_commute
from scoring.competition import (
    compute_competition_raw,
    count_clinics_per_dong,
    count_clinics_within_radius,
)
from scoring.population import load_kosis_population, merge_population
from scoring.weighted_sum import compute_final_scores, top_n

logger = logging.getLogger(__name__)


def _require(path: Path, how: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{path} 없음. 먼저: {how}")


def run(date_str: str) -> tuple[Path, Path]:
    DATA_SCORED.mkdir(parents=True, exist_ok=True)

    centroid_path = DATA_CLEANED / "admin_centroid.parquet"
    clinics_path = DATA_CLEANED / "clinics_by_dong.parquet"
    _require(centroid_path,
             "python -m scoring.spatial_join centroid")
    _require(clinics_path,
             "python -m scrapers.hira_clinic && python -m scoring.spatial_join join-clinics --clinics data/raw/hira/<최신>.parquet")

    admin_centroid = pd.read_parquet(centroid_path)
    # 인구 가중 중심점 overlay (있으면) — centroid_mismatch 보정
    from scoring.spatial_join import apply_pop_weighted_centroid
    admin_centroid = apply_pop_weighted_centroid(admin_centroid)
    clinics_by_dong = pd.read_parquet(clinics_path)
    logger.info("load: %d dongs, %d clinics", len(admin_centroid), len(clinics_by_dong))

    # 1. 경쟁 지표
    logger.info("=== 1. competition ===")
    n_by_dong = count_clinics_per_dong(clinics_by_dong)
    within = count_clinics_within_radius(clinics_by_dong, admin_centroid)
    # 의원 0개 동도 포함시키기 위해 admin_centroid 기준으로 outer merge
    base = admin_centroid[["adm_cd", "adm_cd10", "sido", "sgg", "adm_nm"]].merge(
        n_by_dong[["adm_cd", "n_clinic", "n_clinic_gi"]],
        on="adm_cd", how="left",
    )
    base[["n_clinic", "n_clinic_gi"]] = (
        base[["n_clinic", "n_clinic_gi"]].fillna(0).astype(int)
    )

    # 2. 인구 로드 → 제외 필터 (MIN_POPULATION)
    # merge_population이 P_raw=pop_40plus (or pop_total 폴백) 설정까지 수행
    logger.info("=== 2. population ===")
    pop_raw = load_kosis_population()
    base = merge_population(base, pop_raw)
    logger.info("after population filter: %d dongs", len(base))

    # 3. 경쟁 점수 (인구 반영)
    pop_for_comp = base[["adm_cd", "p_raw"]].rename(columns={"p_raw": "population"})
    comp = compute_competition_raw(base, within, population=pop_for_comp)
    base = base.merge(
        comp[["adm_cd", "n_within_radius", "density_per_10k", "c_raw"]],
        on="adm_cd", how="left",
    )

    # 4. 통근 지표 (점수는 자차 primary, 대중교통은 보조 display)
    logger.info("=== 3. commute ===")
    commute = load_commute()
    base = merge_commute(base, commute)  # adds t_raw (자차)
    # ODSay 대중교통 시간 보조 컬럼
    from scoring.commute import load_transit_supplement
    transit = load_transit_supplement()
    if transit is not None:
        base["adm_cd"] = base["adm_cd"].astype(str)
        before = base["adm_cd"].isin(transit["adm_cd"]).sum()
        base = base.merge(transit, on="adm_cd", how="left")
        logger.info("  + t_transit merged (%d matched)", before)

    # 5. 역세권 메타 지표 (캐시 있으면만)
    try:
        from scoring.station_metrics import compute_for_dongs as station_metrics, STATION_CACHE
        if STATION_CACHE.exists():
            logger.info("=== 3b. station metrics ===")
            st = station_metrics(admin_centroid, clinics_by_dong)
            base = base.merge(st, on="adm_cd", how="left")
    except FileNotFoundError:
        logger.info("역 캐시 없음 → 스킵")

    # 6. 확장 반경 지표 + 해석 플래그
    logger.info("=== 3c. radius coverage ===")
    from scoring.radius_metrics import compute_radius_counts, add_coverage_flags
    radius_df = compute_radius_counts(admin_centroid, clinics_by_dong)
    base = base.merge(radius_df, on="adm_cd", how="left")
    base = add_coverage_flags(base)
    n_desert = int(base["med_desert_flag"].sum())
    n_mismatch = int(base["centroid_mismatch_flag"].sum())
    n_suburban = int(base["suburban_cluster_flag"].sum())
    logger.info(
        "flags: desert=%d, centroid_mismatch=%d, suburban_cluster=%d",
        n_desert, n_mismatch, n_suburban,
    )

    # 6. 최종 가중합
    logger.info("=== 4. weighted sum ===")
    scored = compute_final_scores(base)

    # 저장
    cols_ordered = [
        "rank", "adm_cd", "sido", "sgg", "adm_nm",
        "score", "c_norm", "p_norm", "t_norm",
        "c_raw", "p_raw", "t_raw", "t_transit",
        "pop_total", "pop_40plus", "ratio_40plus",
        "n_clinic", "n_clinic_gi", "n_within_radius", "density_per_10k",
        "n_clinic_500m", "n_clinic_1km", "n_clinic_2km",
        "med_desert_flag", "centroid_mismatch_flag", "suburban_cluster_flag",
        "nearest_station", "station_dist_m", "n_clinic_station_500m",
    ]
    cols_ordered = [c for c in cols_ordered if c in scored.columns]
    scored = scored[cols_ordered + [c for c in scored.columns if c not in cols_ordered]]

    scores_path = DATA_SCORED / f"scores_{date_str}.parquet"
    top30_path = DATA_SCORED / f"top30_{date_str}.parquet"
    scored.to_parquet(scores_path, index=False)
    top_n(scored, 30).to_parquet(top30_path, index=False)
    logger.info("saved %s (%d rows) + %s (30 rows)", scores_path, len(scored), top30_path)

    # 요약 출력
    print()
    print(f"=== Top 10 @ {date_str} ===")
    print(scored.head(10)[
        ["rank", "sido", "sgg", "adm_nm", "score",
         "c_norm", "p_norm", "t_norm", "n_clinic", "p_raw", "t_raw"]
    ].to_string(index=False))

    return scores_path, top30_path


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="개원 입지 스코어링 파이프라인")
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    run(args.date)
    return 0


if __name__ == "__main__":
    sys.exit(main())

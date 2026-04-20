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
    compute_subcluster_max_doctors,
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
    # 전체 의원: n_clinic, n_within_radius_all — display·플래그용
    # 내과 의원: n_clinic_med (의원 수), n_doctors_med (의사 수 합) — 점수 input
    # 가정: 1차 의료기관 내과 전문의 90%+ = 소화기. 키워드만으로 충분.
    INTERNAL_KW = "내과"
    logger.info("=== 1. competition (점수=내과 의사 수, 표시=전체 의원) ===")
    n_by_dong = count_clinics_per_dong(clinics_by_dong)
    within = count_clinics_within_radius(clinics_by_dong, admin_centroid)
    n_by_dong_med = count_clinics_per_dong(
        clinics_by_dong, internal_keyword=INTERNAL_KW, sum_doctors=True
    )
    within_med = count_clinics_within_radius(
        clinics_by_dong, admin_centroid, internal_keyword=INTERNAL_KW, sum_doctors=True
    )
    n_total_clinics = len(clinics_by_dong)
    n_med_clinics = clinics_by_dong["yadmNm"].str.contains(INTERNAL_KW, na=False).sum()
    n_med_doctors = pd.to_numeric(
        clinics_by_dong[clinics_by_dong["yadmNm"].str.contains(INTERNAL_KW, na=False)]["drTotCnt"],
        errors="coerce",
    ).fillna(0).astype(int).sum()
    logger.info(
        "내과 의원 %d개 / 의사 %d명 (평균 %.2f명/의원) — 의사 수 가중 c_raw",
        n_med_clinics, n_med_doctors, n_med_doctors / max(n_med_clinics, 1),
    )

    # 의원 0개 동도 포함시키기 위해 admin_centroid 기준으로 outer merge
    base_cols = ["adm_cd", "adm_cd10", "sido", "sgg", "adm_nm"]
    catchment_cols = [c for c in admin_centroid.columns if c.startswith("catchment_pop_")]
    base_cols += catchment_cols
    base = admin_centroid[base_cols].merge(
        n_by_dong[["adm_cd", "n_clinic", "n_clinic_gi"]],
        on="adm_cd", how="left",
    )
    # 내과 카운트 + 의사 수 머지
    base = base.merge(
        n_by_dong_med[["adm_cd", "n_clinic", "n_doctors"]].rename(
            columns={"n_clinic": "n_clinic_med", "n_doctors": "n_doctors_med"}
        ),
        on="adm_cd", how="left",
    )
    int_cols = ["n_clinic", "n_clinic_gi", "n_clinic_med", "n_doctors_med"]
    base[int_cols] = base[int_cols].fillna(0).astype(int)

    # 2. 인구 로드 → 제외 필터 (MIN_POPULATION)
    # merge_population이 P_raw 설정까지 수행 (catchment × ratio_40plus 우선)
    logger.info("=== 2. population ===")
    pop_raw = load_kosis_population()
    base = merge_population(base, pop_raw)
    logger.info("after population filter: %d dongs", len(base))

    # 2b. 역세권 메타 — c_raw에 페널티로 들어가므로 미리 계산
    # (캐시 없으면 station_penalty=None → 페널티 항 0)
    station_meta = None
    try:
        from scoring.station_metrics import compute_for_dongs as station_metrics, STATION_CACHE
        if STATION_CACHE.exists():
            logger.info("=== 2b. station metrics (c_raw 페널티 입력) ===")
            station_meta = station_metrics(admin_centroid, clinics_by_dong)
    except FileNotFoundError:
        logger.info("역 캐시 없음 → station 페널티 0")

    # 2c. subcluster max — 동 내 가장 밀집된 500m disk 내과 의사 수
    # 컬럼은 항상 출력. c_raw 영향은 W_COMP_SUBCLUSTER 활성화 후 (기본 0).
    logger.info("=== 2c. subcluster max density (c_raw 페널티 입력, W=0이면 영향 없음) ===")
    subcluster_meta = compute_subcluster_max_doctors(admin_centroid, clinics_by_dong)
    logger.info(
        "subcluster max 의사 수: median %.0f, p90 %.0f, max %.0f",
        subcluster_meta["n_doctors_subcluster_max_med"].median(),
        subcluster_meta["n_doctors_subcluster_max_med"].quantile(0.9),
        subcluster_meta["n_doctors_subcluster_max_med"].max(),
    )

    # 3. 경쟁 점수 (내과 의사 수 / 1.5km 배후 40+ 인구 + 역세권 페널티)
    # density 분모 우선순위:
    #   1) catchment_pop_40plus (40+ 환자풀 — 내과 진료 베이스)
    #   2) catchment_pop_*km (전체 인구 폴백)
    #   3) p_raw
    if "catchment_pop_40plus" in base.columns and base["catchment_pop_40plus"].notna().any():
        density_col = "catchment_pop_40plus"
        logger.info("density 분모 = catchment_pop_40plus (40+ 환자풀)")
    elif catchment_cols and base[catchment_cols[0]].notna().any():
        density_col = catchment_cols[0]
        logger.info("density 분모 = %s (전체 인구 폴백)", density_col)
    else:
        density_col = "p_raw"
        logger.info("density 분모 = p_raw (catchment 없음)")
    pop_for_comp = base[["adm_cd", density_col]].rename(columns={density_col: "population"})

    # c_raw 입력: 의사 수 가중 (n_doctors_med, n_doctors_within_radius_med)
    base_for_comp = base.copy()
    base_for_comp["n_clinic"] = base_for_comp["n_doctors_med"]
    within_for_comp = within_med[["adm_cd", "n_doctors_within"]].rename(
        columns={"n_doctors_within": "n_within_radius"}
    )
    comp = compute_competition_raw(
        base_for_comp,
        within_for_comp,
        population=pop_for_comp,
        station_penalty=station_meta,         # 캐시 없으면 None → 페널티 0
        subcluster_penalty=subcluster_meta,   # W_COMP_SUBCLUSTER=0이면 영향 없음
    )
    # subcluster meta의 n_clinics 컬럼도 display용으로 받기
    base = base.merge(
        subcluster_meta[["adm_cd", "n_clinics_subcluster_max_med"]],
        on="adm_cd", how="left",
    )
    base = base.merge(
        comp[["adm_cd", "n_within_radius", "density_per_10k", "c_raw",
              "n_doctors_station_500m_med", "n_doctors_subcluster_max_med"]].rename(
            columns={
                "n_within_radius": "n_doctors_within_radius_med",
                "density_per_10k": "density_per_10k_med",
            }
        ),
        on="adm_cd", how="left",
    )
    # 디스플레이용: 내과 의원 카운트 (의사 수가 아닌)
    base = base.merge(
        within_med[["adm_cd", "n_within_radius"]].rename(
            columns={"n_within_radius": "n_within_radius_med"}
        ),
        on="adm_cd", how="left",
    )
    # 전체 의원 기준 within_radius (display)
    base = base.merge(
        within.rename(columns={"n_within_radius": "n_within_radius_all"}),
        on="adm_cd", how="left",
    )
    # 호환성 컬럼: 의사 수 기준
    base["n_within_radius"] = base["n_within_radius_med"]
    base["density_per_10k"] = base["density_per_10k_med"]

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

    # 5. 역세권 메타 컬럼 머지 (위에서 계산한 station_meta — display용)
    if station_meta is not None:
        # n_doctors_station_500m_med는 c_raw 머지로 이미 들어왔으므로 제외
        st_display_cols = [c for c in station_meta.columns
                           if c != "n_doctors_station_500m_med"]
        base = base.merge(station_meta[st_display_cols], on="adm_cd", how="left")

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
        "catchment_pop_1_5km", "catchment_pop_40plus",
        "n_clinic", "n_clinic_gi", "n_clinic_med", "n_doctors_med",
        "n_within_radius", "n_within_radius_med", "n_within_radius_all",
        "n_doctors_within_radius_med", "density_per_10k",
        "n_clinic_500m", "n_clinic_1km", "n_clinic_2km",
        "med_desert_flag", "centroid_mismatch_flag", "suburban_cluster_flag",
        "nearest_station", "station_dist_m", "n_clinic_station_500m",
        "n_doctors_station_500m_med",
        "n_doctors_subcluster_max_med", "n_clinics_subcluster_max_med",
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

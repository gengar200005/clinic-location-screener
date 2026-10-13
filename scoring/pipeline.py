"""End-to-end 스코어링 오케스트레이션.

입력:
- data/cleaned/admin_centroid.parquet       (653 행정동)
- data/cleaned/clinics_by_dong.parquet      (HIRA 공간조인)
- data/raw/population/kosis_pop_*.parquet   (최신)
- data/cache/commute_v7.parquet             (출퇴근 4구간 — 60분 후보 필터)

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

from config.constants import COMMUTE_LIMIT_MIN, DATA_CLEANED, DATA_SCORED, W_GI_MULTIPLIER
from scoring.commute import apply_manual_transit, classify_commute, load_commute_v7
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


def run(date_str: str, gi_multiplier: float = W_GI_MULTIPLIER, strict: bool = False) -> tuple[Path, Path]:
    """strict=True (GH Actions): v7 입력(추세·생존·검진·GI 장비)이 없으면 경고 후 건너뛰지 않고 실패.
    입력이 빠진 점수로 대시보드·Notion을 덮어쓰는 것을 막기 위함."""
    DATA_SCORED.mkdir(parents=True, exist_ok=True)

    centroid_path = DATA_CLEANED / "admin_centroid.parquet"
    clinics_path = DATA_CLEANED / "clinics_by_dong.parquet"
    _require(centroid_path,
             "python -m scoring.spatial_join centroid")
    _require(clinics_path,
             "python -m scrapers.hira_clinic && python -m scoring.spatial_join join-clinics --clinics data/raw/hira/<최신>.parquet")

    admin_centroid = pd.read_parquet(centroid_path)
    # 중심점 overlay — CENTROID_MODE 따라 (shops/pop/geom). ADR-004.
    from scoring.spatial_join import apply_centroid_overlay
    admin_centroid = apply_centroid_overlay(admin_centroid)
    clinics_by_dong = pd.read_parquet(clinics_path)
    if strict and not clinics_by_dong.get("is_gi", pd.Series(dtype=bool)).astype(bool).any():
        raise RuntimeError("is_gi 전부 False — 장비 캐시(data/cache/clinic_equipment.parquet) 확인")
    logger.info("load: %d dongs, %d clinics", len(admin_centroid), len(clinics_by_dong))

    # 1. 경쟁 지표
    # 전체 의원: n_clinic, n_within_radius_all — display·플래그용
    # 내과 의원: n_clinic_med (의원 수), n_doctors_med (의사 수 합) — display + 회귀
    # 내과 의원 가중: n_doctors_med_weighted = n_doctors_med + (W_GI-1)×n_doctors_gi
    #   c_raw 의사수 입력은 weighted 사용. ADR-005.
    INTERNAL_KW = "내과"
    logger.info(
        "=== 1. competition (점수=내과 의사 수 ×W_GI=%.1f, 표시=전체 의원) ===",
        gi_multiplier,
    )
    n_by_dong = count_clinics_per_dong(clinics_by_dong)
    within = count_clinics_within_radius(clinics_by_dong, admin_centroid)
    # 비가중 (display·회귀)
    n_by_dong_med = count_clinics_per_dong(
        clinics_by_dong, internal_keyword=INTERNAL_KW, sum_doctors=True
    )
    within_med = count_clinics_within_radius(
        clinics_by_dong, admin_centroid, internal_keyword=INTERNAL_KW, sum_doctors=True
    )
    # 가중 (c_raw 입력)
    n_by_dong_med_w = count_clinics_per_dong(
        clinics_by_dong, internal_keyword=INTERNAL_KW, sum_doctors=True,
        gi_multiplier=gi_multiplier,
    )
    within_med_w = count_clinics_within_radius(
        clinics_by_dong, admin_centroid, internal_keyword=INTERNAL_KW,
        sum_doctors=True, gi_multiplier=gi_multiplier,
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
    # 내과 카운트 + 의사 수 머지 (비가중)
    base = base.merge(
        n_by_dong_med[["adm_cd", "n_clinic", "n_doctors"]].rename(
            columns={"n_clinic": "n_clinic_med", "n_doctors": "n_doctors_med"}
        ),
        on="adm_cd", how="left",
    )
    # 가중 의사 수 머지 (c_raw 입력 — n_doctors_med_weighted)
    base = base.merge(
        n_by_dong_med_w[["adm_cd", "n_doctors"]].rename(
            columns={"n_doctors": "n_doctors_med_weighted"}
        ),
        on="adm_cd", how="left",
    )
    int_cols = ["n_clinic", "n_clinic_gi", "n_clinic_med",
                "n_doctors_med", "n_doctors_med_weighted"]
    base[int_cols] = base[int_cols].fillna(0).astype(int)

    # 2. 인구 로드 → 제외 필터 (MIN_POPULATION)
    # merge_population이 P_raw 설정까지 수행 (catchment × ratio_5069 우선)
    logger.info("=== 2. population ===")
    pop_raw = load_kosis_population()
    base = merge_population(base, pop_raw)
    logger.info("after population filter: %d dongs", len(base))
    # 2a. 50~69세 3년 추세 (v7, 표시 전용 — 가중치 미정)
    from scoring.population import aggregate_unmatched_trend, compute_trend, load_population_then
    then = load_population_then()
    if then is not None:
        trend = aggregate_unmatched_trend(compute_trend(base, then), base, then)
        base = base.merge(trend, on="adm_cd", how="left")
    else:
        if strict:
            raise FileNotFoundError("과거 인구(kosis_pop_age_at_*) 없음 — `python -m scrapers.population_kosis --at 202309`")
        logger.warning("과거 인구 없음 → 추세 생략 (`fetch_population_age_at`)")

    # 2b. 역세권 메타 — c_raw에 페널티로 들어가므로 미리 계산
    # (캐시 없으면 station_penalty=None → 페널티 항 0)
    station_meta = None
    try:
        from scoring.station_metrics import compute_for_dongs as station_metrics, STATION_CACHE
        if STATION_CACHE.exists():
            logger.info("=== 2b. station metrics (c_raw 페널티 입력, GI 가중 적용) ===")
            station_meta = station_metrics(
                admin_centroid, clinics_by_dong, gi_multiplier=gi_multiplier,
            )
    except FileNotFoundError:
        logger.info("역 캐시 없음 → station 페널티 0")

    # 2c. subcluster max — 동 내 가장 밀집된 500m disk 내과 의사 수
    # 컬럼은 항상 출력. c_raw 영향은 W_COMP_SUBCLUSTER 활성화 후 (기본 0).
    logger.info("=== 2c. subcluster max density (GI 가중 적용) ===")
    subcluster_meta = compute_subcluster_max_doctors(
        admin_centroid, clinics_by_dong, gi_multiplier=gi_multiplier,
    )
    logger.info(
        "subcluster max 의사 수: median %.0f, p90 %.0f, max %.0f",
        subcluster_meta["n_doctors_subcluster_max_med"].median(),
        subcluster_meta["n_doctors_subcluster_max_med"].quantile(0.9),
        subcluster_meta["n_doctors_subcluster_max_med"].max(),
    )

    # 3. 경쟁 점수 (내과 의사 수 / 1.5km 배후 50~69세 인구 + 역세권 페널티)
    # density 분모 우선순위:
    #   1) catchment_pop_5069 (50~69세 환자풀)
    #   2) catchment_pop_*km (전체 인구 폴백)
    #   3) p_raw
    if "catchment_pop_5069" in base.columns and base["catchment_pop_5069"].notna().any():
        density_col = "catchment_pop_5069"
        logger.info("density 분모 = catchment_pop_5069 (50~69세 환자풀)")
    elif catchment_cols and base[catchment_cols[0]].notna().any():
        density_col = catchment_cols[0]
        logger.info("density 분모 = %s (전체 인구 폴백)", density_col)
    else:
        density_col = "p_raw"
        logger.info("density 분모 = p_raw (catchment 없음)")
    pop_for_comp = base[["adm_cd", density_col]].rename(columns={density_col: "population"})

    # c_raw 입력: 의사 수 GI 가중 (n_doctors_med_weighted, within_med_w)
    base_for_comp = base.copy()
    base_for_comp["n_clinic"] = base_for_comp["n_doctors_med_weighted"]
    within_for_comp = within_med_w[["adm_cd", "n_doctors_within"]].rename(
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

    # 4. 통근 60분 필터 (v7) — 점수 미반영, 후보 판정 전용.
    #    percentile rank가 후보 집합 기준이어야 하므로 가중합 전에 필터.
    logger.info("=== 3. commute filter (출퇴근 %d분) ===", COMMUTE_LIMIT_MIN)
    base["adm_cd"] = base["adm_cd"].astype(str)
    base = base.merge(apply_manual_transit(load_commute_v7()), on="adm_cd", how="left")
    base = classify_commute(base, COMMUTE_LIMIT_MIN)
    status_counts = base["commute_status"].value_counts().to_dict()
    logger.info(
        "commute: %s (자차 OK %d / 대중교통 OK %d)",
        status_counts, int(base["commute_ok_car"].sum()), int(base["commute_ok_transit"].sum()),
    )
    excluded = base[~base["commute_ok"]]
    excluded_path = DATA_SCORED / f"excluded_commute_{date_str}.parquet"
    excluded[["adm_cd", "sido", "sgg", "adm_nm", "commute_status",
              "t_car_am", "t_car_pm", "t_transit_am", "t_transit_pm"]].to_parquet(
        excluded_path, index=False,
    )
    n_car = int((excluded["commute_status"] == "exceed_car").sum())
    if n_car:
        logger.info("자차 60분 초과·대중교통 미확인 %d동 → 제외 (수동 확인: data/manual/transit_manual.csv)", n_car)
    if (excluded["commute_status"] == "unknown").any():
        logger.warning(
            "통근 판정불가(자차 경로 없음) %d동 → 후보 제외 (%s).",
            int((excluded["commute_status"] == "unknown").sum()), excluded_path.name,
        )
    base = base[base["commute_ok"]].copy()
    logger.info("후보 확정: %d동", len(base))

    # 5. 역세권 메타 컬럼 머지 (위에서 계산한 station_meta — display용)
    if station_meta is not None:
        # n_doctors_station_500m_med는 c_raw 머지로 이미 들어왔으므로 제외
        st_display_cols = [c for c in station_meta.columns
                           if c != "n_doctors_station_500m_med"]
        base = base.merge(station_meta[st_display_cols], on="adm_cd", how="left")

    # 5b. 생존율·순증 (v7, 표시 전용 — 점수 미반영).
    #     2026-10-12 검증: 최근 개설 내과의 5년 내 폐업률 3~8% → 동 반경 단위 생존율은
    #     표본(평균 개설 3곳)이 너무 작아 변별력 없음. net_open(개설−폐업)은 진입 압력 참고치.
    try:
        from scoring.survival import compute_survival, load_internal_clinic_history
        history, ref = load_internal_clinic_history()
        base = base.merge(compute_survival(admin_centroid, history, ref), on="adm_cd", how="left")
    except FileNotFoundError as e:
        if strict:
            raise
        logger.warning("생존율 생략: %s", e)

    # 5c. 내시경 위암검진 가능 기관 (v7, 표시 전용 — C 반영 비중 사용자 결정 대기)
    try:
        from scoring.checkup_competition import compute_checkup_competition, load_endoscopy_checkup
        base = base.merge(
            compute_checkup_competition(admin_centroid, load_endoscopy_checkup()), on="adm_cd", how="left",
        )
        base["endo_ck_per_10k"] = base["n_endo_ck"] / (base["catchment_pop_5069"] / 10_000)
    except FileNotFoundError as e:
        if strict:
            raise
        logger.warning("검진기관 경쟁 생략: %s", e)

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
        "c_raw", "p_raw", "t_raw", "t_car", "t_transit",
        "t_car_am", "t_car_pm", "t_transit_am", "t_transit_pm",
        "commute_ok_car", "commute_ok_transit",
        "pop_total", "pop_5069", "ratio_5069",
        "catchment_pop_1_5km", "catchment_pop_5069",
        "n_clinic", "n_clinic_gi", "n_clinic_med",
        "n_doctors_med", "n_doctors_med_weighted",
        "n_within_radius", "n_within_radius_med", "n_within_radius_all",
        "n_doctors_within_radius_med", "density_per_10k",
        "n_clinic_500m", "n_clinic_1km", "n_clinic_2km",
        "med_desert_flag", "centroid_mismatch_flag", "suburban_cluster_flag",
        "nearest_station", "station_dist_m", "n_clinic_station_500m",
        "n_doctors_station_500m_med",
        "n_doctors_subcluster_max_med", "n_clinics_subcluster_max_med",
        "n_open_5y", "n_open_5y_alive", "n_close_5y", "net_open_5y",
        "survival_5y", "survival_sample_flag",
        "pop_5069_then", "trend_5069_3y", "trend_flag",
        "n_endo_ck", "n_endo_ck_clinic", "n_endo_ck_hosp", "n_stomach_ck_unknown",
        "n_colon_ck", "endo_ck_per_10k",
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
    parser.add_argument("--gi-multiplier", type=float, default=W_GI_MULTIPLIER,
                        help=f"GI 의원 의사수 가중치 (기본: {W_GI_MULTIPLIER}, 1.0=가중 없음)")
    parser.add_argument("--strict", action="store_true",
                        help="v7 입력 누락 시 실패 (GH Actions 주간 배치용)")
    args = parser.parse_args()
    run(args.date, gi_multiplier=args.gi_multiplier, strict=args.strict)
    return 0


if __name__ == "__main__":
    sys.exit(main())

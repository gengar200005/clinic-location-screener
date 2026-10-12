"""인구 지표 P 계산.

**P_raw = catchment_pop_1_5km × ratio_5069** (기본, catchment 기반)
   · catchment: 중심점 반경 1.5km WorldPop 인구 합 (행정동 경계 무시)
   · ratio_5069: 동 단위 KOSIS 연령 비율 (동 수준 근사)
**P_raw = pop_5069** (폴백, catchment 없을 때)

근거 (v7, 2026-10-12): 타깃 50~69세. 가온내과는 50~70대 위·대장 내시경 검진 중심.
- v6까지는 40+ (만성질환 빈도 급증 연령). 사용자 결정으로 50~69세로 좁힘.
- 국가검진(위·대장내시경) 수요가 50대 이후 집중.

배후세대 보정: 행정동 경계는 인위적. "작은 동 + 인접 대단지" 케이스(행신2동 등)가
동 단위 pop_5069로는 과소평가됨. catchment 1.5km는 실상권 규모로, 동 경계 밖의
배후세대까지 포착. 연령 비율은 동 단위 통계를 그대로 적용 (근사).

데이터:
- 기본(권장): data/raw/population/kosis_pop_age_{date}.parquet — 총인구 + 50~69세 합
             + data/cache/admin_centroid_pop.parquet 의 catchment 컬럼
- 폴백: data/raw/population/kosis_pop_{date}.parquet — 총인구만 (P_raw=총인구)

KOSIS는 10자리 행정구역 코드(adm_cd10)로 제공. admin_centroid의 `adm_cd10` 컬럼이 브리지.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

from config.constants import DATA_RAW, MIN_POPULATION

logger = logging.getLogger(__name__)


def load_kosis_population(parquet_path: Path | None = None) -> pd.DataFrame:
    """KOSIS 수집 결과 → (adm_cd10, pop_total, pop_5069, ratio_5069) 테이블.

    우선순위:
    1. kosis_pop_age_*.parquet — 연령대 포함 (50~69세)
    2. kosis_pop_*.parquet — 총인구만 (폴백, pop_5069=NaN)

    parquet_path 지정 시 해당 파일만 사용.
    """
    pop_dir = DATA_RAW / "population"

    if parquet_path is not None:
        df = pd.read_parquet(parquet_path)
    else:
        age_files = sorted(p for p in pop_dir.glob("kosis_pop_age_*.parquet")
                           if not p.name.startswith("kosis_pop_age_at_"))  # 과거 시점 파일 제외
        if age_files:
            df = pd.read_parquet(age_files[-1])
            logger.info("population source: %s (연령대 포함)", age_files[-1].name)
        else:
            totals = sorted(pop_dir.glob("kosis_pop_*.parquet"))
            totals = [p for p in totals if "age" not in p.name]
            if not totals:
                raise FileNotFoundError(
                    "KOSIS 인구 parquet 없음. "
                    "`python -m scrapers.population_kosis --age` 먼저."
                )
            df = pd.read_parquet(totals[-1])
            df = df.rename(columns={"population": "pop_total"})
            df["pop_5069"] = pd.NA
            df["ratio_5069"] = pd.NA
            logger.warning("50~69세 데이터 없음 — pop_total만 사용. "
                           "`python -m scrapers.population_kosis --age`로 연령대 수집 권장.")

    # 동 이름 (코드 crosswalk용) — 같은 날짜의 총인구 파일에서
    if "dong_nm" not in df.columns and parquet_path is None and age_files:
        total_path = pop_dir / age_files[-1].name.replace("kosis_pop_age_", "kosis_pop_")
        if total_path.exists():
            names = pd.read_parquet(total_path)[["adm_cd10", "dong_nm"]]
            df = df.merge(names, on="adm_cd10", how="left")

    # 표준 컬럼 반환
    cols = ["adm_cd10", "pop_total"]
    if "pop_5069" in df.columns:
        cols += ["pop_5069", "ratio_5069"]
    if "dong_nm" in df.columns:
        cols += ["dong_nm"]
    return df[cols].copy()


def _find_catchment_col(df: pd.DataFrame) -> str | None:
    """dong_table/merged에서 catchment_pop_*km 컬럼 이름 탐색. 없으면 None."""
    for c in df.columns:
        if c.startswith("catchment_pop_") and c.endswith("km"):
            return c
    return None


def _norm_dong_name(s: str) -> str:
    return re.sub(r"[·.\s]", "", str(s))


def crosswalk_codes(dong_table: pd.DataFrame, pop: pd.DataFrame) -> pd.Series:
    """경계 adm_cd10 → KOSIS adm_cd10 매핑 (코드 불일치 동은 같은 시도 안에서 동 이름으로).

    배경 (2026-10-12): 인천 행정체제 개편(2026-07, 중구·동구 → 제물포구·영종구,
    서구 → 서구·검단구)으로 KOSIS(2026-09~)는 새 구 코드, 경계 GeoJSON(ver20260201)은
    옛 코드. 동 이름은 유지되어 이름 매칭이 1:1 (인천 48동 검증).
    이름이 시도 안에서 유일하지 않거나 못 찾으면 원래 코드 유지 → 인구 결측으로 보고.
    """
    codes = dong_table["adm_cd10"].astype(str)
    if "dong_nm" not in pop.columns:
        return codes
    kosis = pop[["adm_cd10", "dong_nm"]].astype(str)
    missing = ~codes.isin(set(kosis["adm_cd10"]))
    if not missing.any():
        return codes
    pool = kosis[~kosis["adm_cd10"].isin(set(codes))].copy()
    pool["sido2"] = pool["adm_cd10"].str[:2]
    pool["key"] = pool["dong_nm"].map(_norm_dong_name)
    pool = pool.drop_duplicates(["sido2", "key"], keep=False)  # 시도 내 동명 중복은 매칭 안 함
    lookup = pool.set_index(["sido2", "key"])["adm_cd10"]

    out = codes.copy()
    names = dong_table["adm_nm"].astype(str).str.split().str[-1].map(_norm_dong_name)
    n_hit = 0
    for idx in codes[missing].index:
        hit = lookup.get((codes[idx][:2], names[idx]))
        if hit is not None:
            out[idx] = hit
            n_hit += 1
    logger.info("인구 코드 crosswalk (동 이름): %d / %d 동 매칭", n_hit, int(missing.sum()))
    return out


def merge_population(
    dong_table: pd.DataFrame,
    pop: pd.DataFrame,
) -> pd.DataFrame:
    """행정동 테이블에 인구 병합 + MIN_POPULATION 필터.

    P_raw 정책 (우선순위):
    1. catchment_pop_*km 컬럼 + ratio_5069 있으면
       → P_raw = catchment_pop × ratio_5069 (배후세대 기반 50~69세 풀)
    2. pop_5069 있으면 → P_raw = pop_5069 (동 단위 폴백)
    3. 그 외 → P_raw = pop_total
    """
    dong_table = dong_table.copy()
    dong_table["adm_cd10_pop"] = crosswalk_codes(dong_table, pop)
    pop = pop.drop(columns=["dong_nm"], errors="ignore").rename(columns={"adm_cd10": "adm_cd10_pop"})
    merged = dong_table.merge(pop, on="adm_cd10_pop", how="left")
    missing = merged["pop_total"].isna()
    if missing.any():
        logger.warning(
            "인구 매칭 실패: %d 동 → 근거 불충분으로 제외: %s",
            int(missing.sum()), merged.loc[missing, "adm_nm"].tolist()[:20],
        )

    # MIN_POPULATION은 동 단위 총인구 기준 (공단·공원 동 배제)
    before = len(merged)
    merged = merged[merged["pop_total"].fillna(0) >= MIN_POPULATION].copy()
    logger.info("population filter (>= %d): %d / %d", MIN_POPULATION, len(merged), before)

    # P_raw 설정
    catchment_col = _find_catchment_col(merged)
    has_catchment = (
        catchment_col is not None
        and merged[catchment_col].notna().any()
    )
    has_ratio = (
        "ratio_5069" in merged.columns
        and merged["ratio_5069"].notna().any()
    )
    has_target_age = (
        "pop_5069" in merged.columns
        and merged["pop_5069"].notna().any()
    )

    if has_catchment and has_ratio:
        catch = merged[catchment_col].astype(float)
        ratio = merged["ratio_5069"].astype(float)
        # catchment 또는 ratio NaN인 행은 동 단위 pop_5069 로 폴백
        p_catchment = catch * ratio
        merged["catchment_pop_5069"] = p_catchment
        if has_target_age:
            merged["p_raw"] = p_catchment.where(
                p_catchment.notna() & (p_catchment > 0),
                merged["pop_5069"].astype(float),
            )
        else:
            merged["p_raw"] = p_catchment.fillna(0.0)
        logger.info(
            "P_raw = %s × ratio_5069 (중앙값=%d, 동 pop_5069 중앙값=%d)",
            catchment_col,
            int(merged["p_raw"].median()),
            int(merged["pop_5069"].median()) if has_target_age else -1,
        )
    elif has_target_age:
        merged["p_raw"] = merged["pop_5069"].astype(float)
        logger.info(
            "P_raw = pop_5069 (catchment 없음 — 동 단위 폴백, 중앙값=%d)",
            int(merged["pop_5069"].median()),
        )
    else:
        merged["p_raw"] = merged["pop_total"].astype(float)
        logger.warning("P_raw = pop_total (50~69세 데이터 없음)")

    return merged


# ─── v7: 50~69세 3년 추세 ───────────────────────────────────────────
TREND_PRD_LAG_MONTHS = 36
TREND_BOUNDARY_SUSPECT = 0.30   # 총인구 |변화| > 30% → 경계 변경(분동·합동) 의심


def load_population_then(prd_de: str | None = None) -> pd.DataFrame | None:
    """kosis_pop_age_at_{YYYYMM}.parquet (가장 최근 파일 또는 지정 월)."""
    pop_dir = DATA_RAW / "population"
    files = sorted(pop_dir.glob(f"kosis_pop_age_at_{prd_de or '*'}.parquet"))
    if not files:
        return None
    return pd.read_parquet(files[-1])


def compute_trend(dong_table: pd.DataFrame, then: pd.DataFrame) -> pd.DataFrame:
    """[adm_cd, pop_5069_then, trend_5069_3y, trend_flag].

    - 과거 코드 매핑은 crosswalk_codes (코드 일치 → 시도 내 동 이름).
    - trend_flag: ok / no_match(분동 등으로 과거 동 없음) / boundary_suspect(총인구 ±30% 초과)
      → aggregate_unmatched_trend에서 rapid_change(경계 불변 실제 급변) / aggregated(권역 합산)로 보완
    - boundary_suspect·no_match 는 trend NaN (근거 불충분).
    """
    then = then.astype({"adm_cd10": str})
    codes = crosswalk_codes(dong_table, then)
    t = then.set_index("adm_cd10")
    out = pd.DataFrame({"adm_cd": dong_table["adm_cd"].astype(str).values})
    out["pop_5069_then"] = codes.map(t["pop_5069"]).values
    pop_total_then = codes.map(t["pop_total"]).values
    now_5069 = dong_table["pop_5069"].astype(float).values
    now_total = dong_table["pop_total"].astype(float).values

    total_change = now_total / pop_total_then - 1
    out["trend_5069_3y"] = now_5069 / out["pop_5069_then"].astype(float) - 1
    out["trend_flag"] = "ok"
    out.loc[out["pop_5069_then"].isna(), "trend_flag"] = "no_match"
    out.loc[(out["trend_flag"] == "ok") & (abs(total_change) > TREND_BOUNDARY_SUSPECT), "trend_flag"] = "boundary_suspect"
    out["_trend_raw"] = out["trend_5069_3y"]   # aggregate 단계의 급변(실제 변화) 판정용, 이후 drop
    out.loc[out["trend_flag"] != "ok", "trend_5069_3y"] = pd.NA
    logger.info(
        "50~69세 3년 추세: %s, 중앙 %+.1f%%",
        out["trend_flag"].value_counts().to_dict(),
        100 * out["trend_5069_3y"].astype(float).median(),
    )
    return out


TREND_BOUNDARY_THEN = "ver20230701"   # KOSIS 과거 시점(2023-09)과 같은 동 체계의 경계


def aggregate_unmatched_trend(trend: pd.DataFrame, base: pd.DataFrame, then: pd.DataFrame) -> pd.DataFrame:
    """no_match / boundary_suspect 동 → 과거 경계 기준 상위 권역 단위 추세로 보완.

    예: 부천은 2023년 광역동(10개) → 2024-01 3구 37동 부활. 현재 동의 대표점이 속한
    과거 동 G를 찾고, G 안에 대표점이 있는 현재 동 전체의 50~69세 합 / G의 과거 값 − 1.
    권역 총인구도 ±30% 넘게 어긋나면(부분 중첩) 그대로 근거 불충분.
    trend_flag = "aggregated" (권역 합산 추세, 동 고유값 아님).
    """
    import geopandas as gpd

    from config.constants import EPSG_WGS84

    target = trend["trend_flag"].isin(["no_match", "boundary_suspect"])
    if not target.any():
        return trend.drop(columns="_trend_raw", errors="ignore")
    bdir = DATA_RAW / "admin_boundary"
    old_path = bdir / f"HangJeongDong_{TREND_BOUNDARY_THEN}.geojson"
    new_files = sorted(p for p in bdir.glob("HangJeongDong_*.geojson") if p != old_path)
    if not old_path.exists() or not new_files:
        logger.warning("과거 경계 %s 없음 → 권역 보완 생략", old_path.name)
        return trend.drop(columns="_trend_raw", errors="ignore")

    old = gpd.read_file(old_path).to_crs(EPSG_WGS84)[["adm_cd2", "geometry"]]
    new = gpd.read_file(new_files[-1]).to_crs(EPSG_WGS84)[["adm_cd", "adm_cd2", "adm_nm", "geometry"]]
    new = new[new["adm_cd2"].astype(str).str[:2].isin(base["adm_cd10"].astype(str).str[:2].unique())]
    pts = new.copy()
    pts["geometry"] = new.geometry.representative_point()
    pts = gpd.sjoin(pts, old.rename(columns={"adm_cd2": "old_cd"}), predicate="within", how="left")

    # 현재 KOSIS 50~69세 (후보 외 동 포함) — 코드 crosswalk
    now = load_kosis_population()
    now_codes = crosswalk_codes(
        pts.rename(columns={"adm_cd2": "adm_cd10"})[["adm_cd10", "adm_nm"]].reset_index(drop=True), now,
    )
    nowt = now.set_index(now["adm_cd10"].astype(str))
    pts = pts.reset_index(drop=True)
    pts["now_5069"] = now_codes.map(nowt["pop_5069"]).values
    pts["now_total"] = now_codes.map(nowt["pop_total"]).values
    agg = pts.groupby("old_cd").agg(now_5069=("now_5069", "sum"), now_total=("now_total", "sum"),
                                    n_now=("adm_cd", "size"), any_missing=("now_5069", lambda s: s.isna().any()))
    t = then.astype({"adm_cd10": str}).set_index("adm_cd10")
    agg["then_5069"] = t["pop_5069"].reindex(agg.index).values
    agg["then_total"] = t["pop_total"].reindex(agg.index).values
    agg["trend"] = agg["now_5069"] / agg["then_5069"] - 1
    ok = (~agg["any_missing"]) & agg["then_5069"].notna() & \
         ((agg["now_total"] / agg["then_total"] - 1).abs() <= TREND_BOUNDARY_SUSPECT)

    old_of = pts.set_index(pts["adm_cd"].astype(str))["old_cd"]
    out = trend.copy()

    # boundary_suspect 중 같은 코드·같은 면적(±10%)이면 경계 불변 → 재건축·입주 등 실제 급변
    from config.constants import EPSG_KOREA
    area_new = new.to_crs(EPSG_KOREA).set_index(new["adm_cd2"].astype(str)).area
    area_old = old.to_crs(EPSG_KOREA).set_index(old["adm_cd2"].astype(str)).area
    cd10 = base.set_index(base["adm_cd"].astype(str))["adm_cd10"].astype(str)
    n_rapid = 0
    for i in out.index[out["trend_flag"] == "boundary_suspect"]:
        c = cd10.get(out.at[i, "adm_cd"])
        if c in area_new.index and c in area_old.index and abs(area_new[c] / area_old[c] - 1) <= 0.10:
            out.at[i, "trend_5069_3y"] = out.at[i, "_trend_raw"]
            out.at[i, "trend_flag"] = "rapid_change"
            n_rapid += 1
    logger.info("추세 급변(경계 불변, 실제 변화): %d 동", n_rapid)
    target = out["trend_flag"].isin(["no_match", "boundary_suspect"])

    n_fixed = 0
    for i in out.index[target]:
        g = old_of.get(out.at[i, "adm_cd"])
        if isinstance(g, str) and g in agg.index and ok.get(g, False):
            out.at[i, "trend_5069_3y"] = agg.at[g, "trend"]
            out.at[i, "trend_flag"] = "aggregated"
            n_fixed += 1
    logger.info("추세 권역 보완: %d / %d 동 (과거 경계 %s)", n_fixed, int(target.sum()), TREND_BOUNDARY_THEN)
    return out.drop(columns="_trend_raw")

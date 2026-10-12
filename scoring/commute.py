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


def load_transit_supplement() -> pd.DataFrame | None:
    """ODSay 대중교통 캐시 → (adm_cd, t_transit) 테이블.

    점수 계산에는 사용 안 함 (자차 primary). 상세 표시용 보조 데이터.
    캐시 없으면 None.
    """
    if not ODSAY_CACHE.exists():
        return None
    df = pd.read_parquet(ODSAY_CACHE)
    df["adm_cd"] = df["adm_cd"].astype(str)
    df = df[["adm_cd", "minutes"]].rename(columns={"minutes": "t_transit"})
    return df


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


# ─── v7: 출퇴근 60분 필터 ─────────────────────────────────────────────
# scrapers/commute_isochrone.py 캐시(commute_v7.parquet) 기반.
# 통근은 점수에 반영하지 않고 후보 판정에만 사용 (W_COMMUTE=0).

V7_CACHE = DATA_CACHE / "commute_v7.parquet"
V7_LEGS = ("car_am", "car_pm", "transit_am", "transit_pm")


def load_commute_v7(path: Path | None = None) -> pd.DataFrame:
    """캐시 → 동별 wide 테이블 [adm_cd, t_car_am, t_car_pm, t_transit_am, t_transit_pm].

    status != ok 인 구간은 NaN.
    """
    path = path or V7_CACHE
    if not path.exists():
        raise FileNotFoundError(
            f"{path} 없음. `python -m scrapers.commute_isochrone` 먼저."
        )
    df = pd.read_parquet(path)
    df["adm_cd"] = df["adm_cd"].astype(str)
    df.loc[df["status"] != "ok", "minutes"] = pd.NA
    wide = df.pivot_table(index="adm_cd", columns="leg", values="minutes", aggfunc="last")
    wide = wide.reindex(columns=list(V7_LEGS))
    wide.columns = [f"t_{leg}" for leg in wide.columns]
    return wide.reset_index()


def classify_commute(df: pd.DataFrame, limit_min: int) -> pd.DataFrame:
    """t_* 4구간 → 판정 컬럼 추가.

    - t_car / t_transit       : max(출근, 퇴근). 한 구간이라도 없으면 NaN.
    - commute_ok_car/transit  : 해당 수단 출퇴근 모두 limit 이내
    - commute_ok              : 둘 중 하나라도 OK
    - commute_status          : ok / exceed / unknown(어느 수단도 OK 아니고, 판정 못 한 수단 있음)
    - t_raw                   : min(t_car, t_transit) — 표시·호환용 (점수 미반영)
    """
    out = df.copy()
    for leg in V7_LEGS:
        col = f"t_{leg}"
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["t_car"] = out[["t_car_am", "t_car_pm"]].max(axis=1, skipna=False)
    out["t_transit"] = out[["t_transit_am", "t_transit_pm"]].max(axis=1, skipna=False)
    out["commute_ok_car"] = out["t_car"] <= limit_min
    out["commute_ok_transit"] = out["t_transit"] <= limit_min
    out["commute_ok"] = out["commute_ok_car"] | out["commute_ok_transit"]

    any_unknown = out["t_car"].isna() | out["t_transit"].isna()
    out["commute_status"] = "exceed"
    out.loc[~out["commute_ok"] & any_unknown, "commute_status"] = "unknown"
    out.loc[out["commute_ok"], "commute_status"] = "ok"

    out["t_raw"] = out[["t_car", "t_transit"]].min(axis=1)
    return out

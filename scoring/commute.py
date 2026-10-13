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


MANUAL_TRANSIT = DATA_CACHE.parent / "manual" / "transit_manual.csv"


def apply_manual_transit(wide: pd.DataFrame, path: Path | None = None) -> pd.DataFrame:
    """사용자가 지도 앱으로 직접 확인한 대중교통 시간(분)으로 t_transit_am/pm 덮어쓰기.

    data/manual/transit_manual.csv — 컬럼 adm_cd, transit_am_min(출근 이촌→동), transit_pm_min(퇴근 동→이촌).
    빈 칸은 무시. ODSay는 약관상 분석용 대량 호출 불가라 경계 동은 수동 확인 (2026-10-13).
    """
    path = path or MANUAL_TRANSIT
    if not path.exists():
        return wide
    try:
        m = pd.read_csv(path, dtype={"adm_cd": str}, encoding="utf-8-sig")
    except UnicodeDecodeError:  # 엑셀에서 다시 저장하면 cp949
        m = pd.read_csv(path, dtype={"adm_cd": str}, encoding="cp949")
    out = wide.copy()
    n = 0
    for leg, col in (("transit_am", "transit_am_min"), ("transit_pm", "transit_pm_min")):
        if col not in m.columns:
            continue
        vals = pd.to_numeric(m[col], errors="coerce")
        lookup = dict(zip(m["adm_cd"], vals))
        manual = out["adm_cd"].map(lookup)
        has = manual.notna()
        out.loc[has, f"t_{leg}"] = manual[has]
        n += int(has.sum())
    if n:
        logger.info("대중교통 수동 확인값 반영: %d 구간 (%s)", n, path.name)
    return out


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

    # 판정 상태
    #   ok          : 후보
    #   exceed      : 자차·대중교통 모두 확인, 둘 다 초과
    #   exceed_car  : 자차 초과, 대중교통 미확인 → 제외 (2026-10-13 사용자 결정, 수동 확인 대상)
    #   unknown     : 자차 경로 없음 + 대중교통 미확인 → 제외
    out["commute_status"] = "exceed"
    not_ok = ~out["commute_ok"]
    out.loc[not_ok & out["t_car"].notna() & out["t_transit"].isna(), "commute_status"] = "exceed_car"
    out.loc[not_ok & out["t_car"].isna() & out["t_transit"].isna(), "commute_status"] = "unknown"
    out.loc[out["commute_ok"], "commute_status"] = "ok"

    out["t_raw"] = out[["t_car", "t_transit"]].min(axis=1)
    return out

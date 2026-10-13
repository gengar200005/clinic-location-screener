"""v7 생존율 지표 — 후보 중심점 반경 내 최근 N년 내과 의원 개설·폐업.

데이터: 행정안전부 의원 인허가 (scrapers/mois_clinics.py). HIRA 현황 API에는 폐업 의원이
없어 생존자 편향 보정은 이 데이터로만 가능.

정의 (반경 = COMPETITION_RADIUS_M, 중심 = CENTROID_MODE overlay 좌표):
- 대상 의원: 서울·경기·인천, 사업장명에 "내과" (HIRA c_raw와 같은 식별 기준)
  · "제외/삭제/전출"(관할 이전) 행은 제외 — 개설·폐업 어느 쪽도 아님
  · 폐업 = 상태 "폐업" 또는 "취소/말소/만료/정지/중지", 폐업일 = CLSBIZ_YMD → LCPMT_RTRCN_YMD
- 기간: [기준일 − SURVIVAL_YEARS년, 기준일], 기준일 = 데이터 내 최신 인허가일
- n_open_Ny        : 기간 내 개설
- n_open_Ny_alive  : 그중 현재 영업(휴업 포함)
- survival_Ny      : alive / open — 개설 < SURVIVAL_MIN_OPENED 이면 NaN (표본 부족)
- n_close_Ny       : 기간 내 폐업 (개설 시점 무관)
- net_open_Ny      : 개설 − 폐업

한계: 좌표 결측 의원은 Kakao 주소 지오코딩으로 보충, 그래도 실패하면 카운트 누락
(n_unlocated로 보고). 이름이 "내과"가 아닌 내과 전문의 의원(예: "OO의원")은 미포함.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from config.constants import (
    COMPETITION_RADIUS_M,
    DATA_RAW,
    EPSG_KOREA,
    EPSG_WGS84,
    SURVIVAL_MIN_OPENED,
    SURVIVAL_YEARS,
)
from config.regions import context_sido

logger = logging.getLogger(__name__)

INTERNAL_KW = "내과"
CLOSED_STATUSES = {"폐업", "취소/말소/만료/정지/중지"}
MOVED_STATUS = "제외/삭제/전출"


def load_internal_clinic_history(path=None) -> tuple[pd.DataFrame, pd.Timestamp]:
    """MOIS 원천 → 내과 개설·폐업 이력 [MNG_NO, name, opened, closed_at, is_closed, lon, lat, geocoded]."""
    from scrapers.mois_clinics import fill_missing_coords

    if path is None:
        files = sorted((DATA_RAW / "mois").glob("mois_clinics_*.parquet"))
        if not files:
            raise FileNotFoundError("mois_clinics parquet 없음. `python -m scrapers.mois_clinics` 먼저.")
        path = files[-1]
    df = pd.read_parquet(path)
    addr = df["ROAD_NM_ADDR"].fillna("").where(df["ROAD_NM_ADDR"].fillna("") != "", df["LOTNO_ADDR"].fillna(""))
    df = df[addr.str.split().str[0].isin(context_sido())]
    df = df[df["BPLC_NM"].str.contains(INTERNAL_KW, na=False)]
    df = df[df["SALS_STTS_NM"] != MOVED_STATUS].copy()
    df = fill_missing_coords(df)

    df["is_closed"] = df["SALS_STTS_NM"].isin(CLOSED_STATUSES)
    rtrcn = pd.to_datetime(df.get("LCPMT_RTRCN_YMD", pd.Series(dtype=str)).replace("", None), errors="coerce")
    df["closed_at"] = df["CLSBIZ_YMD"].fillna(rtrcn).where(df["is_closed"])
    ref = df["LCPMT_YMD"].max()
    out = df.rename(columns={"BPLC_NM": "name", "LCPMT_YMD": "opened"})[
        ["MNG_NO", "name", "opened", "closed_at", "is_closed", "lon", "lat", "geocoded"]
    ]
    logger.info(
        "내과 이력 %d건 (폐업 %d, 폐업일 결측 %d, 지오코딩 보충 %d, 좌표 없음 %d), 기준일 %s",
        len(out), int(out["is_closed"].sum()),
        int((out["is_closed"] & out["closed_at"].isna()).sum()),
        int(out["geocoded"].sum()), int(out["lon"].isna().sum()), ref.date(),
    )
    return out, ref


def _to_5179(lon, lat) -> tuple[np.ndarray, np.ndarray]:
    from pyproj import Transformer
    t = Transformer.from_crs(EPSG_WGS84, EPSG_KOREA, always_xy=True)
    x, y = t.transform(np.asarray(lon, float), np.asarray(lat, float))
    return np.asarray(x), np.asarray(y)


def compute_survival(
    centroids: pd.DataFrame,
    history: pd.DataFrame,
    ref: pd.Timestamp,
    years: int = SURVIVAL_YEARS,
    radius_m: float = COMPETITION_RADIUS_M,
    min_opened: int = SURVIVAL_MIN_OPENED,
) -> pd.DataFrame:
    """centroids [adm_cd, lon, lat] → 동별 생존 지표."""
    start = ref - pd.DateOffset(years=years)
    h = history.dropna(subset=["lon", "lat"])
    opened = h["opened"].between(start, ref).to_numpy()
    alive = opened & ~h["is_closed"].to_numpy()
    closed = h["closed_at"].between(start, ref).to_numpy()

    hx, hy = _to_5179(h["lon"], h["lat"])
    cx, cy = _to_5179(centroids["lon"], centroids["lat"])
    within = np.hypot(cx[:, None] - hx[None, :], cy[:, None] - hy[None, :]) <= radius_m

    tag = f"{years}y"
    out = pd.DataFrame({"adm_cd": centroids["adm_cd"].astype(str).values})
    out[f"n_open_{tag}"] = (within & opened).sum(axis=1)
    out[f"n_open_{tag}_alive"] = (within & alive).sum(axis=1)
    out[f"n_close_{tag}"] = (within & closed).sum(axis=1)
    out[f"net_open_{tag}"] = out[f"n_open_{tag}"] - out[f"n_close_{tag}"]
    n_open = out[f"n_open_{tag}"]
    out[f"survival_{tag}"] = (out[f"n_open_{tag}_alive"] / n_open).where(n_open >= min_opened)
    out["survival_sample_flag"] = n_open < min_opened
    logger.info(
        "생존율 %s (%s~%s, 반경 %dm): 산출 %d동 / 표본 부족 %d동, 중앙 %.2f",
        tag, start.date(), ref.date(), radius_m,
        int(out[f"survival_{tag}"].notna().sum()), int(out["survival_sample_flag"].sum()),
        out[f"survival_{tag}"].median(),
    )
    return out

"""v7 검진기관 경쟁 — 후보 반경 내 '내시경 위암검진 가능 기관' 수.

정의:
- 건보공단 국가검진기관(scrapers/nhis_checkup.py) 중 위암검진 실시(stomach)
- 종별 의원: HIRA 의료장비(15051055)에서 A304(식도·위·십이지장경) 보유가 확인된 곳만
  (위암검진은 위장조영으로도 가능하므로). 매칭 키 = (시도, 시군구, 기관명 공백 제거),
  실패 시 (시도, 기관명)이 시도 내 유일하면 매칭. 미매칭은 endo_unknown.
- 종별 병원·종합병원: 내시경 보유로 간주 (병원급 검진센터 — v6/HIRA 의원 수집에서 누락되던 경쟁)

반경 지표 (중심 = CENTROID_MODE overlay, 반경 = COMPETITION_RADIUS_M):
- n_endo_ck            : 내시경 위암검진 가능 기관 전체
- n_endo_ck_clinic     : 그중 의원
- n_endo_ck_hosp       : 그중 병원·종합병원
- n_colon_ck           : 대장암 검진기관 (참고 — 1차는 분변잠혈이라 내시경 여부 아님)
- endo_ck_per_10k      : n_endo_ck / (catchment_pop_5069 / 1만)
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd

from config.constants import COMPETITION_RADIUS_M, DATA_RAW, GI_EQUIPMENT_CODE_EGD

logger = logging.getLogger(__name__)

HOSPITAL_TYPES = {"병원", "종합병원", "상급종합병원"}
EQUIPMENT_CSV_GLOB = "건강보험심사평가원_의료장비 상세 현황_*.csv"


def _norm(s) -> str:
    return re.sub(r"\s+", "", str(s or ""))


def _sgg_from_addr(addr: str) -> str:
    """'경기도 부천시 원미구 …' → '부천시원미구', '서울특별시 강남구 …' → '강남구'."""
    t = str(addr or "").split()
    if len(t) < 2:
        return ""
    if t[1].endswith("시") and len(t) > 2 and t[2].endswith("구"):
        return t[1] + t[2]
    return t[1]


EGD_KEYS_CACHE = DATA_RAW.parent / "cache" / "egd_keys.parquet"


def build_egd_keys_cache(csv_path: Path | None = None) -> Path:
    """장비 CSV(로그인 다운로드, 143MB) → A304 보유 기관 (시도, 시군구, 기관명) 캐시.

    GH Actions runner에는 원본 CSV가 없으므로 이 캐시를 커밋해 사용.
    """
    from config.regions import context_sido

    if csv_path is None:
        files = sorted((DATA_RAW / "hira").glob(EQUIPMENT_CSV_GLOB))
        if not files:
            raise FileNotFoundError("HIRA 의료장비 CSV 없음 (data.go.kr 15051055)")
        csv_path = files[-1]
    e = pd.read_csv(csv_path, encoding="cp949",
                    usecols=["요양기관명", "시도명", "시군구명", "장비대분류코드"])
    e = e[e["시도명"].isin(context_sido()) & (e["장비대분류코드"] == GI_EQUIPMENT_CODE_EGD)]
    out = e[["시도명", "시군구명", "요양기관명"]].drop_duplicates()
    out.to_parquet(EGD_KEYS_CACHE, index=False)
    logger.info("saved %s (%d rows, %s)", EGD_KEYS_CACHE, len(out), csv_path.name)
    return EGD_KEYS_CACHE


def _egd_keys(sidos: set[str]) -> tuple[set, dict]:
    files = sorted((DATA_RAW / "hira").glob(EQUIPMENT_CSV_GLOB))
    if files:
        e = pd.read_csv(files[-1], encoding="cp949",
                        usecols=["요양기관명", "시도명", "시군구명", "장비대분류코드"])
        e = e[e["시도명"].isin(sidos) & (e["장비대분류코드"] == GI_EQUIPMENT_CODE_EGD)]
    elif EGD_KEYS_CACHE.exists():
        e = pd.read_parquet(EGD_KEYS_CACHE)
        e = e[e["시도명"].isin(sidos)]
    else:
        raise FileNotFoundError("HIRA 의료장비 CSV·egd_keys 캐시 모두 없음 (data.go.kr 15051055)")
    # 장비 1대 = 1행이라 같은 기관이 여러 행 → 기관 단위로 중복 제거 후 이름 유일성 판단
    # (2026-10-13 수정: 미제거 시 장비 2대 기관이 '동명 복수'로 오판되어 매칭 누락)
    e = e[["시도명", "시군구명", "요양기관명"]].drop_duplicates()
    full = {(r.시도명, _norm(r.시군구명), _norm(r.요양기관명)) for r in e.itertuples()}
    by_name = e.assign(k=e["요양기관명"].map(_norm)).groupby(["시도명", "k"]).size()
    unique_name = {k for k, v in by_name.items() if v == 1}
    return full, unique_name


def load_endoscopy_checkup() -> pd.DataFrame:
    files = sorted((DATA_RAW / "nhis").glob("nhis_checkup_*.parquet"))
    if not files:
        raise FileNotFoundError("nhis_checkup parquet 없음. `python -m scrapers.nhis_checkup` 먼저.")
    n = pd.read_parquet(files[-1])
    n = n[n["stomach"] & n["ykindnm"].isin(HOSPITAL_TYPES | {"의원"}) & n["lon"].notna()].copy()
    full, unique_name = _egd_keys(set(n["sido"]))

    def egd(r) -> str:
        if r.ykindnm in HOSPITAL_TYPES:
            return "hospital"
        name = _norm(r.hmcNm)
        if (r.sido, _sgg_from_addr(r.locAddr), name) in full or (r.sido, name) in unique_name:
            return "yes"
        return "unknown"

    n["endo"] = [egd(r) for r in n.itertuples()]
    logger.info("위암검진기관 %d: %s (%s)", len(n), n["endo"].value_counts().to_dict(), files[-1].name)
    return n


def compute_checkup_competition(centroids: pd.DataFrame, ck: pd.DataFrame,
                                radius_m: float = COMPETITION_RADIUS_M) -> pd.DataFrame:
    from scoring.survival import _to_5179

    nhis_all = pd.read_parquet(sorted((DATA_RAW / "nhis").glob("nhis_checkup_*.parquet"))[-1])
    colon = nhis_all[nhis_all["colon"] & nhis_all["lon"].notna()]

    cx, cy = _to_5179(centroids["lon"], centroids["lat"])

    def within(df: pd.DataFrame) -> np.ndarray:
        x, y = _to_5179(df["lon"], df["lat"])
        return np.hypot(cx[:, None] - x[None, :], cy[:, None] - y[None, :]) <= radius_m

    w = within(ck)
    is_clinic = (ck["endo"] == "yes").to_numpy()
    is_hosp = (ck["endo"] == "hospital").to_numpy()
    is_unknown = (ck["endo"] == "unknown").to_numpy()
    out = pd.DataFrame({"adm_cd": centroids["adm_cd"].astype(str).values})
    out["n_endo_ck_clinic"] = (w & is_clinic).sum(axis=1)
    out["n_endo_ck_hosp"] = (w & is_hosp).sum(axis=1)
    out["n_endo_ck"] = out["n_endo_ck_clinic"] + out["n_endo_ck_hosp"]
    out["n_stomach_ck_unknown"] = (w & is_unknown).sum(axis=1)
    out["n_colon_ck"] = within(colon).sum(axis=1)
    return out

"""ODSay 대중교통 길찾기 — 이촌역(원점) → 각 행정동 중심점 소요시간.

docs/PLAN.md §7 리스크 대응: **영구 캐시**. 월 5,000건 한도지만
캐시 hit 시 0건 소모. 첫 실행만 653건, 이후 경계 변경/신규 동 추가분만.

캐시 파일: data/cache/odsay_commute.parquet (git 커밋)
- 키: adm_cd (행정동코드). 경계 GeoJSON 버전과 쌍을 이룬다.
- 값: minutes (int), fetched_at (iso8601)
- 캐시 무효화 기준: (adm_cd 신규) OR (--force) OR (경계 버전 변경 시 수동 삭제)

API: https://api.odsay.com/v1/api/searchPubTransPathT
- SX, SY = 출발 경도/위도, EX, EY = 도착
- OPT=0 (전체 교통수단)
- 응답 result.path[0].info.totalTime (분 단위)
- 실패 시 COMMUTE_FALLBACK_MIN(999) 저장하고 재시도 목록에 기록

이촌역 원점 (ORIGIN_LNG/ORIGIN_LAT, config.constants).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import (
    COMMUTE_FALLBACK_MIN,
    DATA_CACHE,
    DATA_CLEANED,
    ORIGIN_LAT,
    ORIGIN_LNG,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.odsay.com/v1/api/searchPubTransPathT"
CACHE_PATH = DATA_CACHE / "odsay_commute.parquet"


class ODSayError(RuntimeError):
    pass


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    reraise=True,
)
def _call_odsay(
    api_key: str,
    sx: float, sy: float,
    ex: float, ey: float,
) -> dict:
    params = {
        "apiKey": api_key,
        "SX": sx, "SY": sy,
        "EX": ex, "EY": ey,
        "OPT": 0,
    }
    resp = requests.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # ODSay는 HTTP 200 + JSON 내 error 코드 반환
    if "error" in data:
        err = data["error"]
        code = err.get("code")
        msg = err.get("msg")
        # -8(동일출발지/도착지), -98(경로없음) 등은 재시도 불필요
        raise ODSayError(f"ODSay error [{code}] {msg}")
    return data


def _extract_minutes(data: dict) -> int:
    """응답 JSON에서 최소 소요시간(분) 추출.

    result.path[*].info.totalTime 중 최솟값. path 없으면 예외.
    """
    result = data.get("result") or {}
    paths = result.get("path") or []
    if not paths:
        raise ODSayError("no path in response")
    times = [p.get("info", {}).get("totalTime") for p in paths]
    times = [int(t) for t in times if isinstance(t, (int, float))]
    if not times:
        raise ODSayError("no totalTime in any path")
    return min(times)


def _load_cache() -> pd.DataFrame:
    if CACHE_PATH.exists():
        df = pd.read_parquet(CACHE_PATH)
        logger.info("cache hit: %d entries in %s", len(df), CACHE_PATH)
        return df
    logger.info("cache cold: %s 없음", CACHE_PATH)
    return pd.DataFrame(columns=["adm_cd", "minutes", "fetched_at"])


def _save_cache(df: pd.DataFrame) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values("adm_cd").to_parquet(CACHE_PATH, index=False)
    logger.info("cache saved: %s (%d entries)", CACHE_PATH, len(df))


def fetch_commute_all(
    admin_centroid: pd.DataFrame | None = None,
    force: bool = False,
    max_calls: int | None = None,
) -> pd.DataFrame:
    """전체 행정동 소요시간 수집 (캐시 우선).

    Parameters
    ----------
    admin_centroid : DataFrame
        columns = [adm_cd, lon, lat, ...]. None이면 data/cleaned/admin_centroid.parquet.
    force : bool
        True면 캐시 무시하고 전부 재호출.
    max_calls : int | None
        테스트/쿼터 보호용 호출 상한. None이면 무제한.

    Returns
    -------
    DataFrame [adm_cd, minutes, fetched_at] — 전체 대상 동.
    """
    load_dotenv()
    api_key = os.environ.get("ODSAY_KEY")
    if not api_key:
        raise RuntimeError(".env의 ODSAY_KEY 미설정.")

    if admin_centroid is None:
        admin_centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")

    cache = _load_cache() if not force else pd.DataFrame(
        columns=["adm_cd", "minutes", "fetched_at"]
    )
    cached_codes = set(cache["adm_cd"].astype(str))

    targets = admin_centroid[["adm_cd", "adm_nm", "lon", "lat"]].copy()
    targets["adm_cd"] = targets["adm_cd"].astype(str)
    to_fetch = targets[~targets["adm_cd"].isin(cached_codes)]
    logger.info("to fetch: %d / %d (캐시 %d)",
                len(to_fetch), len(targets), len(cached_codes))

    if max_calls is not None:
        to_fetch = to_fetch.head(max_calls)
        logger.info("max_calls=%d 제한 적용 → %d건만 호출", max_calls, len(to_fetch))

    new_rows = []
    failures = []
    for i, row in enumerate(to_fetch.itertuples(index=False), 1):
        try:
            data = _call_odsay(
                api_key,
                sx=ORIGIN_LNG, sy=ORIGIN_LAT,
                ex=float(row.lon), ey=float(row.lat),
            )
            minutes = _extract_minutes(data)
        except Exception as e:
            logger.warning("[%d/%d] %s (%s) 실패: %s",
                           i, len(to_fetch), row.adm_nm, row.adm_cd, e)
            failures.append((row.adm_cd, row.adm_nm, str(e)))
            minutes = COMMUTE_FALLBACK_MIN

        new_rows.append({
            "adm_cd": row.adm_cd,
            "minutes": int(minutes),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
        if i % 50 == 0:
            logger.info("  진행 %d/%d — checkpoint save", i, len(to_fetch))
            checkpoint = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint = checkpoint.drop_duplicates("adm_cd", keep="last")
            _save_cache(checkpoint)

    if new_rows:
        merged = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        merged = merged.drop_duplicates("adm_cd", keep="last")
        _save_cache(merged)
    else:
        merged = cache

    if failures:
        logger.warning("실패 %d건 — 재시도 필요:", len(failures))
        for cd, nm, e in failures[:10]:
            logger.warning("  %s %s: %s", cd, nm, e)
        if len(failures) > 10:
            logger.warning("  ... 외 %d건", len(failures) - 10)

    # 대상 653동 기준으로 필터
    out = targets[["adm_cd"]].merge(merged, on="adm_cd", how="left")
    # 캐시에 없고 이번에도 못 받은 동 = fallback
    n_fallback = out["minutes"].isna().sum()
    if n_fallback:
        logger.warning("minutes 미확보 %d동 → fallback(%d)", n_fallback, COMMUTE_FALLBACK_MIN)
        out["minutes"] = out["minutes"].fillna(COMMUTE_FALLBACK_MIN).astype(int)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="ODSay 이촌역→행정동 소요시간 (캐시 우선)")
    parser.add_argument("--force", action="store_true", help="캐시 무시 전체 재호출")
    parser.add_argument("--max-calls", type=int, default=None,
                        help="쿼터 보호용 호출 상한 (기본 무제한)")
    parser.add_argument("--admin-centroid", type=Path, default=None)
    args = parser.parse_args()

    centroid = pd.read_parquet(
        args.admin_centroid or (DATA_CLEANED / "admin_centroid.parquet")
    )
    df = fetch_commute_all(centroid, force=args.force, max_calls=args.max_calls)

    # 요약
    valid = df[df["minutes"] < COMMUTE_FALLBACK_MIN]
    logger.info("완료: %d동 (유효 %d, fallback %d)",
                len(df), len(valid), len(df) - len(valid))
    if len(valid):
        logger.info("소요시간 분포 (분): min=%d, median=%d, p90=%d, max=%d",
                    valid["minutes"].min(),
                    int(valid["minutes"].median()),
                    int(valid["minutes"].quantile(0.9)),
                    valid["minutes"].max())
    return 0


if __name__ == "__main__":
    sys.exit(main())

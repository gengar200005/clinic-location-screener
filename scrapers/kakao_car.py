"""카카오 모빌리티 Directions — 이촌역(원점) → 각 행정동 중심점 자차 소요시간.

`departure_time` 파라미터에 미래 timestamp(YYYYMMDDHHMMSS) 지정 시
과거 교통 통계 기반 **누적 평균 예측** 반환. 실시간 아닌 "평일 특정 시각
평균" 값이라 일요일·새벽에 돌려도 결과 동일.

사용자 기존 Notion DB(58 생활권)와 동일 방법론:
- 출발: 이촌1동 주민센터 (ORIGIN_LNG/LAT)
- 도착 시각: 다음 화요일 07:30 KST (8시 도착 목표)
- priority=TIME (최단시간)

캐시: data/cache/kakao_car.parquet (영구 커밋)
- columns: adm_cd, minutes, fetched_at, departure_time
- 동일 departure_time 건은 hit. 시각 바꾸면 재수집.

무료 쿼터: 카카오 모빌리티 일 10,000건 (653건이면 하루 안에 여유).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
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

KST = timezone(timedelta(hours=9))
DIRECTIONS_URL = "https://apis-navi.kakaomobility.com/v1/directions"
CACHE_PATH = DATA_CACHE / "kakao_car.parquet"

# commute_checker.py와 동일: 호출 간격 (API 안정화)
CALL_DELAY_SEC = 0.3


def next_weekday_0730_kst(weekday: int = 1) -> datetime:
    """다음 평일 특정 요일 07:30 KST.

    weekday: 0=월, 1=화, ..., 4=금. 기본 화(commute_checker.py와 동일).
    오늘이 그 요일이고 이미 07:30 지났으면 다음 주.
    """
    now = datetime.now(KST)
    days_ahead = (weekday - now.weekday()) % 7
    target = (now + timedelta(days=days_ahead)).replace(
        hour=7, minute=30, second=0, microsecond=0
    )
    if target <= now:
        target += timedelta(days=7)
    return target


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=20),
    reraise=True,
)
def _call_kakao(
    api_key: str,
    origin: tuple[float, float],
    dest: tuple[float, float],
    departure_time: str,
) -> dict:
    headers = {"Authorization": f"KakaoAK {api_key}"}
    params = {
        "origin": f"{origin[0]},{origin[1]}",
        "destination": f"{dest[0]},{dest[1]}",
        "departure_time": departure_time,
        "priority": "TIME",
    }
    resp = requests.get(DIRECTIONS_URL, headers=headers, params=params, timeout=15)
    if resp.status_code in (401, 403):
        raise RuntimeError(f"{resp.status_code} auth/forbidden: {resp.text[:200]}")
    if resp.status_code == 429:
        raise RuntimeError("429 quota exceeded")
    resp.raise_for_status()
    return resp.json()


def _extract_minutes(data: dict) -> int | None:
    """응답에서 duration(초) → 분 변환. 실패 시 None."""
    routes = data.get("routes") or []
    if not routes:
        return None
    r0 = routes[0]
    # result_code 0=정상, 나머지는 실패 (104=출발지/도착지 너무 가까움 등)
    if r0.get("result_code", -1) != 0:
        return None
    duration = (r0.get("summary") or {}).get("duration")
    if not isinstance(duration, (int, float)):
        return None
    return max(1, round(duration / 60))


def _load_cache() -> pd.DataFrame:
    if CACHE_PATH.exists():
        df = pd.read_parquet(CACHE_PATH)
        logger.info("cache hit: %d entries in %s", len(df), CACHE_PATH)
        return df
    logger.info("cache cold: %s 없음", CACHE_PATH)
    return pd.DataFrame(
        columns=["adm_cd", "minutes", "fetched_at", "departure_time"]
    )


def _save_cache(df: pd.DataFrame) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values("adm_cd").to_parquet(CACHE_PATH, index=False)
    logger.info("cache saved: %s (%d entries)", CACHE_PATH, len(df))


def fetch_commute_all(
    admin_centroid: pd.DataFrame | None = None,
    force: bool = False,
    max_calls: int | None = None,
    departure_time: str | None = None,
) -> pd.DataFrame:
    """전체 행정동 자차 소요시간 (캐시 우선).

    Parameters
    ----------
    departure_time : str (YYYYMMDDHHMMSS)
        None이면 다음 화요일 07:30 KST. 동일 departure_time은 캐시 hit.
    """
    load_dotenv()
    api_key = os.environ.get("KAKAO_KEY")
    if not api_key:
        raise RuntimeError(".env의 KAKAO_KEY 미설정.")

    if admin_centroid is None:
        admin_centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")

    if departure_time is None:
        dt = next_weekday_0730_kst(weekday=1)  # 화요일
        departure_time = dt.strftime("%Y%m%d%H%M%S")
    logger.info("departure_time=%s (KST)", departure_time)

    cache = _load_cache() if not force else pd.DataFrame(
        columns=["adm_cd", "minutes", "fetched_at", "departure_time"]
    )
    if not cache.empty:
        cached_mask = cache["departure_time"] == departure_time
        cached_codes = set(cache.loc[cached_mask, "adm_cd"].astype(str))
    else:
        cached_codes = set()

    targets = admin_centroid[["adm_cd", "adm_nm", "lon", "lat"]].copy()
    targets["adm_cd"] = targets["adm_cd"].astype(str)
    to_fetch = targets[~targets["adm_cd"].isin(cached_codes)]
    logger.info("to fetch: %d / %d (캐시 hit %d)",
                len(to_fetch), len(targets), len(cached_codes))

    if max_calls is not None:
        to_fetch = to_fetch.head(max_calls)
        logger.info("max_calls=%d 제한 → %d건만 호출", max_calls, len(to_fetch))

    new_rows = []
    failures = []
    for i, row in enumerate(to_fetch.itertuples(index=False), 1):
        try:
            data = _call_kakao(
                api_key,
                origin=(ORIGIN_LNG, ORIGIN_LAT),
                dest=(float(row.lon), float(row.lat)),
                departure_time=departure_time,
            )
            minutes = _extract_minutes(data)
            if minutes is None:
                # result_code != 0 (경로 없음 등) — fallback
                logger.warning("[%d/%d] %s (%s) no route",
                               i, len(to_fetch), row.adm_nm, row.adm_cd)
                failures.append((row.adm_cd, row.adm_nm, "no_route"))
                minutes = COMMUTE_FALLBACK_MIN
        except Exception as e:
            msg = str(e)
            if "401" in msg or "403" in msg or "429" in msg:
                logger.error("조기 중단: %s", e)
                raise
            logger.warning("[%d/%d] %s (%s) 실패: %s",
                           i, len(to_fetch), row.adm_nm, row.adm_cd, e)
            failures.append((row.adm_cd, row.adm_nm, str(e)[:80]))
            minutes = COMMUTE_FALLBACK_MIN

        new_rows.append({
            "adm_cd": row.adm_cd,
            "minutes": int(minutes),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "departure_time": departure_time,
        })

        if i % 50 == 0:
            logger.info("  진행 %d/%d — checkpoint save", i, len(to_fetch))
            cp = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
            cp = cp.drop_duplicates(subset=["adm_cd", "departure_time"], keep="last")
            _save_cache(cp)

        time.sleep(CALL_DELAY_SEC)

    if new_rows:
        merged = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        merged = merged.drop_duplicates(subset=["adm_cd", "departure_time"], keep="last")
        _save_cache(merged)
    else:
        merged = cache

    if failures:
        logger.warning("실패 %d건:", len(failures))
        for cd, nm, e in failures[:10]:
            logger.warning("  %s %s: %s", cd, nm, e)
        if len(failures) > 10:
            logger.warning("  ... 외 %d건", len(failures) - 10)

    # 현재 departure_time 기준 정리
    latest = merged[merged["departure_time"] == departure_time]
    out = targets[["adm_cd"]].merge(latest, on="adm_cd", how="left")
    n_fallback = out["minutes"].isna().sum()
    if n_fallback:
        out["minutes"] = out["minutes"].fillna(COMMUTE_FALLBACK_MIN).astype(int)
        logger.warning("minutes 미확보 %d동 → fallback(%d)", n_fallback, COMMUTE_FALLBACK_MIN)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Kakao Mobility 이촌→행정동 자차 소요시간 (평일 07:30 누적통계)")
    parser.add_argument("--force", action="store_true", help="캐시 무시 전체 재호출")
    parser.add_argument("--max-calls", type=int, default=None)
    parser.add_argument("--departure-time", default=None,
                        help="YYYYMMDDHHMMSS (미지정 시 다음 화요일 07:30 KST)")
    args = parser.parse_args()

    df = fetch_commute_all(
        force=args.force,
        max_calls=args.max_calls,
        departure_time=args.departure_time,
    )

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

"""⚠️ DEPRECATED: scrapers/kakao_car.py로 대체됨.

TMAP 무료 키는 /tmap/routes/prediction (시간대 예측) 미지원 (2026-04-19 검증).
실시간 /tmap/routes만 가능 → 실행 시점이 평일 07:30이어야 의미 있음.
카카오 모빌리티는 departure_time 파라미터로 누적통계 예측 제공 → 그쪽 사용.

미래에 TMAP Business 유료 전환 시 이 파일 재활용 가능.

캐시: data/cache/tmap_commute.parquet (영구 커밋, git tracked)
- columns: adm_cd, minutes, fetched_at, prediction_time, mode
- mode = "prediction" | "realtime"
- 동일 prediction_time 건은 캐시 hit

Reference:
- https://openapi.sk.com/portal/product/ProductApiView.do (TMAP API 문서)
- 파라미터: startX/Y, endX/Y (WGS84 경도/위도), predictionType, predictionTime
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
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
PREDICTION_URL = "https://apis.openapi.sk.com/tmap/routes/prediction?version=1&format=json"
REALTIME_URL = "https://apis.openapi.sk.com/tmap/routes?version=1&format=json"
CACHE_PATH = DATA_CACHE / "tmap_commute.parquet"


class TMAPError(RuntimeError):
    pass


def next_monday_0730_kst() -> datetime:
    """이번 주 또는 다음 주 월요일 07:30 KST.

    오늘이 월요일 07:30 이전이면 오늘 사용. 그 외엔 다음 월요일.
    """
    now = datetime.now(KST)
    weekday = now.weekday()  # 월=0
    days_ahead = (0 - weekday) % 7
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
def _call_tmap(
    api_key: str,
    sx: float, sy: float,
    ex: float, ey: float,
    prediction_time: datetime | None,
    mode: str,
) -> dict:
    body = {
        "startX": str(sx), "startY": str(sy),
        "endX": str(ex), "endY": str(ey),
        "reqCoordType": "WGS84GEO",
        "resCoordType": "WGS84GEO",
        "searchOption": "0",  # 교통최적+추천
        "trafficInfo": "Y",
    }
    if mode == "prediction" and prediction_time is not None:
        body["predictionType"] = "departure"
        # TMAP 포맷: "2026-04-20T07:30:00+0900" (콜론 없는 타임존)
        body["predictionTime"] = prediction_time.strftime("%Y-%m-%dT%H:%M:%S%z")
        url = PREDICTION_URL
    else:
        url = REALTIME_URL

    headers = {"appKey": api_key, "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=30)
    if resp.status_code == 401:
        raise TMAPError(f"401 auth: appKey 확인 ({resp.text[:200]})")
    if resp.status_code == 403:
        raise TMAPError(f"403 forbidden: 해당 API 구독 필요 ({resp.text[:200]})")
    if resp.status_code == 429:
        raise TMAPError(f"429 quota: 일일 호출량 초과")
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise TMAPError(f"TMAP error: {data['error']}")
    return data


def _extract_minutes(data: dict) -> int:
    """TMAP 응답에서 소요시간(분) 추출.

    GeoJSON FeatureCollection. features[0].properties.totalTime (초).
    """
    features = data.get("features") or []
    if not features:
        raise TMAPError("no features")
    props = features[0].get("properties") or {}
    total_sec = props.get("totalTime")
    if not isinstance(total_sec, (int, float)):
        raise TMAPError(f"no totalTime in properties: keys={list(props.keys())[:5]}")
    return max(1, int(round(total_sec / 60)))


def _load_cache() -> pd.DataFrame:
    if CACHE_PATH.exists():
        df = pd.read_parquet(CACHE_PATH)
        logger.info("cache hit: %d entries in %s", len(df), CACHE_PATH)
        return df
    logger.info("cache cold: %s 없음", CACHE_PATH)
    return pd.DataFrame(
        columns=["adm_cd", "minutes", "fetched_at", "prediction_time", "mode"]
    )


def _save_cache(df: pd.DataFrame) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values("adm_cd").to_parquet(CACHE_PATH, index=False)
    logger.info("cache saved: %s (%d entries)", CACHE_PATH, len(df))


def fetch_commute_all(
    admin_centroid: pd.DataFrame | None = None,
    force: bool = False,
    max_calls: int | None = None,
    mode: str = "prediction",
    prediction_time: datetime | None = None,
) -> pd.DataFrame:
    """전체 행정동 자차 소요시간 (캐시 우선).

    동일 prediction_time로 이미 캐시된 동은 스킵. prediction_time 바뀌면 재호출.
    """
    load_dotenv()
    api_key = os.environ.get("TMAP_KEY")
    if not api_key:
        raise RuntimeError(".env의 TMAP_KEY 미설정.")

    if admin_centroid is None:
        admin_centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")

    if mode == "prediction" and prediction_time is None:
        prediction_time = next_monday_0730_kst()
    pt_str = prediction_time.strftime("%Y-%m-%dT%H:%M:%S%z") if prediction_time else ""
    logger.info("mode=%s, prediction_time=%s", mode, pt_str or "(realtime)")

    cache = _load_cache() if not force else pd.DataFrame(
        columns=["adm_cd", "minutes", "fetched_at", "prediction_time", "mode"]
    )
    # 동일 prediction_time의 캐시만 hit로 간주
    if not cache.empty and mode == "prediction":
        cached_mask = (cache["mode"] == "prediction") & (cache["prediction_time"] == pt_str)
        cached_codes = set(cache.loc[cached_mask, "adm_cd"].astype(str))
    elif not cache.empty:
        cached_codes = set(cache.loc[cache["mode"] == mode, "adm_cd"].astype(str))
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
            data = _call_tmap(
                api_key,
                sx=ORIGIN_LNG, sy=ORIGIN_LAT,
                ex=float(row.lon), ey=float(row.lat),
                prediction_time=prediction_time,
                mode=mode,
            )
            minutes = _extract_minutes(data)
        except TMAPError as e:
            # 구독·쿼터 에러는 조기 중단
            msg = str(e)
            if "403" in msg or "401" in msg or "429" in msg:
                logger.error("조기 중단: %s", e)
                raise
            logger.warning("[%d/%d] %s (%s) 실패: %s",
                           i, len(to_fetch), row.adm_nm, row.adm_cd, e)
            failures.append((row.adm_cd, row.adm_nm, str(e)))
            minutes = COMMUTE_FALLBACK_MIN
        except Exception as e:
            logger.warning("[%d/%d] %s (%s) 예외: %s",
                           i, len(to_fetch), row.adm_nm, row.adm_cd, e)
            failures.append((row.adm_cd, row.adm_nm, str(e)))
            minutes = COMMUTE_FALLBACK_MIN

        new_rows.append({
            "adm_cd": row.adm_cd,
            "minutes": int(minutes),
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "prediction_time": pt_str,
            "mode": mode,
        })

        if i % 50 == 0:
            logger.info("  진행 %d/%d — checkpoint save", i, len(to_fetch))
            checkpoint = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
            checkpoint = checkpoint.drop_duplicates(
                subset=["adm_cd", "mode", "prediction_time"], keep="last"
            )
            _save_cache(checkpoint)

    if new_rows:
        merged = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True)
        merged = merged.drop_duplicates(
            subset=["adm_cd", "mode", "prediction_time"], keep="last"
        )
        _save_cache(merged)
    else:
        merged = cache

    if failures:
        logger.warning("실패 %d건:", len(failures))
        for cd, nm, e in failures[:10]:
            logger.warning("  %s %s: %s", cd, nm, e)

    # 대상 동 기준으로 정리 (해당 mode 행만)
    if mode == "prediction":
        latest = merged[(merged["mode"] == "prediction") & (merged["prediction_time"] == pt_str)]
    else:
        latest = merged[merged["mode"] == mode]
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
    parser = argparse.ArgumentParser(description="TMAP 이촌→행정동 자차 소요시간")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-calls", type=int, default=None)
    parser.add_argument("--mode", choices=["prediction", "realtime"], default="prediction",
                        help="prediction=월요일 07:30 예측(기본), realtime=실시간")
    parser.add_argument("--prediction-time", default=None,
                        help="ISO8601 (예: 2026-04-20T07:30:00+09:00). 미지정 시 다음 월요일 07:30 KST")
    args = parser.parse_args()

    pt = None
    if args.mode == "prediction":
        if args.prediction_time:
            pt = datetime.fromisoformat(args.prediction_time)
        else:
            pt = next_monday_0730_kst()

    df = fetch_commute_all(
        force=args.force,
        max_calls=args.max_calls,
        mode=args.mode,
        prediction_time=pt,
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

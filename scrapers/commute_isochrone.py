"""v7 통근 60분 필터용 출퇴근 4구간 소요시간 수집.

구간 (leg):
- car_am     : Kakao future directions, 이촌 → 동, 화 07:30 출발
- car_pm     : Kakao future directions, 동 → 이촌, 화 18:30 출발
- transit_am : ODSay searchPubTransPathT, 이촌 → 동
- transit_pm : ODSay searchPubTransPathT, 동 → 이촌

v6(kakao_car.py)는 `v1/directions`(실시간 교통)에 departure_time을 넘겼으나 이 엔드포인트는
출발시각을 반영하지 않는 것으로 확인됨 (2026-10-12: 운정중앙 실시간 69분 vs 07:30 예측 38분).
시간대 예측은 `v1/future/directions` 사용.

ODSay는 출발시각 파라미터가 없어 시간대 구분 불가 (방향만 구분).
GTX-A는 ODSay에 반영됨 (서울역↔운정중앙, 수서↔동탄 구간만; 서울역↔수서 미연결).

목적지 좌표 = CENTROID_MODE overlay (기본 1·2층 상가 중심, ADR-004).

캐시: data/cache/commute_v7.parquet (영구 커밋)
- columns: adm_cd, leg, minutes, status, lat, lon, depart, fetched_at
- (adm_cd, leg)별 1행. 좌표가 COORD_TOL_M 넘게 바뀌면 재조회.
- Kakao는 날짜가 아니라 요일·시각 슬롯(depart="tue0730")으로 hit 판정.
- status: ok / no_route / error. error는 다음 실행에서 재시도, no_route는 유지.

실행:
    python -m scrapers.commute_isochrone                    # 4구간 전부
    python -m scrapers.commute_isochrone --legs car_am car_pm --max-calls 500
"""
from __future__ import annotations

import argparse
import logging
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

from config.constants import (
    COMMUTE_AM_DEPART,
    COMMUTE_PM_DEPART,
    COMMUTE_WEEKDAY,
    DATA_CACHE,
    DATA_CLEANED,
    ORIGIN_LAT,
    ORIGIN_LNG,
)

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
KAKAO_FUTURE_URL = "https://apis-navi.kakaomobility.com/v1/future/directions"
ODSAY_URL = "https://api.odsay.com/v1/api/searchPubTransPathT"
CACHE_PATH = DATA_CACHE / "commute_v7.parquet"
CACHE_COLS = ["adm_cd", "leg", "minutes", "status", "lat", "lon", "depart", "fetched_at", "via"]
# via: anchor(기준점) / alt(기준점 no_route → 인구 가중 중심점 재조회, 2026-10-13 회현동 사례)

LEGS = ("car_am", "car_pm", "transit_am", "transit_pm")
COORD_TOL_M = 50
CALL_DELAY_SEC = {"car": 0.1, "transit": 1.0}   # ODSay: 동시·연속 호출 시 429 Too Many Requests (2026-10-13)
WORKERS = {"car": 6, "transit": 1}
MAX_CONSECUTIVE_ERRORS = 10   # 배치 전체 오류면 쿼터·장애로 보고 중단 (캐시는 저장)
CHECKPOINT_EVERY = 50
WEEKDAY_TAG = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class QuotaError(RuntimeError):
    """인증 실패·쿼터 초과 — 즉시 중단."""


def _depart_slot(leg: str) -> str:
    """캐시 키용 요일·시각 슬롯. transit은 시간대 없음."""
    if leg.startswith("transit"):
        return "any"
    h, m = COMMUTE_AM_DEPART if leg == "car_am" else COMMUTE_PM_DEPART
    return f"{WEEKDAY_TAG[COMMUTE_WEEKDAY]}{h:02d}{m:02d}"


def _next_departure(leg: str) -> str:
    """Kakao future API용 다음 해당 요일·시각 (YYYYMMDDHHMM, KST)."""
    h, m = COMMUTE_AM_DEPART if leg == "car_am" else COMMUTE_PM_DEPART
    now = datetime.now(KST)
    target = (now + timedelta(days=(COMMUTE_WEEKDAY - now.weekday()) % 7)).replace(
        hour=h, minute=m, second=0, microsecond=0,
    )
    if target <= now + timedelta(hours=1):
        target += timedelta(days=7)
    return target.strftime("%Y%m%d%H%M")


def _endpoints(leg: str, lon: float, lat: float) -> tuple[tuple[float, float], tuple[float, float]]:
    """(출발, 도착) — am은 이촌 → 동, pm은 동 → 이촌."""
    origin = (ORIGIN_LNG, ORIGIN_LAT)
    dong = (lon, lat)
    return (origin, dong) if leg.endswith("_am") else (dong, origin)


def _call_kakao(api_key: str, src, dst, departure: str) -> tuple[int | None, str]:
    resp = requests.get(
        KAKAO_FUTURE_URL,
        headers={"Authorization": f"KakaoAK {api_key}"},
        params={
            "origin": f"{src[0]},{src[1]}",
            "destination": f"{dst[0]},{dst[1]}",
            "departure_time": departure,
            "priority": "TIME",
        },
        timeout=15,
    )
    if resp.status_code in (401, 403, 429):
        raise QuotaError(f"Kakao {resp.status_code}: {resp.text[:200]}")
    resp.raise_for_status()
    routes = resp.json().get("routes") or []
    if not routes or routes[0].get("result_code", -1) != 0:
        return None, "no_route"
    duration = (routes[0].get("summary") or {}).get("duration")
    if not isinstance(duration, (int, float)):
        return None, "no_route"
    return max(1, round(duration / 60)), "ok"


def _call_odsay(api_key: str, src, dst) -> tuple[int | None, str]:
    resp = requests.get(
        ODSAY_URL,
        params={"apiKey": api_key, "SX": src[0], "SY": src[1], "EX": dst[0], "EY": dst[1], "OPT": 0},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        err = data["error"]
        err = err[0] if isinstance(err, list) else err
        code, msg = str(err.get("code")), str(err.get("msg") or err.get("message"))
        # -8: 동일 출발·도착, -98: 경로 없음 (odsay_transit.py 주석 기준).
        # 그 외 코드는 일시 오류로 보고 재시도 대상. 쿼터·인증은 메시지로 판별,
        # 못 잡으면 MAX_CONSECUTIVE_ERRORS로 중단.
        if code in {"-8", "-98", "-99"}:
            return None, "no_route"
        low = msg.lower().replace(" ", "")
        if code == "429" or "quota" in low or "인증" in msg or "초과" in msg or "apikey" in low:
            raise QuotaError(f"ODSay [{code}] {msg}")
        raise RuntimeError(f"ODSay [{code}] {msg}")
    paths = (data.get("result") or {}).get("path") or []
    times = [p.get("info", {}).get("totalTime") for p in paths]
    times = [int(t) for t in times if isinstance(t, (int, float))]
    if not times:
        return None, "no_route"
    return min(times), "ok"


def _dist_m(lat1, lon1, lat2, lon2) -> float:
    dlat = (lat2 - lat1) * 111_000
    dlon = (lon2 - lon1) * 111_000 * math.cos(math.radians(lat1))
    return math.hypot(dlat, dlon)


def load_cache() -> pd.DataFrame:
    if CACHE_PATH.exists():
        df = pd.read_parquet(CACHE_PATH)
        df["adm_cd"] = df["adm_cd"].astype(str)
        if "via" not in df.columns:
            df["via"] = "anchor"
        return df
    return pd.DataFrame(columns=CACHE_COLS)


def _save_cache(df: pd.DataFrame) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = df.drop_duplicates(["adm_cd", "leg"], keep="last")
    df.sort_values(["adm_cd", "leg"])[CACHE_COLS].to_parquet(CACHE_PATH, index=False)


def _needs_fetch(points: pd.DataFrame, cache: pd.DataFrame, leg: str) -> pd.DataFrame:
    """캐시 miss·좌표 변경·error·슬롯 변경 행만."""
    slot = _depart_slot(leg)
    c = cache[cache["leg"] == leg].set_index("adm_cd")
    keep = []
    for r in points.itertuples(index=False):
        if r.adm_cd not in c.index:
            keep.append(True)
            continue
        hit = c.loc[r.adm_cd]
        retry_alt = (
            leg.startswith("car") and hit["status"] == "no_route" and hit.get("via") != "alt"
            and pd.notna(getattr(r, "lat_alt", None))
        )
        stale = (
            retry_alt
            or hit["status"] == "error"
            or hit["depart"] != slot
            or _dist_m(float(hit["lat"]), float(hit["lon"]), r.lat, r.lon) > COORD_TOL_M
        )
        keep.append(bool(stale))
    return points[keep]


def _fetch_one(mode: str, key: str, leg: str, r, departure: str | None, stop: threading.Event) -> dict | None:
    """한 구간 호출. 쿼터 오류면 stop을 세우고 None — 같은 배치의 완료분은 저장되도록."""
    if stop.is_set():
        return None
    src, dst = _endpoints(leg, float(r.lon), float(r.lat))
    via = "anchor"
    try:
        if mode == "car":
            minutes, status = _call_kakao(key, src, dst, departure)
            # 상가 기준점이 차량 진입 불가 지점(시장·보행가)이면 no_route → 인구 가중 중심점으로 1회 재조회
            lat_alt, lon_alt = getattr(r, "lat_alt", None), getattr(r, "lon_alt", None)
            if status == "no_route" and pd.notna(lat_alt) and pd.notna(lon_alt):
                src, dst = _endpoints(leg, float(lon_alt), float(lat_alt))
                minutes, status = _call_kakao(key, src, dst, departure)
                via = "alt"
        else:
            minutes, status = _call_odsay(key, src, dst)
    except QuotaError as e:
        logger.error("[%s] 쿼터·인증 오류: %s", leg, e)
        stop.set()
        return None
    except Exception as e:  # noqa: BLE001 — 네트워크·일시 오류는 다음 실행 재시도
        logger.warning("[%s] %s 실패: %s", leg, r.adm_nm, e)
        minutes, status = None, "error"
    time.sleep(CALL_DELAY_SEC[mode])
    return {
        "adm_cd": r.adm_cd, "leg": leg, "minutes": minutes, "status": status,
        "lat": round(float(r.lat), 6), "lon": round(float(r.lon), 6),
        "depart": _depart_slot(leg),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "via": via,
    }


def fetch(points: pd.DataFrame, legs=LEGS, max_calls: int | None = None) -> pd.DataFrame:
    """points: [adm_cd, adm_nm, lat, lon]. 캐시 갱신 후 전체 캐시 반환.

    구간별로 CHECKPOINT_EVERY건씩 묶어 WORKERS개 스레드로 병렬 호출 → 배치마다 캐시 저장.
    배치 전체가 error면 쿼터·장애로 보고 중단.
    """
    load_dotenv()
    keys = {"car": os.environ.get("KAKAO_KEY"), "transit": os.environ.get("ODSAY_KEY")}
    cache = load_cache()
    cols = ["adm_cd", "adm_nm", "lat", "lon"] + [c for c in ("lat_alt", "lon_alt") if c in points.columns]
    points = points[cols].copy()
    points["adm_cd"] = points["adm_cd"].astype(str)

    budget = max_calls
    for leg in legs:
        mode = leg.split("_")[0]
        if not keys[mode]:
            raise RuntimeError(f".env {'KAKAO_KEY' if mode == 'car' else 'ODSAY_KEY'} 미설정")
        todo = _needs_fetch(points, cache, leg)
        if budget is not None:
            todo = todo.head(budget)
            budget -= len(todo)
        departure = _next_departure(leg) if mode == "car" else None
        logger.info("[%s] to fetch %d / %d (depart=%s)", leg, len(todo), len(points),
                    departure or "n/a")
        rows = list(todo.itertuples(index=False))
        for b in range(0, len(rows), CHECKPOINT_EVERY):
            batch = rows[b:b + CHECKPOINT_EVERY]
            stop = threading.Event()
            with ThreadPoolExecutor(max_workers=WORKERS[mode]) as ex:
                results = list(ex.map(
                    lambda r: _fetch_one(mode, keys[mode], leg, r, departure, stop), batch,
                ))
            results = [r for r in results if r is not None]
            if results:
                cache = pd.concat([cache, pd.DataFrame(results)], ignore_index=True)
                _save_cache(cache)
                cache = load_cache()
            if stop.is_set():
                raise QuotaError(f"{leg}: 쿼터 소진 — 완료분 {len(results)}건 저장 후 중단")
            n_err = sum(r["status"] == "error" for r in results)
            logger.info("  [%s] %d/%d (error %d)", leg, b + len(batch), len(rows), n_err)
            if len(batch) >= MAX_CONSECUTIVE_ERRORS and n_err == len(batch):
                raise QuotaError(f"{leg}: 배치 전체 오류 — 쿼터·장애 추정")
        if budget is not None and budget <= 0:
            logger.info("max_calls=%d 도달 — 중단", max_calls)
            break
    return cache


def _car_ok_flags() -> pd.DataFrame:
    """캐시 기준 동별 자차 출퇴근 OK 여부 [adm_cd, car_ok]."""
    from config.constants import COMMUTE_LIMIT_MIN
    c = load_cache()
    c = c[c["leg"].isin(["car_am", "car_pm"])]
    c = c.assign(ok=(c["status"] == "ok") & (c["minutes"].astype(float) <= COMMUTE_LIMIT_MIN))
    flags = c.groupby("adm_cd")["ok"].agg(lambda x: bool(x.all()) and len(x) == 2)
    return flags.rename("car_ok").reset_index()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="v7 출퇴근 4구간 소요시간 (캐시 우선)")
    # 기본 = 자차만. ODSay 약관: 대중교통 API는 서비스용 단일 호출 한정, 분석·대량 호출은 사전 협의 필수
    # (Basic 무료 30건/일, 2026-10-13 확인). transit 구간은 명시적으로 --legs 지정 시에만.
    parser.add_argument("--legs", nargs="+", choices=LEGS, default=["car_am", "car_pm"])
    parser.add_argument("--max-calls", type=int, default=None)
    args = parser.parse_args()

    from scoring.spatial_join import apply_centroid_overlay
    centroid = apply_centroid_overlay(pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet"))
    pop_path = DATA_CACHE / "admin_centroid_pop.parquet"
    if pop_path.exists():
        alt = pd.read_parquet(pop_path)[["adm_cd", "lat_pop", "lon_pop"]].rename(
            columns={"lat_pop": "lat_alt", "lon_pop": "lon_alt"})
        alt["adm_cd"] = alt["adm_cd"].astype(str)
        centroid = centroid.assign(adm_cd=centroid["adm_cd"].astype(str)).merge(alt, on="adm_cd", how="left")
    centroid["adm_cd"] = centroid["adm_cd"].astype(str)
    # ODSay 일일 한도가 작음(2026-10-12 ~100건에서 429) → 자차로 판정 안 된 동을 먼저 조회.
    # 자차 OK 동의 대중교통 시간은 표시용이라 후순위.
    flags = centroid[["adm_cd"]].merge(_car_ok_flags(), on="adm_cd", how="left")
    undecided = centroid[~flags["car_ok"].fillna(False).astype(bool).values]
    try:
        if 0 < len(undecided) < len(centroid):
            logger.info("1차: 자차 미판정 %d동 우선 (출퇴근 구간 모두)", len(undecided))
            fetch(undecided, legs=args.legs, max_calls=args.max_calls)
        cache = fetch(centroid, legs=args.legs, max_calls=args.max_calls)
    except QuotaError as e:
        logger.error("중단 (캐시 저장됨, 재실행 시 이어서): %s", e)
        return 2

    codes = set(centroid["adm_cd"].astype(str))
    c = cache[cache["adm_cd"].isin(codes)]
    summary = c.pivot_table(index="leg", columns="status", values="adm_cd", aggfunc="count", fill_value=0)
    logger.info("구간별 상태 (대상 %d동):\n%s", len(codes), summary.to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())

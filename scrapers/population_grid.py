"""WorldPop 100m 격자 인구 GeoTIFF 다운로드.

목적: 행정동 폴리곤 안의 픽셀 인구를 가중치로 인구 가중 중심점 계산.
   기하 중심점이 산·하천·공원에 찍히는 centroid_mismatch 문제 해결.

데이터: WorldPop "Top-down Unconstrained 2020" Korea (KOR)
- URL: data.worldpop.org/GIS/Population/Global_2000_2020/2020/KOR/kor_ppp_2020.tif
- 라이선스: CC BY 4.0
- 단위: 픽셀당 인구 수 (float)
- 좌표계: EPSG:4326 (WGS84)
- 파일 ~72MB, 분기 1회 갱신 (실제론 거의 안 변함)

idempotent. 한 번 다운로드 후 재실행 시 skip.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

WORLDPOP_URL = "https://data.worldpop.org/GIS/Population/Global_2000_2020/2020/KOR/kor_ppp_2020.tif"
OUT_DIR = DATA_RAW / "worldpop"
OUT_PATH = OUT_DIR / "kor_ppp_2020.tif"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=20), reraise=True)
def download(force: bool = False) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUT_PATH.exists() and not force:
        size_mb = OUT_PATH.stat().st_size / 1e6
        logger.info("skip: %s already exists (%.1f MB)", OUT_PATH, size_mb)
        return OUT_PATH

    logger.info("downloading WorldPop Korea 2020 (~72MB)...")
    with requests.get(WORLDPOP_URL, stream=True, timeout=180) as resp:
        resp.raise_for_status()
        with open(OUT_PATH, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 16):
                f.write(chunk)
    size_mb = OUT_PATH.stat().st_size / 1e6
    logger.info("saved %s (%.1f MB)", OUT_PATH, size_mb)
    return OUT_PATH


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description="WorldPop Korea 100m 격자 인구 다운로드")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    download(force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

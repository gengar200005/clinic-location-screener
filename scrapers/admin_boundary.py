"""vuski/admdongkor 행정동 경계 GeoJSON 다운로드.

1주차 1회성 작업. 이후 월 1회 .github/workflows/boundary_refresh.yml에서 호출.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import requests

from config.constants import DATA_RAW

logger = logging.getLogger(__name__)

DEFAULT_VERSION = "ver20260201"
URL_TEMPLATE = "https://raw.githubusercontent.com/vuski/admdongkor/master/{version}/HangJeongDong_{version}.geojson"


def download_boundary(version: str = DEFAULT_VERSION, force: bool = False) -> Path:
    out_dir = DATA_RAW / "admin_boundary"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"HangJeongDong_{version}.geojson"

    if out.exists() and not force:
        logger.info("skip: %s already exists (%.1f MB)", out, out.stat().st_size / 1e6)
        return out

    url = os.environ.get("ADMDONG_URL") or URL_TEMPLATE.format(version=version)
    logger.info("downloading %s", url)

    resp = requests.get(url, timeout=60, stream=True)
    resp.raise_for_status()

    with open(out, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 14):
            f.write(chunk)

    logger.info("saved %s (%.1f MB)", out, out.stat().st_size / 1e6)
    return out


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Download vuski/admdongkor GeoJSON")
    parser.add_argument("--version", default=DEFAULT_VERSION)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    download_boundary(version=args.version, force=args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())

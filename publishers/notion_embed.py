"""Notion Top 30 페이지 본문에 GitHub Pages iframe embed 삽입.

notion_detail.py의 PNG+링크 방식을 **인터랙티브 Leaflet 맵** embed로 교체.
각 페이지 구조:
  1. 📍 위치 요약 (좌표·역·Kakao/Naver 링크)
  2. 🗺 인터랙티브 지도 (GitHub Pages embed)
  3. 📊 상세 점수
  4. ⚠️ 플래그 (해당 시)

의원 리스트는 embed 안에 포함되므로 페이지 본문에서 제거.

사용:
    python -m publishers.notion_embed [--only RANK]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from notion_client import Client

from config.constants import DATA_SCORED
from publishers.notion_detail import (
    _append_blocks,
    _bullet,
    _callout,
    _clear_page_content,
    _divider,
    _embed,
    _heading2,
    _paragraph,
    _rt,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://gengar200005.github.io/clinic-location-screener/web/detail/"


def build_embed_blocks(row: pd.Series) -> list[dict]:
    """페이지 본문 블록 — 요약 + embed + 점수 + 플래그."""
    adm_cd = str(row["adm_cd"])
    embed_url = f"{BASE_URL}#{adm_cd}"
    kakao_link = f"https://map.kakao.com/link/map/{row['adm_nm']},{row.get('lat','')},{row.get('lon','')}"
    # 좌표 없을 수도 있으니 Top 30 parquet에 포함된 건 rank·score·adm_cd 정도.
    # embed 내부에서 상세 표시되므로 여기는 최소 정보만.

    blocks: list[dict] = []

    blocks.append(_heading2("🗺 인터랙티브 지도"))
    blocks.append(_paragraph([
        _rt("의원 마커 클릭 또는 리스트 항목 클릭 → 지도 위 위치 하이라이트 + 상세 정보"),
    ]))
    blocks.append(_embed(embed_url))

    blocks.append(_divider())

    # 상세 점수
    blocks.append(_heading2("📊 상세 점수"))
    blocks.append(_bullet([
        _rt("총점: ", bold=True),
        _rt(f"{float(row['score']):.4f}  (C={float(row['c_norm']):.2f}, P={float(row['p_norm']):.2f}, T={float(row['t_norm']):.2f})"),
    ]))
    blocks.append(_bullet([
        _rt("40+ 인구: ", bold=True),
        _rt(f"{int(row['pop_40plus']):,}명"),
        _rt(f"  ·  총 {int(row['pop_total']):,}명  ·  비율 {float(row['ratio_40plus'])*100:.1f}%"),
    ]))
    blocks.append(_bullet([
        _rt("경쟁: ", bold=True),
        _rt(f"동내 {int(row['n_clinic'])}개  ·  "),
        _rt(f"500m {int(row.get('n_clinic_500m', row.get('n_within_radius', 0)))}개  ·  "),
        _rt(f"1km {int(row.get('n_clinic_1km', 0))}개  ·  "),
        _rt(f"2km {int(row.get('n_clinic_2km', 0))}개"),
    ]))
    blocks.append(_bullet([
        _rt("통근: ", bold=True),
        _rt(f"자차 {int(row['t_raw'])}분"),
        _rt(f"  ·  🚇 {row.get('nearest_station','-')} ({int(row.get('station_dist_m',0))}m)  "),
        _rt(f"·  역 500m 의원 {int(row.get('n_clinic_station_500m',0))}개"),
    ]))

    # 외부 링크
    blocks.append(_paragraph([
        _rt("🗺 외부 지도: "),
        _rt("Kakao Maps",
            link=f"https://map.kakao.com/link/search/{row['adm_nm']}"),
        _rt("  ·  "),
        _rt("네이버 지도",
            link=f"https://map.naver.com/p?q={row['adm_nm']}"),
    ]))

    # 플래그
    flags = []
    if row.get("med_desert_flag"):
        flags.append(("🏜", "의료사막 의심 (1km≤5 AND 2km≤30)"))
    if row.get("centroid_mismatch_flag"):
        flags.append(("📍", "중심점 에러 (500m=0 이지만 2km≥50) — 상권이 중심점 밖에 있음"))
    if row.get("suburban_cluster_flag"):
        flags.append(("🏘", "신도시 상가밀집형 (동내≥10 AND 1km≤5) — 아파트+집중 상가"))
    if flags:
        blocks.append(_divider())
        blocks.append(_heading2("⚠️ 플래그"))
        for emoji, text in flags:
            color = "orange_background" if emoji == "🏜" else "yellow_background"
            blocks.append(_callout(text, emoji=emoji, color=color))

    return blocks


def run(only_rank: int | None = None) -> dict:
    load_dotenv()
    token = os.environ.get("NOTION_TOKEN")
    ds_id = os.environ.get("NOTION_DS_ID")
    if not token or not ds_id:
        raise RuntimeError(".env의 NOTION_TOKEN·NOTION_DS_ID 미설정.")

    client = Client(auth=token)
    top30_files = sorted(DATA_SCORED.glob("top30_*.parquet"))
    top30 = pd.read_parquet(top30_files[-1])
    logger.info("loaded top30 from %s", top30_files[-1].name)

    from publishers.notion_sync import _query_existing
    name_to_id = _query_existing(client, ds_id)
    logger.info("existing pages: %d", len(name_to_id))

    processed = 0
    for _, row in top30.iterrows():
        rank = int(row["rank"])
        if only_rank is not None and rank != only_rank:
            continue
        name = str(row["adm_nm"])
        page_id = name_to_id.get(name)
        if not page_id:
            logger.warning("  [rank %d] %s — 없음 (skip)", rank, name)
            continue

        blocks = build_embed_blocks(row)
        _clear_page_content(client, page_id)
        _append_blocks(client, page_id, blocks)
        logger.info("  [rank %d] %s — %d blocks OK", rank, name, len(blocks))
        processed += 1

    summary = {"pages": processed}
    logger.info("complete: %s", summary)
    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Notion 30페이지 embed 교체")
    parser.add_argument("--only", type=int, default=None)
    args = parser.parse_args()
    run(only_rank=args.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())

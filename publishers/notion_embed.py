"""Notion Top 30 페이지 본문 자동/수동 영역 관리.

페이지 본문 구조:
  🤖 자동 갱신 영역  (← 시작 마커)
    · 🗺 인터랙티브 지도 (GitHub Pages iframe)
    · 📊 상세 점수
    · ⚠️ 플래그 (해당 시)
  ✍️ 답사 기록 (수동)  (← 보존 마커)
    · 📍 현장 답사 기록 (답사일·동행자·임대료·층수·면적)
    · 🔍 상권 현황
    · 🏥 경쟁 현황 (반경 500m)
    · 💬 총평
    · ✅ 체크리스트
    · 📎 메모

보존 로직:
- 두 마커가 모두 있으면: 마커 사이만 삭제, 새 자동 블록을 시작 마커 뒤에 삽입.
  → 답사 기록은 절대 건드리지 않음. 순위 이탈·재진입해도 보존.
- 마커 없으면 (신규 페이지 또는 옛 포맷): 전체 클리어 + 마커 포함 풀 템플릿 주입.

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
    _heading3,
    _paragraph,
    _rt,
    _todo,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://gengar200005.github.io/clinic-location-screener/web/detail/"

# 마커 heading 텍스트 (substring 매칭)
AUTO_MARKER_TEXT = "🤖 자동 갱신 영역"
MANUAL_MARKER_TEXT = "✍️ 답사 기록"


# ─────────────────────────────────────────────────────────
# 자동 영역 블록 생성
# ─────────────────────────────────────────────────────────
def _auto_marker_block() -> dict:
    return _heading2(f"{AUTO_MARKER_TEXT} (매주 토 03:00 자동 갱신)")


def _manual_marker_block() -> dict:
    return _heading2(f"{MANUAL_MARKER_TEXT} (수동 · 매주 갱신 시 보존됨)")


def build_auto_blocks(row: pd.Series) -> list[dict]:
    """자동 영역 본문 — 지도 + 점수 + 플래그. 매주 새로 생성.

    주의: 시작 마커는 별도로 _auto_marker_block()에서 처리 — 여기엔 포함 X.
    """
    adm_cd = str(row["adm_cd"])
    embed_url = f"{BASE_URL}#{adm_cd}"

    blocks: list[dict] = []

    # 지도
    blocks.append(_heading3("🗺 인터랙티브 지도"))
    blocks.append(_paragraph([
        _rt("의원 마커 클릭 또는 리스트 항목 클릭 → 지도 위 위치 하이라이트 + 상세 정보"),
    ]))
    blocks.append(_embed(embed_url))

    # 점수
    blocks.append(_heading3("📊 상세 점수"))
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
    commute_parts = [
        _rt("통근: ", bold=True),
        _rt(f"🚗 자차 {int(row['t_raw'])}분"),
    ]
    t_transit = row.get("t_transit")
    if pd.notna(t_transit) and int(t_transit) < 999:
        commute_parts.append(_rt(f"  ·  🚇 대중교통 {int(t_transit)}분"))
    commute_parts.append(
        _rt(f"  ·  🚉 {row.get('nearest_station','-')} ({int(row.get('station_dist_m',0))}m)  ·  역 500m 의원 {int(row.get('n_clinic_station_500m',0))}개")
    )
    blocks.append(_bullet(commute_parts))

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
        flags.append(("📍", "중심점 에러 (500m=0 이지만 2km≥50) — 상권이 중심점 밖"))
    if row.get("suburban_cluster_flag"):
        flags.append(("🏘", "신도시 상가밀집형 (동내≥10 AND 1km≤5)"))
    if flags:
        blocks.append(_heading3("⚠️ 플래그"))
        for emoji, text in flags:
            color = "orange_background" if emoji == "🏜" else "yellow_background"
            blocks.append(_callout(text, emoji=emoji, color=color))

    return blocks


def build_manual_template() -> list[dict]:
    """답사 기록 수동 영역 — 신규 페이지 최초 진입 시 1회 주입."""
    blocks: list[dict] = []

    blocks.append(_heading3("📍 현장 답사 기록"))
    blocks.append(_bullet([_rt("답사일: ")]))
    blocks.append(_bullet([_rt("동행자: ")]))
    blocks.append(_bullet([_rt("임대료: 보증금 ___ / 월세 ___")]))
    blocks.append(_bullet([_rt("층수·면적: ")]))
    blocks.append(_paragraph([
        _rt("💡 팁: 답사일·임대료·층수·면적은 상단 DB 프로퍼티에도 입력하면 "),
        _rt("📍 임장 관리 view", bold=True),
        _rt("에서 한눈에 비교 가능."),
    ]))

    blocks.append(_heading3("🔍 상권 현황"))
    blocks.append(_paragraph([_rt("유동인구·주요 상권 축·주차 여건·지하철 접근성 등")]))

    blocks.append(_heading3("🏥 경쟁 현황 (반경 500m)"))
    blocks.append(_paragraph([_rt("임장 중 실제 확인한 내과·소화기 의원 관찰 (대기환자 수·리모델링 여부·신규 개원 등)")]))

    blocks.append(_heading3("💬 총평"))
    blocks.append(_paragraph([_rt("")]))

    blocks.append(_heading3("✅ 체크리스트"))
    for item in [
        "1층 가시성 양호",
        "주차 공간 확보",
        "대중교통 접근성",
        "경쟁 내과 위치 확인",
        "건물 상태·리모델링 필요 여부",
        "건물주 미팅",
        "재방문 의향",
    ]:
        blocks.append(_todo(item, checked=False))

    blocks.append(_heading3("📎 사진·링크·메모"))
    blocks.append(_paragraph([_rt("")]))

    return blocks


# ─────────────────────────────────────────────────────────
# 마커 탐지 + 부분 업데이트
# ─────────────────────────────────────────────────────────
def _get_heading_text(block: dict) -> str:
    """heading_2/3 블록의 plain text 추출. 다른 타입은 빈 문자열."""
    btype = block.get("type", "")
    if btype not in ("heading_2", "heading_3"):
        return ""
    rt = block.get(btype, {}).get("rich_text", [])
    return "".join(r.get("plain_text", "") for r in rt)


def _list_all_children(client: Client, page_id: str) -> list[dict]:
    all_blocks: list[dict] = []
    start_cursor = None
    while True:
        resp = client.blocks.children.list(
            block_id=page_id, start_cursor=start_cursor, page_size=100,
        )
        all_blocks.extend(resp.get("results", []))
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return all_blocks


def _find_markers(children: list[dict]) -> tuple[int | None, int | None]:
    """자동 영역 시작·수동 영역 시작 heading 인덱스. 없으면 None."""
    auto_idx = None
    manual_idx = None
    for i, b in enumerate(children):
        t = _get_heading_text(b)
        if auto_idx is None and AUTO_MARKER_TEXT in t:
            auto_idx = i
        elif manual_idx is None and MANUAL_MARKER_TEXT in t:
            manual_idx = i
    return auto_idx, manual_idx


def update_page_body(
    client: Client,
    page_id: str,
    auto_blocks: list[dict],
) -> tuple[str, int]:
    """페이지 본문 갱신. 마커 있으면 자동 영역만 교체, 없으면 풀 템플릿 주입.

    반환: (모드 문자열, 삽입한 블록 수)
    """
    children = _list_all_children(client, page_id)
    auto_idx, manual_idx = _find_markers(children)

    if auto_idx is None or manual_idx is None or auto_idx >= manual_idx:
        # 신규 페이지 or 옛 포맷 → 전체 클리어 + 풀 템플릿
        _clear_page_content(client, page_id)
        full = (
            [_auto_marker_block()]
            + auto_blocks
            + [_divider()]
            + [_manual_marker_block()]
            + build_manual_template()
        )
        _append_blocks(client, page_id, full)
        return "full", len(full)

    # 기존 포맷 → 마커 사이만 교체
    auto_start_id = children[auto_idx]["id"]
    # 삭제 대상: auto_idx+1 ~ manual_idx-1
    to_delete = children[auto_idx + 1 : manual_idx]
    for b in to_delete:
        client.blocks.delete(block_id=b["id"])

    # 새 자동 블록을 시작 마커 바로 뒤에 삽입
    # auto_blocks는 일반적으로 <30개라 단일 요청으로 충분
    if auto_blocks:
        client.blocks.children.append(
            block_id=page_id,
            children=auto_blocks[:100],
            after=auto_start_id,
        )
        # 100 초과 시 chain (안전망)
        after_id = auto_start_id
        if len(auto_blocks) > 100:
            # 첫 청크 삽입 직후의 마지막 블록 ID를 다음 after로 쓰려면 response 필요
            # 단순화: 한 번 더 호출 (100 초과 케이스는 거의 없음)
            logger.warning("auto_blocks %d개 > 100 — 200까지만 처리", len(auto_blocks))
            client.blocks.children.append(
                block_id=page_id,
                children=auto_blocks[100:200],
                # after 없이 → 페이지 끝에 붙는 이슈. 100 초과 시 수작업 필요.
            )
    return "partial", len(auto_blocks)


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────
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
    n_full, n_partial = 0, 0
    for _, row in top30.iterrows():
        rank = int(row["rank"])
        if only_rank is not None and rank != only_rank:
            continue
        name = str(row["adm_nm"])
        page_id = name_to_id.get(name)
        if not page_id:
            logger.warning("  [rank %d] %s — 페이지 없음 (skip)", rank, name)
            continue

        auto_blocks = build_auto_blocks(row)
        mode, n = update_page_body(client, page_id, auto_blocks)
        if mode == "full":
            n_full += 1
        else:
            n_partial += 1
        logger.info("  [rank %d] %s — %s (%d blocks)", rank, name, mode, n)
        processed += 1

    summary = {
        "pages": processed,
        "full_template": n_full,
        "partial_auto": n_partial,
    }
    logger.info("complete: %s", summary)
    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Notion Top 30 페이지 본문 자동/수동 영역 관리"
    )
    parser.add_argument("--only", type=int, default=None,
                        help="특정 순위만 처리 (테스트용)")
    args = parser.parse_args()
    run(only_rank=args.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())

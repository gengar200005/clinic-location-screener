"""Notion Top 30 페이지 본문 자동/수동 영역 관리.

페이지 본문 구조 (3-zone):
  🤖 자동 갱신 영역  (← 매주 토 03:00 자동, notion_embed)
    · 🗺 인터랙티브 지도 / 📊 점수 / ⚠️ 플래그
  🧠 Claude 브리핑  (← 사용자가 토요일 "브리핑 업데이트" 지시 시)
    · SWOT·지역 분석 (Claude 세션에서 수동 트리거)
  ✍️ 답사 기록 (수동)  (← 사용자 영구 보존 영역)
    · 현장 답사·상권·경쟁·총평·체크리스트·메모

보존 로직:
- 3 마커 모두 있음 → 🤖와 🧠 사이만 교체 (데이터 zone). 브리핑·답사 기록 불가침.
- 2 마커 (🤖·✍️만, 옛 포맷) → 🧠 마커 + placeholder 마이그레이션 주입.
- 마커 없음 → 전체 클리어 + 3-zone 풀 템플릿 주입.

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
BRIEF_MARKER_TEXT = "🧠 Claude 브리핑"
MANUAL_MARKER_TEXT = "✍️ 답사 기록"


# ─────────────────────────────────────────────────────────
# 마커·placeholder 블록 생성
# ─────────────────────────────────────────────────────────
def _auto_marker_block() -> dict:
    return _heading2(f"{AUTO_MARKER_TEXT} (매주 토 03:00 자동 갱신)")


def _brief_marker_block() -> dict:
    return _heading2(f"{BRIEF_MARKER_TEXT} (토요일 수동 갱신)")


def _manual_marker_block() -> dict:
    return _heading2(f"{MANUAL_MARKER_TEXT} (수동 · 영구 보존)")


def _brief_placeholder_blocks() -> list[dict]:
    """브리핑 placeholder (최초 진입 시)."""
    return [_callout(
        "아직 브리핑이 생성되지 않았습니다. "
        "Claude Code 세션에서 '브리핑 업데이트' 지시 시 자동 생성됩니다.",
        emoji="💤", color="gray_background",
    )]


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


def _find_markers(
    children: list[dict],
) -> tuple[int | None, int | None, int | None]:
    """🤖·🧠·✍️ 마커 인덱스. 없으면 None."""
    auto_idx = brief_idx = manual_idx = None
    for i, b in enumerate(children):
        t = _get_heading_text(b)
        if auto_idx is None and AUTO_MARKER_TEXT in t:
            auto_idx = i
        elif brief_idx is None and BRIEF_MARKER_TEXT in t:
            brief_idx = i
        elif manual_idx is None and MANUAL_MARKER_TEXT in t:
            manual_idx = i
    return auto_idx, brief_idx, manual_idx


def _migrate_add_brief_marker(
    client: Client, page_id: str, children: list[dict], manual_idx: int,
) -> None:
    """2-marker 페이지에 🧠 브리핑 마커 + placeholder 주입.

    위치: 수동 영역 마커 바로 앞. manual_idx-1 블록 뒤에 삽입.
    """
    if manual_idx == 0:
        # 페이지 맨 위가 수동 마커 — 비정상이지만 방어
        return
    insert_after_id = children[manual_idx - 1]["id"]
    client.blocks.children.append(
        block_id=page_id,
        children=[_brief_marker_block()] + _brief_placeholder_blocks(),
        after=insert_after_id,
    )


def update_page_body(
    client: Client,
    page_id: str,
    auto_blocks: list[dict],
) -> tuple[str, int]:
    """페이지 본문 갱신. 3-zone 마커 기준 데이터 영역(🤖↔🧠)만 교체.

    반환: (모드 문자열, 삽입한 블록 수)
    """
    children = _list_all_children(client, page_id)
    auto_idx, brief_idx, manual_idx = _find_markers(children)

    # Case 1: 마커 없음 또는 🤖·✍️ 중 하나 없음 → 전체 클리어 + 풀 템플릿
    if auto_idx is None or manual_idx is None or auto_idx >= manual_idx:
        _clear_page_content(client, page_id)
        full = (
            [_auto_marker_block()]
            + auto_blocks
            + [_divider()]
            + [_brief_marker_block()]
            + _brief_placeholder_blocks()
            + [_divider()]
            + [_manual_marker_block()]
            + build_manual_template()
        )
        _append_blocks(client, page_id, full)
        return "full", len(full)

    # Case 2: 🤖·✍️만 있고 🧠 없음 (옛 2-marker 포맷) → 마이그레이션
    if brief_idx is None:
        _migrate_add_brief_marker(client, page_id, children, manual_idx)
        # 갱신된 children 다시 로드
        children = _list_all_children(client, page_id)
        auto_idx, brief_idx, manual_idx = _find_markers(children)
        if brief_idx is None:
            # 마이그레이션 실패 — 풀 템플릿으로 폴백
            _clear_page_content(client, page_id)
            full = (
                [_auto_marker_block()] + auto_blocks + [_divider()]
                + [_brief_marker_block()] + _brief_placeholder_blocks() + [_divider()]
                + [_manual_marker_block()] + build_manual_template()
            )
            _append_blocks(client, page_id, full)
            return "full-fallback", len(full)

    # Case 3: 3 마커 모두 존재 → 🤖와 🧠 사이만 교체
    auto_start_id = children[auto_idx]["id"]
    to_delete = children[auto_idx + 1 : brief_idx]
    for b in to_delete:
        client.blocks.delete(block_id=b["id"])

    if auto_blocks:
        # 일반적으로 <30개. 100 초과는 로그만 경고.
        if len(auto_blocks) > 100:
            logger.warning("auto_blocks %d개 > 100 — 100까지만 삽입", len(auto_blocks))
        client.blocks.children.append(
            block_id=page_id,
            children=auto_blocks[:100],
            after=auto_start_id,
        )
    return "partial", len(auto_blocks)


# ─────────────────────────────────────────────────────────
# 브리핑 영역 업데이트 (수동 트리거 — 사용자가 토요일 지시)
# ─────────────────────────────────────────────────────────
def update_briefing(
    client: Client, page_id: str, brief_blocks: list[dict],
) -> int:
    """🧠와 ✍️ 마커 사이를 brief_blocks로 교체. 수동 갱신 전용.

    페이지에 3 마커가 모두 있어야 함. 없으면 ValueError.
    반환: 삽입한 블록 수.
    """
    children = _list_all_children(client, page_id)
    auto_idx, brief_idx, manual_idx = _find_markers(children)
    if brief_idx is None or manual_idx is None or brief_idx >= manual_idx:
        raise ValueError(
            "🧠 / ✍️ 마커를 찾을 수 없습니다. "
            "`python -m publishers.notion_embed` 먼저 실행해 페이지 구조를 만드세요."
        )
    brief_start_id = children[brief_idx]["id"]
    to_delete = children[brief_idx + 1 : manual_idx]
    for b in to_delete:
        client.blocks.delete(block_id=b["id"])
    if brief_blocks:
        client.blocks.children.append(
            block_id=page_id,
            children=brief_blocks[:100],
            after=brief_start_id,
        )
    return len(brief_blocks)


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

"""Claude 브리핑 → Notion 페이지 🧠 영역 주입.

사용자가 Claude Code 세션에서 "브리핑 업데이트" 지시 시, Claude가 Top 30
각 동을 분석(+ 웹 검색)하여 briefings.yaml 을 작성한 뒤 이 스크립트로 일괄 주입.

페이지 구조 (notion_embed.py와 공유):
  🤖 자동 갱신 영역 (매주 cron)
  🧠 Claude 브리핑   ← 이 섹션만 update_briefing()으로 교체
  ✍️ 답사 기록 (사용자 보존)

입력 파일 형식 (YAML, UTF-8):

    2026-04-19:   # 생성일 (메타)
    briefings:
      "경기도 남양주시 호원동":   # adm_nm (Top 30 DB 타이틀)
        summary: "호원동은 40+ 인구 47K 압도적 1위..."
        swot:
          strengths: ["...", "..."]
          weaknesses: ["...", "..."]
          opportunities: ["...", "..."]
          threats: ["...", "..."]
        notes:     # 선택 — 자유 서술 bullet
          - "인근 재건축 N건"
          - "..."

사용:
    python -m publishers.claude_briefing --input briefings.yaml
    python -m publishers.claude_briefing --input briefings.yaml --only "경기도 남양주시 호원동"
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import yaml
from dotenv import load_dotenv
from notion_client import Client

from publishers.notion_detail import (
    _bullet,
    _heading3,
    _paragraph,
    _rt,
)
from publishers.notion_embed import update_briefing

logger = logging.getLogger(__name__)

SWOT_LABELS = [
    ("strengths",     "💪 Strengths (강점)"),
    ("weaknesses",    "⚠️ Weaknesses (약점)"),
    ("opportunities", "🌱 Opportunities (기회)"),
    ("threats",       "⚔️ Threats (위협)"),
]


def build_brief_blocks(brief: dict, generated_at: str) -> list[dict]:
    """brief 딕셔너리 → Notion block 리스트."""
    blocks: list[dict] = []

    # 메타
    blocks.append(_paragraph([
        _rt(f"갱신일: {generated_at}", color="gray"),
    ]))

    # 요약
    summary = brief.get("summary")
    if summary:
        blocks.append(_paragraph([_rt(summary)]))

    # SWOT
    swot = brief.get("swot") or {}
    for key, title in SWOT_LABELS:
        items = swot.get(key) or []
        if items:
            blocks.append(_heading3(title))
            for item in items:
                blocks.append(_bullet([_rt(str(item))]))

    # 참고 노트
    notes = brief.get("notes") or []
    if notes:
        blocks.append(_heading3("📌 Notes"))
        for n in notes:
            blocks.append(_bullet([_rt(str(n))]))

    return blocks


def run(input_path: Path, only_name: str | None = None) -> dict:
    load_dotenv()
    token = os.environ.get("NOTION_TOKEN")
    ds_id = os.environ.get("NOTION_DS_ID")
    if not token or not ds_id:
        raise RuntimeError(".env의 NOTION_TOKEN·NOTION_DS_ID 미설정.")

    with open(input_path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    generated_at = doc.get("generated_at") or date.today().isoformat()
    briefings: dict = doc.get("briefings") or {}
    if not briefings:
        raise RuntimeError(f"{input_path}에 briefings 키가 비어있음")

    client = Client(auth=token)
    from publishers.notion_sync import _query_existing
    name_to_id = _query_existing(client, ds_id)
    logger.info("existing pages: %d", len(name_to_id))

    updated = 0
    missing = 0
    for adm_nm, brief in briefings.items():
        if only_name and adm_nm != only_name:
            continue
        page_id = name_to_id.get(adm_nm)
        if not page_id:
            logger.warning("  %s — 페이지 없음 (skip)", adm_nm)
            missing += 1
            continue
        blocks = build_brief_blocks(brief, generated_at)
        n = update_briefing(client, page_id, blocks)
        logger.info("  %s — %d blocks OK", adm_nm, n)
        updated += 1

    summary = {"updated": updated, "missing": missing, "total": len(briefings)}
    logger.info("complete: %s", summary)
    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Claude 브리핑 → Notion 🧠 영역 일괄 주입"
    )
    parser.add_argument("--input", type=Path, required=True,
                        help="briefings.yaml 경로")
    parser.add_argument("--only", default=None,
                        help="특정 adm_nm 하나만 업데이트 (테스트용)")
    args = parser.parse_args()
    run(input_path=args.input, only_name=args.only)
    return 0


if __name__ == "__main__":
    sys.exit(main())

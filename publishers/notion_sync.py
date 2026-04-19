"""Top 30 → Notion DB 동기화 (idempotent upsert).

사용처:
- 로컬 수동: `python -m publishers.notion_sync`
- GH Actions 주간 배치 (6주차): secrets에 NOTION_TOKEN·NOTION_DB_ID 등록 후 호출

동작:
1. data/scored/top30_*.parquet 최신 파일 읽기
2. Notion DB 쿼리해 기존 페이지 (동명 title 기준) 맵 구축
3. 각 Top 30 행에 대해:
   - 기존 있음 → properties 업데이트 (임장상태·메모 보존)
   - 없음 → 신규 페이지 생성 (임장상태="미방문")
4. 기존이지만 이번 Top 30에 없는 페이지 → 메모에 "탈락" 태그 (물리 삭제 X)
   → 수동 리뷰 후 Notion에서 직접 삭제하거나 "배제" 처리

키: `동명` (title). adm_cd 컬럼도 함께 저장(보조 메타).
행정동명 재편성은 드물고 adm_nm이 "시도+시군구+동" 전체라서 충돌 없음.

환경변수:
- NOTION_TOKEN       Notion integration secret (ntn_...)
- NOTION_DB_ID       Top 30 DB의 database_id
- NOTION_DS_ID       (선택) data_source_id. 미지정 시 DB로부터 조회.

의존성: notion-client (requirements.txt에 이미 포함)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from notion_client import Client

from config.constants import DATA_SCORED

logger = logging.getLogger(__name__)

NOTION_VERSION = "2022-06-28"  # notion-client 내부에서 사용

# DB 프로퍼티 이름 (create_database 때 정의한 것과 일치)
PROP_TITLE = "동명"
PROP_RANK = "순위"
PROP_SCORE = "총점"
PROP_SIDO = "시도"
PROP_SGG = "시군구"
PROP_C = "경쟁점수"
PROP_P = "인구점수"
PROP_T = "통근점수"
PROP_N_CLINIC = "의원수"
PROP_POP_TOTAL = "총인구"
PROP_POP_40 = "40+인구"
PROP_RATIO_40 = "40+비율"
PROP_COMMUTE = "자차분"
PROP_N500 = "500m내 의원"
PROP_STATUS = "임장상태"
PROP_MEMO = "메모"
PROP_DATE = "업데이트일"
PROP_ADM_CD = "adm_cd"  # 숨김 키 (RICH_TEXT로 추가 필요)


def _find_latest_top30() -> Path:
    files = sorted(DATA_SCORED.glob("top30_*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"{DATA_SCORED} 에 top30_*.parquet 없음. "
            "`python -m scoring.pipeline` 먼저."
        )
    return files[-1]


def _page_props(row: pd.Series, today: str) -> dict:
    """top30 row → Notion page properties dict (Notion API v1 포맷).

    Number / Rich text / Title / Select / Date 각 타입 맞춤.
    """
    return {
        PROP_TITLE: {"title": [{"text": {"content": str(row["adm_nm"])}}]},
        PROP_ADM_CD: {"rich_text": [{"text": {"content": str(row["adm_cd"])}}]},
        PROP_RANK: {"number": int(row["rank"])},
        PROP_SCORE: {"number": round(float(row["score"]), 4)},
        PROP_SIDO: {"rich_text": [{"text": {"content": str(row["sido"])}}]},
        PROP_SGG: {"rich_text": [{"text": {"content": str(row["sgg"])}}]},
        PROP_C: {"number": round(float(row["c_norm"]), 4)},
        PROP_P: {"number": round(float(row["p_norm"]), 4)},
        PROP_T: {"number": round(float(row["t_norm"]), 4)},
        PROP_N_CLINIC: {"number": int(row["n_clinic"])},
        PROP_POP_TOTAL: {"number": int(row["pop_total"])},
        PROP_POP_40: {"number": int(row["pop_40plus"])},
        PROP_RATIO_40: {"number": round(float(row["ratio_40plus"]), 4)},
        PROP_COMMUTE: {"number": int(row["t_raw"])},
        PROP_N500: {"number": int(row["n_within_radius"])},
        PROP_DATE: {"date": {"start": today}},
    }


def _query_existing(client: Client, database_id: str) -> dict[str, str]:
    """기존 페이지를 동명(title) → page_id 맵으로 반환."""
    result: dict[str, str] = {}
    start_cursor = None
    while True:
        resp = client.databases.query(
            database_id=database_id,
            start_cursor=start_cursor,
            page_size=100,
        )
        for page in resp.get("results", []):
            props = page.get("properties") or {}
            title_prop = props.get(PROP_TITLE)
            if not title_prop:
                continue
            title_items = title_prop.get("title") or []
            if title_items:
                name = title_items[0].get("plain_text") or ""
                if name:
                    result[name] = page["id"]
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    return result


def sync(
    database_id: str | None = None,
    notion_token: str | None = None,
    top30_path: Path | None = None,
    mark_dropped: bool = True,
) -> dict:
    """Top 30 → Notion DB upsert.

    Returns summary dict: {created, updated, dropped, total}.
    """
    load_dotenv()
    token = notion_token or os.environ.get("NOTION_TOKEN")
    db_id = database_id or os.environ.get("NOTION_DB_ID")
    if not token:
        raise RuntimeError(".env의 NOTION_TOKEN 미설정.")
    if not db_id:
        raise RuntimeError(".env의 NOTION_DB_ID 미설정.")

    top30_path = top30_path or _find_latest_top30()
    df = pd.read_parquet(top30_path)
    logger.info("loaded %d rows from %s", len(df), top30_path.name)

    today = date.today().isoformat()
    client = Client(auth=token)

    logger.info("querying existing pages...")
    existing = _query_existing(client, db_id)
    logger.info("existing pages: %d", len(existing))

    created, updated = 0, 0
    current_names = set()
    for _, row in df.iterrows():
        name = str(row["adm_nm"])
        current_names.add(name)
        props = _page_props(row, today)

        if name in existing:
            # 기존 페이지 업데이트. 임장상태·메모는 건드리지 않음.
            client.pages.update(page_id=existing[name], properties=props)
            updated += 1
        else:
            # 신규. 상태 "미방문"으로 시작.
            props[PROP_STATUS] = {"select": {"name": "미방문"}}
            client.pages.create(
                parent={"database_id": db_id},
                properties=props,
            )
            created += 1

    # Top 30에서 빠진 기존 페이지 → "탈락" 메모 태그 (물리 삭제 X)
    dropped = 0
    if mark_dropped:
        for name, page_id in existing.items():
            if name not in current_names:
                client.pages.update(
                    page_id=page_id,
                    properties={
                        PROP_MEMO: {
                            "rich_text": [{
                                "text": {"content": f"⬇️ {today} Top 30 밖으로 탈락"}
                            }]
                        }
                    },
                )
                dropped += 1

    summary = {
        "created": created,
        "updated": updated,
        "dropped": dropped,
        "total": len(df),
    }
    logger.info("sync complete: %s", summary)
    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Top 30 → Notion DB sync")
    parser.add_argument("--top30", type=Path, default=None,
                        help="top30 parquet 경로 (미지정 시 최신)")
    parser.add_argument("--no-mark-dropped", action="store_true",
                        help="탈락 페이지에 메모 태깅 건너뜀")
    args = parser.parse_args()

    sync(
        top30_path=args.top30,
        mark_dropped=not args.no_mark_dropped,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

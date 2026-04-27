"""HIRA 의료장비 상세 현황 — 의원별 장비 보유 정보 추출.

원본: data.go.kr 데이터셋 15051055 ("건강보험심사평가원_의료장비 상세 현황").
CSV 파일을 data/raw/hira/ 에 떨궈두면 이 모듈이 ykiho별 장비 set으로 정리.

API 아닌 파일 데이터셋이라 OpenAPI 스크래퍼와 달리 다운로드는 사용자가 직접
data.go.kr 로그인 후 수행. 이 모듈은 후처리만 담당.

산출물:
- data/cleaned/clinic_equipment.parquet
  컬럼: ykiho, has_egd, has_colo, is_gi, eqp_codes (str — 콤마 결합)
  - has_egd: 식도·위·십이지장경 (A304) 보유
  - has_colo: 결장경 (A320) 보유
  - is_gi: A304 ∩ A320 (둘 다 보유 → GI 후보)
  - eqp_codes: 보유 장비대분류코드 set (분석용)

reference: ~/.claude/.../memory/reference_hira_equipment.md
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from config.constants import DATA_CLEANED, DATA_RAW

logger = logging.getLogger(__name__)

EQUIPMENT_CSV_GLOB = "건강보험심사평가원_의료장비 상세 현황_*.csv"

# A304: 식도·위·십이지장경 (위내시경)
# A320: 결장경 (대장내시경)
GI_EGD_CODE = "A304"
GI_COLO_CODE = "A320"


def find_latest_csv() -> Path | None:
    candidates = sorted((DATA_RAW / "hira").glob(EQUIPMENT_CSV_GLOB))
    return candidates[-1] if candidates else None


def build_equipment_table(csv_path: Path) -> pd.DataFrame:
    """장비 CSV → 의원당 장비코드 집계 + GI 플래그.

    cp949 인코딩, ~711k 행. 메모리는 200MB 정도.
    """
    logger.info("loading %s", csv_path)
    df = pd.read_csv(
        csv_path,
        encoding="cp949",
        dtype={"장비대분류코드": str, "장비세분류코드": str},
        usecols=["암호화된 요양기호", "장비대분류코드"],
    )
    df = df.rename(columns={"암호화된 요양기호": "ykiho", "장비대분류코드": "eqp_code"})
    logger.info("loaded %d equipment rows", len(df))

    grouped = (
        df.groupby("ykiho")["eqp_code"]
        .apply(lambda s: ",".join(sorted(set(s))))
        .reset_index()
        .rename(columns={"eqp_code": "eqp_codes"})
    )
    grouped["has_egd"] = grouped["eqp_codes"].str.contains(GI_EGD_CODE, regex=False)
    grouped["has_colo"] = grouped["eqp_codes"].str.contains(GI_COLO_CODE, regex=False)
    grouped["is_gi"] = grouped["has_egd"] & grouped["has_colo"]

    logger.info(
        "yadmNm-equipment 매핑: %d 의료기관 (위 %d, 대장 %d, 둘 다 %d)",
        len(grouped), grouped["has_egd"].sum(),
        grouped["has_colo"].sum(), grouped["is_gi"].sum(),
    )
    return grouped


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="HIRA 의료장비 CSV 후처리")
    parser.add_argument("--csv", type=Path, default=None,
                        help="CSV 경로 (기본: data/raw/hira/ 최신)")
    parser.add_argument("--out", type=Path,
                        default=DATA_CLEANED / "clinic_equipment.parquet")
    args = parser.parse_args()

    csv_path = args.csv or find_latest_csv()
    if csv_path is None:
        logger.error(
            "data/raw/hira/ 에 '%s' 파일 없음. data.go.kr 데이터셋 15051055 다운로드.",
            EQUIPMENT_CSV_GLOB,
        )
        return 1

    table = build_equipment_table(csv_path)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    table.to_parquet(args.out, index=False)
    logger.info("saved %s (%d rows)", args.out, len(table))
    return 0


if __name__ == "__main__":
    sys.exit(main())

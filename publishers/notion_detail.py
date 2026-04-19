"""Top 30 Notion 페이지 본문에 상세 맵핑 추가.

각 페이지에 다음 섹션 자동 생성:
1. 📍 위치 — 좌표·최근접역·지도 링크 3종
2. 🗺 Google Maps 임베드 (Notion iframe)
3. 🏥 반경 1km 의원 리스트 (거리·주소·전화·의사수·개원일, 소화기 ⭐ 마크)
4. 📊 상세 점수
5. ⚠️ 플래그 (해당 시)

멱등성: 실행 시마다 기존 페이지 본문 전체 삭제 후 재작성.
사용자 메모는 **메모 프로퍼티**에 적을 것 (본문 X).

사용:
    python -m publishers.notion_detail [--dry-run] [--only RANK]
"""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from pathlib import Path

import geopandas as gpd
import httpx
import matplotlib as mpl
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from notion_client import Client

from config.constants import DATA_CLEANED, DATA_SCORED, EPSG_KOREA, EPSG_WGS84

logger = logging.getLogger(__name__)

RADIUS_M = 1000  # 반경 1km
MAP_RADIUS_M = 1500  # 지도에 표시할 범위 (여유)
MAX_CLINICS_IN_LIST = 50  # 페이지당 최대 표시 의원 수

# ─── 한글 폰트 설정 ────────────────────────────────────
for _font in ["Malgun Gothic", "NanumGothic", "AppleGothic", "Noto Sans CJK KR"]:
    if any(_font in f.name for f in fm.fontManager.ttflist):
        mpl.rc("font", family=_font)
        logger.info("matplotlib font: %s", _font)
        break
mpl.rc("axes", unicode_minus=False)


# ─────────────────────────────────────────────────────────
# 데이터 로드 + 의원 거리 계산
# ─────────────────────────────────────────────────────────
def load_data():
    top30 = pd.read_parquet(DATA_SCORED / "top30_2026-04-19.parquet")
    # 가장 최신 top30 자동 선택
    files = sorted(DATA_SCORED.glob("top30_*.parquet"))
    top30 = pd.read_parquet(files[-1])

    centroid = pd.read_parquet(DATA_CLEANED / "admin_centroid.parquet")
    clinics = pd.read_parquet(DATA_CLEANED / "clinics_by_dong.parquet")

    # 의원 좌표를 EPSG:5179로 변환
    gdf = gpd.GeoDataFrame(
        clinics,
        geometry=gpd.points_from_xy(
            pd.to_numeric(clinics["XPos"]),
            pd.to_numeric(clinics["YPos"]),
        ),
        crs=EPSG_WGS84,
    ).to_crs(EPSG_KOREA)
    clinics = clinics.copy()
    clinics["x_5179"] = gdf.geometry.x.values
    clinics["y_5179"] = gdf.geometry.y.values

    return top30, centroid, clinics


def nearby_clinics(
    dong_row: pd.Series,
    centroid: pd.DataFrame,
    clinics: pd.DataFrame,
    radius_m: int = RADIUS_M,
) -> pd.DataFrame:
    """특정 동 중심점 기준 반경 내 의원 리스트 + 거리."""
    adm_cd = str(dong_row["adm_cd"])
    cent_row = centroid[centroid["adm_cd"].astype(str) == adm_cd]
    if cent_row.empty:
        return pd.DataFrame()
    cx = float(cent_row.iloc[0]["x_5179"])
    cy = float(cent_row.iloc[0]["y_5179"])

    dx = clinics["x_5179"].to_numpy() - cx
    dy = clinics["y_5179"].to_numpy() - cy
    dist = np.sqrt(dx * dx + dy * dy)
    mask = dist <= radius_m
    sub = clinics[mask].copy()
    sub["dist_m"] = dist[mask].astype(int)
    return sub.sort_values("dist_m").head(MAX_CLINICS_IN_LIST)


# ─────────────────────────────────────────────────────────
# 정적 PNG 지도 생성
# ─────────────────────────────────────────────────────────
def generate_map_png(
    row: pd.Series,
    centroid: pd.DataFrame,
    clinics: pd.DataFrame,
    map_radius_m: int = MAP_RADIUS_M,
) -> bytes | None:
    """행정동 + 주변 의원을 matplotlib으로 렌더링. PNG bytes 반환."""
    adm_cd = str(row["adm_cd"])
    cent_row = centroid[centroid["adm_cd"].astype(str) == adm_cd]
    if cent_row.empty:
        return None
    cx = float(cent_row.iloc[0]["x_5179"])
    cy = float(cent_row.iloc[0]["y_5179"])

    # 지도 범위 내 의원만
    dx = clinics["x_5179"].to_numpy() - cx
    dy = clinics["y_5179"].to_numpy() - cy
    dist = np.sqrt(dx * dx + dy * dy)
    mask = dist <= map_radius_m
    near = clinics[mask].copy()
    near["dx"] = dx[mask]
    near["dy"] = dy[mask]
    near["dist"] = dist[mask]

    # 색상: 소화기 ⭐ 빨강, 내과 계열 파랑, 기타 회색
    def _color(r):
        if bool(r.get("is_gi", False)):
            return "#e53935"  # red
        kind = str(r.get("clCdNm", ""))
        if "의원" in kind:
            return "#1976d2"  # blue
        return "#9e9e9e"  # gray

    near["_color"] = near.apply(_color, axis=1)

    fig, ax = plt.subplots(figsize=(7, 7), dpi=110)
    # 반경 원 (500m, 1km)
    for r, color, alpha in [(500, "#4caf50", 0.12), (1000, "#2196f3", 0.08)]:
        circle = plt.Circle((0, 0), r, fill=True, facecolor=color,
                            alpha=alpha, edgecolor=color, linewidth=1, linestyle="--")
        ax.add_patch(circle)
        ax.text(0, r, f"{r}m", ha="center", va="bottom",
                fontsize=8, color="#555")

    # 의원 점
    # 소화기는 맨 위에 그리도록 뒤에 plot
    non_gi = near[~near["is_gi"]] if "is_gi" in near.columns else near
    gi = near[near["is_gi"]] if "is_gi" in near.columns else near.iloc[0:0]
    ax.scatter(non_gi["dx"], non_gi["dy"], c=non_gi["_color"],
               s=40, alpha=0.75, edgecolor="white", linewidth=0.5, zorder=3)
    ax.scatter(gi["dx"], gi["dy"], c=gi["_color"],
               s=120, marker="*", alpha=0.95, edgecolor="white", linewidth=0.8, zorder=5)

    # 중심점 (노란 별)
    ax.scatter(0, 0, c="#ffd600", s=300, marker="*",
               edgecolor="black", linewidth=1.5, zorder=10)
    ax.annotate(str(row["adm_nm"]).split(" ")[-1],
                (0, 0), xytext=(8, 8), textcoords="offset points",
                fontsize=10, fontweight="bold")

    # 최근접역 표시
    if pd.notna(row.get("nearest_station")) and pd.notna(row.get("station_dist_m")):
        # 역 좌표는 별도 파일에서 읽어야 함 (station_metrics에서 사용)
        try:
            from scoring.station_metrics import load_stations
            st = load_stations()
            sta = st[st["name"] == row["nearest_station"]]
            if not sta.empty:
                sx = float(sta.iloc[0]["x_5179"]) - cx
                sy = float(sta.iloc[0]["y_5179"]) - cy
                if abs(sx) < map_radius_m and abs(sy) < map_radius_m:
                    ax.scatter(sx, sy, c="#ff6f00", s=200, marker="s",
                               edgecolor="white", linewidth=1, zorder=7)
                    ax.annotate(f"🚇 {row['nearest_station']}",
                                (sx, sy), xytext=(8, 8), textcoords="offset points",
                                fontsize=9, color="#ff6f00", fontweight="bold")
        except Exception:
            pass

    # 범위 설정
    margin = map_radius_m * 1.1
    ax.set_xlim(-margin, margin)
    ax.set_ylim(-margin, margin)
    ax.set_aspect("equal")
    ax.grid(True, linestyle=":", alpha=0.3)
    ax.set_xlabel("m (동→)")
    ax.set_ylabel("m (북↑)")

    # 통계 타이틀
    n_gi = int(gi.shape[0])
    n_others = int(len(near) - n_gi)
    ax.set_title(
        f"{row['adm_nm']}  ·  반경 {map_radius_m//1000}km 내 의원 {len(near)}개"
        f"  (소화기 ⭐{n_gi} / 그 외 {n_others})",
        fontsize=11, fontweight="bold"
    )

    # 레전드
    from matplotlib.lines import Line2D
    legend_items = [
        Line2D([0], [0], marker="*", color="w", markerfacecolor="#ffd600",
               markeredgecolor="black", markersize=14, label="동 중심점"),
        Line2D([0], [0], marker="*", color="w", markerfacecolor="#e53935",
               markersize=12, label="소화기 태깅"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#1976d2",
               markersize=8, label="일반 의원"),
        Line2D([0], [0], marker="o", color="w", markerfacecolor="#9e9e9e",
               markersize=8, label="병원·기타"),
        Line2D([0], [0], marker="s", color="w", markerfacecolor="#ff6f00",
               markersize=10, label="최근접역"),
    ]
    ax.legend(handles=legend_items, loc="lower right", fontsize=8, framealpha=0.85)

    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def upload_png_to_notion(token: str, png_bytes: bytes, filename: str) -> str:
    """Notion file_uploads API로 PNG 업로드. 반환: file_upload_id."""
    client = Client(auth=token)
    resp = client.file_uploads.create()
    upload_id = resp["id"]
    upload_url = resp["upload_url"]

    r = httpx.post(
        upload_url,
        headers={
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2022-06-28",
        },
        files={"file": (filename, png_bytes, "image/png")},
        timeout=30,
    )
    r.raise_for_status()
    return upload_id


# ─────────────────────────────────────────────────────────
# Notion 블록 구성
# ─────────────────────────────────────────────────────────
def _rt(text: str, bold: bool = False, link: str | None = None, color: str = "default") -> dict:
    """Rich-text fragment helper."""
    obj = {
        "type": "text",
        "text": {"content": text, "link": {"url": link} if link else None},
        "annotations": {
            "bold": bold, "italic": False, "strikethrough": False,
            "underline": False, "code": False, "color": color,
        },
    }
    return obj


def _heading2(text: str) -> dict:
    return {
        "type": "heading_2",
        "heading_2": {"rich_text": [_rt(text, bold=True)]},
    }


def _heading3(text: str) -> dict:
    return {
        "type": "heading_3",
        "heading_3": {"rich_text": [_rt(text, bold=True)]},
    }


def _todo(text: str, checked: bool = False) -> dict:
    return {
        "type": "to_do",
        "to_do": {"rich_text": [_rt(text)], "checked": checked},
    }


def _paragraph(rich_items: list[dict]) -> dict:
    return {"type": "paragraph", "paragraph": {"rich_text": rich_items}}


def _divider() -> dict:
    return {"type": "divider", "divider": {}}


def _bullet(rich_items: list[dict]) -> dict:
    return {"type": "bulleted_list_item", "bulleted_list_item": {"rich_text": rich_items}}


def _callout(text: str, emoji: str = "⚠️", color: str = "yellow_background") -> dict:
    return {
        "type": "callout",
        "callout": {
            "rich_text": [_rt(text)],
            "icon": {"type": "emoji", "emoji": emoji},
            "color": color,
        },
    }


def _embed(url: str) -> dict:
    return {"type": "embed", "embed": {"url": url}}


def _image_uploaded(upload_id: str) -> dict:
    return {
        "type": "image",
        "image": {
            "type": "file_upload",
            "file_upload": {"id": upload_id},
        },
    }


def build_blocks(
    row: pd.Series,
    centroid: pd.DataFrame,
    clinics: pd.DataFrame,
    map_upload_id: str | None = None,
) -> list[dict]:
    """한 행정동 페이지의 본문 블록 리스트 생성.

    map_upload_id: PNG 업로드된 file_upload_id. 있으면 이미지 블록 삽입.
    """
    adm_cd = str(row["adm_cd"])
    cent_row = centroid[centroid["adm_cd"].astype(str) == adm_cd].iloc[0]
    lat = float(cent_row["lat"])
    lon = float(cent_row["lon"])

    # 링크 URL들
    google_embed = f"https://www.google.com/maps?q={lat},{lon}&z=15&output=embed"
    google_link = f"https://www.google.com/maps/@{lat},{lon},15z"
    kakao_link = f"https://map.kakao.com/link/map/{row['adm_nm']},{lat},{lon}"
    naver_link = f"https://map.naver.com/p?c={lon},{lat},15,0,0,0,dh"

    blocks: list[dict] = []

    # ─── 1. 위치 ───────────────────────────────────────
    blocks.append(_heading2("📍 위치"))
    station_info = ""
    if pd.notna(row.get("nearest_station")) and pd.notna(row.get("station_dist_m")):
        station_info = f"  ·  최근접역: {row['nearest_station']} ({int(row['station_dist_m'])}m)"
    blocks.append(_paragraph([
        _rt(f"중심점 좌표: "),
        _rt(f"{lat:.5f}, {lon:.5f}", bold=True),
        _rt(station_info),
    ]))
    blocks.append(_paragraph([
        _rt("🗺 ", bold=True),
        _rt("Google Maps", link=google_link),
        _rt("  ·  "),
        _rt("카카오맵", link=kakao_link),
        _rt("  ·  "),
        _rt("네이버지도", link=naver_link),
    ]))
    blocks.append(_embed(google_embed))

    # 정적 PNG 지도 (의원 핀 포함)
    if map_upload_id:
        blocks.append(_image_uploaded(map_upload_id))

    blocks.append(_divider())

    # ─── 2. 반경 의원 리스트 ───────────────────────────
    nearby = nearby_clinics(row, centroid, clinics)
    n_found = len(nearby)
    blocks.append(_heading2(f"🏥 반경 {RADIUS_M//1000}km 의원 ({n_found}개)"))
    if n_found == 0:
        blocks.append(_callout("반경 1km 내 의원 없음 — 의료사막 의심 or 중심점이 상권 밖",
                               emoji="🏜", color="red_background"))
    else:
        # 상단 설명
        n_gi = int(nearby["is_gi"].sum()) if "is_gi" in nearby.columns else 0
        blocks.append(_paragraph([
            _rt(f"총 {n_found}개"),
            _rt(f"  ·  소화기 태깅 ⭐ {n_gi}개", bold=(n_gi > 0)),
            _rt(f"  ·  거리 오름차순"),
        ]))
        # 각 의원을 bullet item으로
        for _, cl in nearby.iterrows():
            is_gi = bool(cl.get("is_gi", False))
            marker = "⭐ " if is_gi else ""
            name = str(cl.get("yadmNm", ""))
            kind = str(cl.get("clCdNm", ""))  # 병원·의원·한의원 등
            dist = int(cl.get("dist_m", 0))
            addr = str(cl.get("addr", "") or "")
            tel = str(cl.get("telno", "") or "")
            drs = cl.get("drTotCnt")
            estb = str(cl.get("estbDd", "") or "")
            estb_year = estb[:4] if len(estb) >= 4 and estb.isdigit() else ""

            # 의원 좌표 (Kakao Maps 딥링크용)
            try:
                cl_lat = float(cl.get("YPos"))
                cl_lon = float(cl.get("XPos"))
                kakao_marker = f"https://map.kakao.com/link/map/{name},{cl_lat},{cl_lon}"
            except Exception:
                kakao_marker = None

            # Line 1: 거리 · 이름(링크) · 종별 · 의사수
            line1 = [
                _rt(f"{dist:>4}m", bold=True),
                _rt(f"  {marker}"),
                _rt(name, bold=is_gi, link=kakao_marker),
                _rt(f"  ({kind}"),
            ]
            if isinstance(drs, (int, float)) and drs > 0:
                line1.append(_rt(f", 의사 {int(drs)}명"))
            if estb_year:
                line1.append(_rt(f", {estb_year} 개원"))
            line1.append(_rt(")"))

            blocks.append(_bullet(line1))
            # Line 2 (sub-bullet): 주소 + 전화
            sub_items = []
            if addr:
                sub_items.append(_rt(f"📍 {addr}"))
            if tel:
                sub_items.append(_rt(f"  ·  ☎ {tel}"))
            if sub_items:
                # bullet 하위로 넣으려면 children 설정 필요. 단순화: paragraph로 indent 없이.
                # Notion API에서 하위는 append시 관리. 여기선 sub-paragraph로 처리.
                blocks.append({
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [_rt("    ")] + sub_items,
                    },
                })

    blocks.append(_divider())

    # ─── 3. 상세 점수 ───────────────────────────────────
    blocks.append(_heading2("📊 상세 점수"))
    blocks.append(_bullet([
        _rt("총점: ", bold=True),
        _rt(f"{float(row['score']):.4f}"),
        _rt(f"  (C={float(row['c_norm']):.2f}, "),
        _rt(f"P={float(row['p_norm']):.2f}, "),
        _rt(f"T={float(row['t_norm']):.2f})"),
    ]))
    blocks.append(_bullet([
        _rt("40+ 인구: ", bold=True),
        _rt(f"{int(row['pop_40plus']):,}명"),
        _rt(f"  ·  총 인구 {int(row['pop_total']):,}명"),
        _rt(f"  ·  비율 {float(row['ratio_40plus'])*100:.1f}%"),
    ]))
    blocks.append(_bullet([
        _rt("경쟁: ", bold=True),
        _rt(f"동내 의원 {int(row['n_clinic'])}개  ·  "),
        _rt(f"500m {int(row.get('n_clinic_500m', row.get('n_within_radius', 0)))}개  ·  "),
        _rt(f"1km {int(row.get('n_clinic_1km', 0))}개  ·  "),
        _rt(f"2km {int(row.get('n_clinic_2km', 0))}개"),
    ]))
    blocks.append(_bullet([
        _rt("통근: ", bold=True),
        _rt(f"자차 {int(row['t_raw'])}분"),
    ]))
    if pd.notna(row.get("nearest_station")):
        blocks.append(_bullet([
            _rt("역세권: ", bold=True),
            _rt(f"{row['nearest_station']}"),
            _rt(f" ({int(row['station_dist_m'])}m)"),
            _rt(f"  ·  그 역 500m 내 의원 {int(row.get('n_clinic_station_500m', 0))}개"),
        ]))

    # ─── 4. 플래그 (있을 때만) ─────────────────────────
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
            blocks.append(_callout(text, emoji=emoji,
                                   color="orange_background" if emoji == "🏜" else "yellow_background"))

    return blocks


# ─────────────────────────────────────────────────────────
# Notion 페이지 업데이트
# ─────────────────────────────────────────────────────────
def _clear_page_content(client: Client, page_id: str) -> int:
    """페이지 기존 블록 전부 아카이브(삭제). 반환: 삭제한 블록 수."""
    deleted = 0
    start_cursor = None
    block_ids: list[str] = []
    while True:
        resp = client.blocks.children.list(
            block_id=page_id, start_cursor=start_cursor, page_size=100,
        )
        for b in resp.get("results", []):
            block_ids.append(b["id"])
        if not resp.get("has_more"):
            break
        start_cursor = resp.get("next_cursor")
    for bid in block_ids:
        client.blocks.delete(block_id=bid)
        deleted += 1
    return deleted


def _append_blocks(client: Client, page_id: str, blocks: list[dict]) -> None:
    """100개씩 쪼개 append."""
    for i in range(0, len(blocks), 100):
        chunk = blocks[i:i + 100]
        client.blocks.children.append(block_id=page_id, children=chunk)


def update_page_detail(
    client: Client,
    token: str,
    page_id: str,
    row: pd.Series,
    centroid: pd.DataFrame,
    clinics: pd.DataFrame,
    dry_run: bool = False,
    with_map: bool = True,
) -> int:
    """한 페이지에 상세 본문 삽입 (기존 내용 삭제 후 재생성).
    반환: append한 블록 수."""
    map_upload_id = None
    if with_map and not dry_run:
        try:
            png = generate_map_png(row, centroid, clinics)
            if png:
                filename = f"map_{row['adm_cd']}_{row['adm_nm'].split(' ')[-1]}.png"
                map_upload_id = upload_png_to_notion(token, png, filename)
        except Exception as e:
            logger.warning("  map PNG 생성/업로드 실패: %s — 텍스트만 진행", e)
    blocks = build_blocks(row, centroid, clinics, map_upload_id=map_upload_id)
    if dry_run:
        return len(blocks)
    _clear_page_content(client, page_id)
    _append_blocks(client, page_id, blocks)
    return len(blocks)


# ─────────────────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────────────────
def run(
    only_rank: int | None = None,
    dry_run: bool = False,
    with_map: bool = True,
) -> dict:
    load_dotenv()
    token = os.environ.get("NOTION_TOKEN")
    ds_id = os.environ.get("NOTION_DS_ID")
    if not token:
        raise RuntimeError(".env의 NOTION_TOKEN 미설정.")
    if not ds_id:
        raise RuntimeError(".env의 NOTION_DS_ID 미설정.")

    client = Client(auth=token)
    top30, centroid, clinics = load_data()
    logger.info("loaded top30=%d, clinics=%d", len(top30), len(clinics))

    # DB 조회해서 동명 → page_id 매핑
    from publishers.notion_sync import _query_existing
    name_to_id = _query_existing(client, ds_id)
    logger.info("existing pages: %d", len(name_to_id))

    processed = 0
    total_blocks = 0
    for _, row in top30.iterrows():
        rank = int(row["rank"])
        if only_rank is not None and rank != only_rank:
            continue
        name = str(row["adm_nm"])
        page_id = name_to_id.get(name)
        if not page_id:
            logger.warning("  [rank %d] %s — Notion 페이지 없음 (skip)", rank, name)
            continue
        n = update_page_detail(
            client, token, page_id, row, centroid, clinics,
            dry_run=dry_run, with_map=with_map,
        )
        logger.info("  [rank %d] %s — %d blocks%s",
                    rank, name, n, " (dry)" if dry_run else " OK")
        processed += 1
        total_blocks += n

    summary = {"pages": processed, "total_blocks": total_blocks, "dry_run": dry_run}
    logger.info("complete: %s", summary)
    return summary


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(description="Top 30 Notion 상세 본문 자동 생성")
    parser.add_argument("--dry-run", action="store_true", help="블록만 만들고 Notion에 안 보냄")
    parser.add_argument("--only", type=int, default=None, help="특정 순위만 (테스트용)")
    parser.add_argument("--no-map", action="store_true", help="정적 PNG 맵 생성 생략")
    args = parser.parse_args()
    run(only_rank=args.only, dry_run=args.dry_run, with_map=not args.no_map)
    return 0


if __name__ == "__main__":
    sys.exit(main())

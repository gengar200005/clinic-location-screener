"""프로젝트 상수. 가중치·임계치·좌표계·경로.

변경 시 docs/PLAN.md §3(스코어링) 업데이트 필수.
"""
from pathlib import Path

# ─── 경로 ───
ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_CLEANED = ROOT / "data" / "cleaned"
DATA_CACHE = ROOT / "data" / "cache"
DATA_SCORED = ROOT / "data" / "scored"
WEB_DATA = ROOT / "web" / "data"

# ─── 스코어 가중치 (합 = 1.0) ───
# 2026-04-19 조정: T=0.2 → 0.1 (T 민감도 분석 결과 16/30 동 교체로 결정적 변수임을 확인).
# T는 임장 우선순위 보조용 tie-breaker로 약화. C·P 동등 0.45.
# PWA 메인에서 사용자가 슬라이더로 동적 재가중 가능 (heatmap.json에 c_norm·p_norm·t_norm 모두 포함).
W_COMPETITION = 0.45
W_POPULATION = 0.45
W_COMMUTE = 0.1

# 경쟁 서브가중치: 밀도 vs 반경
W_COMP_DENSITY = 0.5
W_COMP_RADIUS = 0.5

# 인구 서브가중치: 총인구 vs 40+ 비율
W_POP_TOTAL = 0.6
W_POP_AGE40 = 0.4

# ─── 임계치 ───
COMPETITION_RADIUS_M = 500
MIN_POPULATION = 500          # 미만 동은 스코어링에서 제외 (공단·공원)
COMMUTE_FALLBACK_MIN = 999    # ODSay 응답 실패 시 대입값

# ─── 좌표계 ───
EPSG_WGS84 = 4326    # 저장용 (GeoJSON·Leaflet)
EPSG_KOREA = 5179    # 거리 계산용 (UTM-K)

# ─── 출발지: 이촌1동 주민센터 ───
ORIGIN_LNG = 126.9718
ORIGIN_LAT = 37.5224

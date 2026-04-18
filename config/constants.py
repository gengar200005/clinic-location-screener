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
W_COMPETITION = 0.4
W_POPULATION = 0.4
W_COMMUTE = 0.2

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

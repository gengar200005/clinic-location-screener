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

# 경쟁 서브가중치: 밀도 vs 반경 vs 역세권 페널티
# 2026-04-21 추가: W_COMP_STATION (역 500m 내과 의사 수 보수 가중)
#   배경: 동 centroid가 여러 subcluster mean이라 역세권 밀집이 묻힘.
#         답사 가면 점수 상위권도 역 근처 내과 매우 많음 — 체감 경쟁이 점수보다 강함.
#   안전장치: 0.2× — density·radius 항(각 0.5) 대비 절반 미만으로 보수적 적용.
#         이전 sensitivity 분석에서 centroid 자체를 역으로 옮기면 과잉 페널티(NG)였음.
W_COMP_DENSITY = 0.5
W_COMP_RADIUS = 0.5
W_COMP_STATION = 0.2
# 2026-04-21 추가: W_COMP_SUBCLUSTER (동 내 가장 밀집된 500m disk 내과 의사 수)
#   (c) 안: 1.5km 안 의원 각각을 anchor로 sliding disk → max.
#         (b) 역세권 페널티가 "최근접역" 한 점 기준이라면, 이건 동 내 임의 subcluster 검출.
#         가장 정확하지만 (b)와 페널티 의도 중첩 — ablation 후 활성화.
#   기본 0.0: 컬럼만 출력하고 c_raw 영향 없음. 데스크톱에서 결과 검토 후 0.1~0.2 권장.
W_COMP_SUBCLUSTER = 0.0

# 인구 서브가중치: 총인구 vs 40+ 비율
W_POP_TOTAL = 0.6
W_POP_AGE40 = 0.4

# ─── 임계치 ───
# 2026-04-20: COMPETITION_RADIUS_M 500 → 1500. 변경 이유:
#   (1) 내과 의원 기준 c_raw 적용 시 500m 내 0개 동이 165개로 동률 c_norm 다발
#   (2) P (1.5km 배후) vs C (500m) radius mismatch — 서울 도심 인구 인플레 원인
#   1.5km 통일로 두 문제 동시 해결.
COMPETITION_RADIUS_M = 1500
CATCHMENT_RADIUS_M = 1500     # 배후 상권 반경 (P_raw · density 분모 공통)
MIN_POPULATION = 500          # 미만 동은 스코어링에서 제외 (공단·공원)
COMMUTE_FALLBACK_MIN = 999    # ODSay 응답 실패 시 대입값

# ─── 좌표계 ───
EPSG_WGS84 = 4326    # 저장용 (GeoJSON·Leaflet)
EPSG_KOREA = 5179    # 거리 계산용 (UTM-K)

# ─── 출발지: 이촌1동 주민센터 ───
ORIGIN_LNG = 126.9718
ORIGIN_LAT = 37.5224

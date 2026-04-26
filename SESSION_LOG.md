# Session Log

최근 세션이 상단. 각 세션: 결정 / 검토한 대안 / 다음 세션 할 일 / 미해결.

---

## 2026-04-26 — 중심점 sensitivity·shops 가중 채택

### 결정
- **W_COMP_STATION sensitivity 검증** (`scripts/sensitivity_w_station.py`): W∈[0.1, 0.3] Top30 100% 동일, 극단(W=0.5)도 27/30 유지. 0.2 그대로 유지 (변경 없음).
- **CENTROID_MODE = "shops"** 채택 (ADR-004). 1·2층 상가 평균 좌표를 모든 거리 측정 기준점으로. 인구 가중 mean이 아파트단지·산에 찍히는 문제 + 사용자 우려("중심점은 토지 분류상 상가만 들어올 수 있는 위치여야") 해결.
- 신규 파일: `scoring/centroid_shops_weighted.py`, `data/cache/admin_centroid_shops.parquet` (653동, 모두 shops anchor).
- 신규 함수: `scoring.spatial_join.apply_shops_weighted_centroid` + `apply_centroid_overlay` (mode 분기).
- `data/scored/scores_2026-04-26.parquet` + `top30_2026-04-26.parquet` 새 생성. Top30 25/30 유지, 5 교체.

### 검토한 대안
- W_COMP_STATION sensitivity: 0.0~0.5 6시나리오 ablation. 0.2가 안전 영역 한복판으로 판명.
- 중심점 옵션: pop 유지 / hybrid (0.5·pop + 0.5·shops) / shops 정식 채택. 데이터로 shops 우수성 확인 → 정식.
- shops 데이터 vs 국토부 LURIS 용도지역: LURIS는 무겁고 자동갱신 어려움. 1·2층 상가로 사실상 같은 정보.

### 다음 세션 할 일
- pytest 회귀 확인 (centroid 변경에 따른 fixture 영향).
- 웹 재배포 여부 결정 (PWA heatmap.json + Notion sync). 답사 앞두고 새 Top30 반영할지.
- Kakao Mobility 재호출 — t_raw 좌표 기반 정확도 향상 (다음 cron에서 자동인지 확인).
- 답사 실시 (관악 대학동·노원 중계2·3동 등 신규 진입 동 우선).

### 미해결
- t_raw가 인구 가중 시점 캐시 그대로 — 좌표 1km 이동의 통근시간 영향 ±2분 수준이라 점수 영향 미미 (ADR-004 caveat 명시).
- `centroid_mismatch_flag`·`med_desert_flag` 의미 변화 (이제 shops 중심 기준). PWA 표시·답사 가이드 업데이트 필요할 수도.

---

## 2026-04-22 — 역세권 페널티 반영 재실행

### 결정
- W_COMP_STATION=0.2로 역세권 500m 페널티를 점수에 실제 반영 (커밋 `23b5551`, 브랜치 `claude/resume-clinic-screener-890DX`).
- Top30에 `n_doctors_station_500m_med`(0~10), `n_doctors_subcluster_max_med`(4~17) 컬럼 노출.
- pytest 26/26 그린 → scoring.pipeline → web_export → push.

### 검토한 대안
- subcluster max density는 기본 W=0 (옵션 항만 추가, 점수 영향 X). 데이터 관찰 후 W 값 재결정.

### 다음 세션 할 일
- 역세권 페널티 전/후 Top30 diff로 효과 검증.
- W_COMP_STATION 0.1 / 0.2 / 0.3 sensitivity.
- main 병합 여부 결정 (GH Pages 반영 전제).

### 미해결
- GH Pages 반영은 main 병합 후.

---

## 2026-04-20 — 스코어링 정밀화 + 답사 UX 완성

### 결정
- **A안 catchment 1.5km** P_raw + C density 분모 통일 확정 (`34fc056` → ADR-001).
- **C_raw를 내과 의사 수 가중**, 분모 40+ 환자풀 (`b954f56`).
- **역세권 500m 페널티 항** 추가 (`feac049`, W=0.2 → ADR-003).
- **답사 UX**: 상가 anchor(소상공인진흥공단) + 의원 convex hull + 지도 클릭 1km 의원 필터 + 내과 강조/홈페이지 직링크.
- HANDOFF.md "답사 단계 진입"으로 갱신.

### 검토한 대안
- HIRA 진료과목 코드로 소화기 식별 → 불가 (01 내과 통합). 병원명 "소화기" 키워드 대체.
- 격자(D안) 스코어링 기각 — 답사 대상이 동 단위라 실용성 낮음.

### 다음 세션 할 일
- 역세권 페널티를 점수에 반영할지 확정 (2026-04-22 확정됨).

### 미해결
- R-ONE 임대료/공실률은 시군구 단위라 가치 낮음 — 답사 후 매물 단계에서 재검토.

---

## 2026-04-19 — Post-MVP 1차 (중심점 + Notion UI)

### 결정
- **T 가중치 0.2 → 0.1** (sensitivity 분석 결과 16/30 동 좌우 → ADR-002).
- **WorldPop 인구 가중 중심점** 도입 (centroid_mismatch flag 28→11).
- **Notion 3-zone 마커** (🤖·🧠·✍️) + 답사 리포트 템플릿 + 4 view + DB 속성 확장.
- PWA 가중치 슬라이더 추가.

### 검토한 대안
- Notion 페이지 마커 vs 속성 분리 → 마커 택일 (한 페이지에 세 영역 공존).

### 다음 세션 할 일
- A안 catchment 스코어링 (다음 날 실행).

### 미해결
- 72MB WorldPop tif가 git history(`49041e8`)에 잔존 — 용량 영향 미미, 보류.

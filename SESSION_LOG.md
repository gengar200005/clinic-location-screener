# Session Log

최근 세션이 상단. 각 세션: 결정 / 검토한 대안 / 다음 세션 할 일 / 미해결.

---

## 2026-04-27 — GI 의원 페널티 가중 (ADR-005) + estbDd 보류

### 결정
- **estbDd(개원일) 컬럼 도입 보류**. 가설("끓는 시장 vs 정체 시장 식별 → 세대교체 기회 동")을 데이터로 검증한 결과 부정됨. 정체 1순위 후보 노원 월계2동이 현재 score rank 1, 끓는 시장 강남 역삼/대치는 rank 630+ → 끓음/정체 라벨이 score와 약한 역상관. 점수 모델이 이미 인구·경쟁을 옳게 평가 중이라 추가 컬럼은 인지부하만 늘어남. 진단 도구로의 가치는 4분면 해석을 사용자가 매번 해야 해서 약함.
- **`W_GI_MULTIPLIER = 2.0` 채택** (ADR-005, 커밋 `e7b9d0f`). 마스터 콘셉트(소화기+내시경)에 맞춰 GI 후보 의원 의사수에 ×2.0 가중. c_raw의 4개 항(density 분자, 1.5km 반경, 역세권 500m, subcluster max)에 동일 적용.
- **`is_gi` 식별을 의원명 키워드(0건)에서 HIRA 의료장비 데이터셋(`data.go.kr 15051055`) 기반 A304∩A320 보유로 재정의**. ykiho 매칭 98.5%. 위내시경(A304) 보유 81.9% / 대장내시경(A320) 56.1% / 둘 다 56.1% — 56%가 GI 후보로 잡힘.
- **신규 파일**: `scrapers/hira_equipment.py`, `scripts/ablation_gi_weight.py`, `docs/decisions/005-gi-weight.md`, `data/cleaned/clinic_equipment.parquet` (69,819 의료기관, .gitignore).
- **변경**: `config/constants.py` (W_GI_MULTIPLIER + GI_EQUIPMENT_CODE_EGD/COLO), `scoring/spatial_join.py` (is_gi 머지), `scoring/competition.py` (`_weighted_doctors` 헬퍼 + 3개 함수에 gi_multiplier), `scoring/station_metrics.py`, `scoring/pipeline.py` (n_doctors_med_weighted 컬럼 + `--gi-multiplier` CLI), `tests/test_competition.py` (가중 케이스 8개, 17 passed).
- pytest 34/34 그린.

### 검토한 대안
- **estbDd display**: 추가 컬럼만 노출(점수 미반영) → 기각. 사용자가 "잘 이해 안 된다"고 한 것이 신호 — 4분면 해석 framework 안 와닿으면 노출해도 의사결정에 안 쓰임.
- **GI 식별 방식**:
  - HIRA OpenAPI 의원명 키워드 매칭 → "소화기" 0건으로 사용 불가
  - HIRA OpenAPI `MadmDtlInfoService2`(상세정보) endpoint → 사용자 service 미신청 상태 (HTTP 500). 신청 시 1~2시간 대기 필요
  - 홈페이지 키워드 크롤링 → hospUrl coverage 13.4%만 (한국 동네 의원 자체 홈페이지 보유율 낮음 + 폐쇄 도메인)로 fatal
  - 카카오 로컬 API 카테고리 → 미시도 (HIRA 데이터셋 더 직접적)
  - **HIRA 의료장비 상세 현황 CSV (15051055)** 채택 — API 신청 불필요, 712k 행, ykiho 매칭 가능
- **GI 정의 강화 (옵션 B)**: A304∩A320 + (ERCP A316 ∪ 에스상 결장경 A305 ∪ 내시경 ≥ 2대)로 진짜 시술량 多 의원 좁힘 → 후속 검토 (이번엔 옵션 A 채택)
- **W ablation**: {1.0, 1.5, 2.0, 2.5}. W=1.0(0교체), W=1.5(1), **W=2.0(1)**, W=2.5(3). W=2.0이 직관과 일치 + ranking 안정성 균형.
- **Claude in Chrome으로 data.go.kr 자동 다운로드 시도** → 권한 prompt + 로그인 필요로 connection 끊김. 사용자가 직접 다운로드 (5분).

### 다음 세션 할 일
- **detail 페이지 노출**: `publishers/notion_detail.py` + `publishers/web_export.py`에 `is_gi`/`n_doctors_med_weighted`/`has_egd`/`has_colo` 표시 추가. ADR-005 caveat 3에서 명시한 "사용자가 c_raw 절대값으로 직접 비교"를 가능하게.
- **답사 실시**. 이전 세션 미완: 관악 대학동·노원 중계2·3동·마포 상암동·성북 정릉2동·부천 신흥동.
- **W=2.0 답사 검증 후 fine-tune**: 답사·상담 데이터 쌓이면 0.1 단위로 1.8~2.2 범위 조정.
- 답사 후 `W_COMP_SUBCLUSTER` 활성화 여부 결정 (이전 세션 이월).

### 미해결
- A304∩A320이 진짜 시술량 큰 GI 전문 의원과 일치하지 않음. 검진용 1대씩 굴리는 일반 내과 + 시술량 多 의원이 같이 잡힘. 진짜 차별화는 시술량·평판인데 데이터로 측정 불가 — 임대료·HIRA 비급여 진료비 정보 도입 시 fine-tune 가능.
- W=2.0이 percentile 정규화 특성상 Top30 ranking 변동 작음 (1개 교체뿐). 진짜 시그널은 c_raw 절대값과 n_doctors_med_weighted — detail 페이지 노출 전엔 사용자가 직접 확인 어려움.
- estbDd 작업은 사용자 추후 결정. 데이터(estbDd) 자체는 hira_*.parquet에 이미 수집되어 있음 (별도 스크래퍼 불필요). 진행 시 ad-hoc 스크립트로 충분.
- HIRA 의료장비 데이터셋 갱신 주기 = 연 1회 (매년 12월 31일 기준 익년 2월 공개). 신규 의원 GI 분류에 약 1~14개월 지연.

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

### 배포 (세션 말미)
- pytest 26/26 그린.
- `publishers/web_export` → web/data/* (heatmap·boundaries·detail·narrow_lists·all_clinics) 갱신.
- `publishers/notion_sync` → Notion DB sync (created 5, updated 25, dropped 50).
- `publishers/notion_embed` → 페이지 본문 30개 자동 영역 갱신 (full 28, partial 2 — 답사 기록 보존).
- 커밋 `945b365` push → GH Pages 자동 배포 (https://gengar200005.github.io/clinic-location-screener/).

### 사후 조정 (세션 말미 추가)
- **PWA root URL이 README.md를 렌더링하던 문제 해결** (커밋 `3d9eb6f`). root에 `index.html` redirect 추가 → root URL 그대로 두고 자동으로 `/web/` PWA로 이동. 진짜 PWA URL은 `https://gengar200005.github.io/clinic-location-screener/web/`.
- **네이버 부동산 링크 전세→월세 통일** (커밋 `ce0a393`, `5724429`). 상가 매물은 거의 월세라 전세(B1) 필터로는 빈 결과. PWA(`publishers/web_export`)와 답사 카드(`scripts/gen_survey_cards`) 둘 다 `new.land.naver.com/offices?b=B2&e=RETAIL` 패턴으로 통일. 50개 detail JSON 재생성.

### 다음 세션 할 일
- Kakao Mobility 재호출 — t_raw 좌표 기반 정확도 향상 (다음 cron에서 자동 갱신 여부 확인). 또는 출근시간 정확도 검증 시 TMAP 재검토 (사용자 직감 확인).
- 답사 실시 (관악 대학동·노원 중계2·3동·마포 상암동·성북 정릉2동·부천 신흥동 신규 진입 동 우선).
- 답사 후 W_COMP_SUBCLUSTER 활성화 여부 결정 (데이터 관찰 후).

### 미해결
- t_raw가 인구 가중 시점 캐시 그대로 — 좌표 1km 이동의 통근시간 영향 ±2분 수준이라 점수 영향 미미 (ADR-004 caveat 명시).
- `centroid_mismatch_flag`·`med_desert_flag` 의미 변화 (이제 shops 중심 기준). PWA 표시·답사 가이드 업데이트 필요할 수도.
- 사용자 기억상 "출근시간은 TMAP이 더 정확"이라 판단했으나 실제 코드/문서는 Kakao primary (TMAP은 무료 키 시간대 예측 미지원으로 deprecated). 추후 실체감 확인 후 재결정.

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

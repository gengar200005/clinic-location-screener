# 서울+경기 내과 개원 입지 스크리너 — 최종 구현 플랜

> **작성일**: 2026-04-19
> **최종 목표**: 8주차에 Top 30이 매주 자동 갱신되어 PWA·Notion에 반영

---

## Context — 왜 이 프로젝트인가

서울 이촌동 거주 40대 초반 소화기내과 전문의가 **2027년 5월 개원**을 목표로 한다. 현재 후보지 0개, 완전 탐색 단계. 서울 424개 + 경기 인접 주요 시 9개(성남·고양·부천·안양·광명·하남·과천·구리·남양주) 약 **700~800개 행정동**(실제 **653개** 확인)을 대상으로 데이터 기반 자동 스크리닝을 통해 **Top 30 행정동**을 걸러낸다. 실제 임장은 Top 30에만 투입하여 시간·교통비를 최소화하고, 결정 근거를 정량화해 나중에 "왜 이 동을 선택했는가"를 설명 가능하게 한다.

**가중치 (확정)**: 경쟁 40% · 인구/세대 40% · 통근편의 20%

---

## 사용자 결정 사항 (Phase 1~3 완료)

| 항목 | 결정 |
|------|------|
| 대중교통 API | **ODSay** (월 5,000건 무료, 이촌역 기준) |
| 경기 범위 | 서울 인접 9개 시 (성남·고양·부천·안양·광명·하남·과천·구리·남양주) |
| HIRA 계정 | **개발 계정으로 시작**(1만 건/일), 운영 계정(10만 건/일) 병렬 신청 |
| 배포 | 신규 GitHub repo + PWA `gengar200005.github.io/clinic-location-screener/` |
| Notion | 신규 DB 생성 (Top 30 전용) |
| 실패 알림·SWOT | MVP 제외 (Post-MVP 9주차 이후) |

---

## 설계 원칙

1. **Idempotent 배치** — 같은 날 여러 번 실행해도 결과 동일
2. **Layered Lake** — `raw → cleaned → scored` 3단, 중간 복구 가능
3. **캐시 커밋 원칙** — 불변 축(행정동 중심점, ODSay 결과)은 영구 커밋
4. **정렬 = 의사결정 순서** — 경쟁(HIRA) → 인구(행안부) → 통근(ODSay) 순 비용 증가

---

## 폴더 구조

```
config/        constants.py, target_regions.yaml, hira_codes.yaml
scrapers/      admin_boundary, hira_clinic, population_*, subway_stations, odsay_transit
scoring/       spatial_join, competition, population, commute, normalize, weighted_sum, pipeline
publishers/    notion_sync, web_export
data/
  raw/         날짜 버전 parquet (.gitignore)
  cleaned/     재생성 가능 (.gitignore)
  cache/       ODSay 영구 (커밋)
  scored/      스코어 히스토리 (커밋)
web/
  index.html, manifest.json, sw.js, js/, css/
  data/        top30.json, heatmap.json, boundaries.geojson
.github/workflows/  weekly_pipeline.yml, boundary_refresh.yml, deploy_pages.yml
tests/         pytest
docs/          PLAN.md, DATA_SOURCES.md, SCORING.md, RUNBOOK.md
```

---

## 데이터 파이프라인 흐름

```
admin_boundary → admin_centroid.parquet
     │
     ├─► hira_clinic → clinics raw
     │        └─► spatial_join (within) → clinics_by_dong
     ├─► population_mois/kosis → pop_by_dong
     └─► odsay_transit (캐시) → commute_by_dong
                        │
                        ▼
            scoring/pipeline (merge → normalize → weighted_sum)
                        │
                        ▼
                scores_YYYY-MM-DD.parquet
                        │
         ┌──────────────┴──────────────┐
         ▼                             ▼
    notion_sync                   web_export
```

### 부분 재실행

- 각 스크레이퍼 `--force` 플래그, 기본은 skip-if-exists
- HIRA만 실패: `python -m scrapers.hira_clinic --force --date YYYY-MM-DD` → `python -m scoring.pipeline` 재실행
- ODSay 쿼터 소진: 캐시 보존한 채 실행 → 신규만 호출

---

## 스코어링 알고리즘

### 경쟁 지표 C (밀도 + 반경 혼합)

```
C_raw = 0.5 · (N_clinic / (P / 10,000))  +  0.5 · N_clinic^500m
```
- N_clinic: 동 내 내과+소화기내과 의원 수
- P: 동 주민등록 인구
- N_clinic^500m: 중심점 반경 500m 내 의원 수 (EPSG:5179 변환 후 euclidean)

⚠️ 0.5:0.5는 임의. 8주차 이후 이상점 보며 조정.

### 인구 지표 P

```
P_raw = 0.6 · P_total + 0.4 · HouseholdRatio_40+
```
근거: 총인구만 쓰면 청년 1인가구 많은 동(신림) 과평가.

### 통근 지표 T

```
T_raw = CommuteMinutes(이촌역 → 동중심)
```

### 정규화: **percentile rank**

```
rank_pct(x_i) = #{j : x_j ≤ x_i} / N
C_norm = 1 − rank_pct(C_raw)   # 낮을수록 좋음
P_norm = rank_pct(P_raw)        # 높을수록 좋음
T_norm = 1 − rank_pct(T_raw)    # 낮을수록 좋음
```

근거: 이상치 강건, 해석 용이, 분포 가정 없음.

### 최종

```
Score_i = 0.4 · C_norm + 0.4 · P_norm + 0.2 · T_norm
Top 30 = 내림차순 상위 30
```

### 이상치 처리

| 케이스 | 처리 |
|--------|------|
| 인구 < 500 | 스코어링 제외 (공단·공원) |
| 의원 0개 | 그대로 (블루오션, 유효) |
| ODSay 실패 | T_raw=999 최하위 + 재시도 목록 |

---

## GitHub Actions

- **스케줄**: 주 1회, 토 03:00 KST = 금 18:00 UTC
- **Job DAG**: scrape → score → publish (각 job 실패 시 하류 중단)
- **실행시간**: 평상시 5~6분, 첫 실행 ~18분 (ODSay cold cache)
- **Secrets**: HIRA_KEY, ODSAY_KEY, KOSIS_KEY, NOTION_TOKEN, NOTION_DB_ID, (선택) KAKAO_KEY, MOIS_KEY

---

## PWA MVP

- **필수**: 전체 동 히트맵 + Top 30 테이블 + 동 클릭 상세 + Service Worker
- **라이브러리**: **Leaflet** (무료·OSM 타일·모바일 터치·42KB)
- **JSON fetch**: `raw.githubusercontent.com/.../web/data/*` (pokemon-tcg-guide 패턴)
- **경계 단순화**: mapshaper 0.5% tolerance → 1~2MB

---

## 리스크 대응

| 리스크 | 대응 |
|--------|------|
| HIRA API 장애 | data.go.kr 월간 CSV 폴백, `--mode api\|csv` |
| ODSay 월 5,000 초과 | 영구 캐시 (첫 실행만 700건, 이후 0~5) |
| 임대료 부재 | Post-MVP Top 30에만 국토부 실거래가 2차 필터 |
| HIRA 매출·환자수 미제공 | "최근 12개월 신규개원 N개" 대리지표 → Notion 상세 표시 |

---

## 마일스톤

### ✅ 1주차: 인프라 + 행정동 경계
- GitHub repo · venv · requirements · config
- `scrapers/admin_boundary.py` → 653개 동 GeoJSON (34.6MB)
- `scoring/spatial_join.py::build_admin_centroid` → 중심점 parquet

### ✅ 2주차: HIRA 수집 + 공간조인
- HIRA 개발 계정 (1만 건/일)
- `scrapers/hira_clinic.py` 페이징·XML 파싱·재시도
- 9,564 의원 수집 · 6,669개가 대상 653동 내부
- Top: 역삼1동(59), 분당1동(47), 삼성동(44) — 강남·분당 밀집 합리적

### ⬜ 3주차: 인구·세대 + 경쟁/인구 점수
- **[본인] KOSIS API 키 발급**
- `scrapers/population_mois.py`, `population_kosis.py`
- `scoring/competition.py`, `population.py`, `normalize.py`
- 검증: 상위/하위 10 동 직관 비교

### ⬜ 4주차: ODSay + 가중합
- **[본인] ODSay 계정 + API 키**
- `scrapers/odsay_transit.py` — **캐시 로직 필수**
- `scoring/commute.py`, `weighted_sum.py`, `pipeline.py`

### ⬜ 5주차: Notion DB 연동
- **[본인] Notion DB 생성 + 통합 토큰**
- Top 30 DB 스키마: 동명/총점/경쟁/인구/통근/의원수/총인구/이촌소요/메모/임장상태/업데이트일
- `publishers/notion_sync.py`

### ✅ 6주차: GitHub Actions 배치화
- Secrets 7개 등록 (HIRA·KOSIS·KAKAO·ODSAY·NOTION_TOKEN·NOTION_DB_ID·NOTION_DS_ID)
- `weekly_pipeline.yml` single-job 15-step DAG (cron 토 03:00 KST + workflow_dispatch)
- 첫 검증 통과: 2026-04-19 12:17 UTC, 4분 7초, 캐시 100% hit
- Workflow permission "read+write" 부여, commit-back은 변경 시에만 (idempotent)
- 발견한 gotcha: `data/raw/admin_boundary` 가 .gitignore라 runner마다 GeoJSON 재다운로드 필요 (~5초)

### ⬜ 7주차: PWA
- **[본인] GitHub Pages 활성화 (`main /web`)**
- `publishers/web_export.py`
- `web/index.html` Leaflet + Top 30 + 상세 패널
- `manifest.json`, `sw.js`

### ⬜ 8주차: 안정화
- `tests/` 최소 케이스
- `docs/DATA_SOURCES.md`, `SCORING.md`, `RUNBOOK.md`
- 2회 연속 배치 성공 확인
- 가중치 민감도 (선택)

### ⬜ 9주차+ (Post-MVP)
- 임대료 2차 필터 (국토부 실거래가)
- Claude API 주간 SWOT (Top 10)
- 실패 알림 (Slack/Discord)
- HIRA 운영 계정 승인 시 병렬 수집

---

## 되돌리기 어려운 결정

1. **Layered Lake + parquet 날짜 버전** — DB로 바꾸면 재작성
2. **Percentile rank 정규화** — 바꾸면 히스토리 재계산 필수
3. **가중치 40/40/20** — Post-MVP 슬라이더 위해 `heatmap.json`에 raw + normalized 둘 다 포함
4. **ODSay 캐시 영구 커밋** — 삭제 시 한도 위험. `.gitignore`에 **절대** 포함 금지
5. **EPSG:4326 저장 / 5179 계산** — 역방향 불가

---

## 검증

| 수준 | 방법 |
|------|------|
| 단위 | `pytest tests/` 정규화·공간조인 |
| 통합 | `python -m scoring.pipeline --date YYYY-MM-DD` → Top 30 parquet |
| E2E | Actions `workflow_dispatch` → Notion 30행 + PWA 렌더 |
| 정성 | Top 30 중 익숙한 동 포함, 모르는 상위 동이 점수로 설명 가능 |
| 모바일 | iPhone Safari 홈화면 + 오프라인 |

---

## ⚠️ 근거 불충분 (착수 전 확인)

- **HIRA 진료과목 코드 01=내과 (확정됨, 2주차 검증 완료). 소화기내과 별도 코드 없음 → 병원명 `is_gi` 태깅**
- **HIRA 시도코드 경기=310000 (확정됨, 행안부 법정동 코드와 상이). 서울=110000**
- 0.5:0.5 경쟁 서브가중치 임의 — 8주차 재평가
- 대리지표(신규개원) vs 매출 상관 불명 — 3개월 운영 후 임장 주관평가와 비교
- 경기 9시 확정 리스트 — 수원·용인·김포 추가 여부 재검토 가능
- 행정동 신/구 코드 매핑 — 2006년 이후 변경내역 수작업 확인

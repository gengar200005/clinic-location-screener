# CLAUDE.md

웹/데스크톱 Claude Code 세션이 공유하는 프로젝트 컨텍스트.

## 프로젝트

**clinic-location-screener** — 서울+경기 내과 개원 입지 자동 스크리너. 서울 424 + 경기 인접 9개 시 = 약 **653개 행정동**을 대상으로 Top 30 후보지를 주간 자동 갱신.

**최종 목표 (2027-05 개원)**: PWA 히트맵 + Notion DB (Top 30) + GitHub Actions 주간 배치.

## 현재 상태 (2026-04-26 업데이트)

**Phase 6 — 중심점 1·2층 상가 가중 채택 + 배포 완료** (ADR-004). `CENTROID_MODE="shops"` 정식 적용. 인구 가중 mean이 아파트단지·산에 찍히는 문제 해결, 토지 분류상 개원 가능 위치만 anchor. `scores_2026-04-26.parquet` 생성 + PWA·Notion 모두 새 Top30 반영 완료 (커밋 `945b365`). **답사 실시 대기**.

## 최근 세션 (자세한 건 [SESSION_LOG.md](SESSION_LOG.md))

- 2026-04-26: W_STATION sensitivity 검증 + 1·2층 상가 가중 중심점 채택 (ADR-004)
- 2026-04-22: 역세권 페널티 반영 재실행 + 재배포
- 2026-04-20: A안 catchment 확정 + 답사 UX 완성 + 역세권 페널티 항 추가
- 2026-04-19: Post-MVP 1차 (T 0.2→0.1 + WorldPop centroid + Notion 3-zone)

## 최근 주요 결정 (자세한 건 [docs/decisions/](docs/decisions/))

- [ADR-001] catchment 1.5km 기반 P_raw + C density 분모 통일 (2026-04-20)
- [ADR-002] T 가중치 0.2 → 0.1 (2026-04-19)
- [ADR-003] 역세권 500m 경쟁 페널티 W_COMP_STATION=0.2 (2026-04-20 추가, 2026-04-22 반영)
- [ADR-004] CENTROID_MODE="shops" — 1·2층 상가 평균 좌표를 모든 거리 측정 기준점으로 (2026-04-26)

## 진행 중 이슈

- W_COMP_STATION sensitivity 검증 완료 (2026-04-26): W∈[0.1, 0.3] 구간 Top30 동일 → 0.2 유지. Stable core 25/30. 상세는 `scripts/sensitivity_w_station.py`.
- t_raw — Kakao 캐시가 다음 cron에서 새 shops 좌표로 자동 갱신되는지 확인 필요. 또는 출근시간 정확도 의문 시 TMAP 재검토 (현재 deprecated, 무료 키 시간대 예측 미지원).
- **답사 실시 대기.** 신규 진입 동 우선: 관악 대학동(두 클러스터·인구↔상가 1049m), 노원 중계2·3동, 마포 상암동, 성북 정릉2동, 부천 신흥동(의료사막 패턴).
- 답사 후 W_COMP_SUBCLUSTER 활성화 여부 결정.

## 가중치 (2026-04-19 조정, docs/SCORING.md와 동기화)

```
Score = 0.45 · C_norm + 0.45 · P_norm + 0.1 · T_norm
  C = 경쟁 (1 − percentile_rank)     ← 낮을수록 좋음
  P = 40+ 인구 (percentile_rank)     ← 소화기내과 유효 환자풀
  T = 이촌역 자차분 (1 − percentile_rank, Kakao Mobility)
```

초안 0.4/0.4/0.2에서 조정: T sensitivity 분석 결과 16/30 동 좌우하는 결정적 변수로 드러나 약화. PWA 메인의 슬라이더로 사용자 동적 재가중 가능.

행정동 중심점은 **WorldPop 100m 격자 인구 가중** 좌표 사용 (기하 중심점이 산·하천에 찍히는 centroid_mismatch 해결). docs/SCORING.md 참조.

정규화는 **percentile rank 고정**. 변경 시 히스토리 전체 재계산 필요.

## 폴더 구조

```
config/       상수·대상지역·HIRA 코드 매핑
scrapers/     외부 API 수집 (admin_boundary, hira_clinic, population_*, odsay_transit)
scoring/      공간조인·정규화·가중합 (pipeline.py가 orchestrator)
publishers/   Notion sync, PWA JSON export
data/
  raw/        외부 API 응답 (날짜 버전, .gitignore)
  cleaned/    재생성 가능 (.gitignore)
  cache/      ODSay 영구 캐시 — 커밋 필수 (삭제 금지)
  scored/     스코어 히스토리 (커밋)
web/          PWA (Leaflet 히트맵 + Top 30 테이블)
.github/workflows/  주간 배치 (토 03:00 KST)
docs/PLAN.md  상세 설계 (이 파일보다 깊은 설명)
```

## 환경 설정

```bash
python -m venv .venv
.venv\Scripts\activate           # Windows
pip install -r requirements.txt
cp .env.example .env              # 키 채우기
```

필수 Python **3.11** (geopandas·fiona 호환성). Windows pip로 설치 가능 확인됨.

## API·환경 변수 (전부 발급·등록 완료)

| 변수 | 용도 |
|------|------|
| `HIRA_KEY` | data.go.kr 병원정보서비스 — 주력 의원 수집 |
| `KOSIS_KEY` | kosis.kr — 40+ 인구 |
| `KAKAO_KEY` | Kakao Mobility — 자차 통근 (T 주력) |
| `ODSAY_KEY` | odsay.com — 대중교통 (보조 display) |
| `NOTION_TOKEN`, `NOTION_DB_ID`, `NOTION_DS_ID` | Notion Top 30 DB sync |

모두 `.env` + GitHub Secrets 등록 완료. 신규 발급 없어도 운영 가능.

## 중요 기술 결정 (엎기 어려운 것)

1. **좌표계**: 저장 `EPSG:4326` (WGS84 · Leaflet용), 거리 계산 시에만 `EPSG:5179` (UTM-K) 변환. 역방향 금지.
2. **HIRA 시도코드는 행정안전부 표준과 다름**: 서울=`110000`(공통), **경기=`310000`** (행안부는 410000이나 HIRA에선 세종). `scrapers/hira_clinic.py`에 하드코딩.
3. **HIRA API 응답은 XML**: `_type=json` 미지원. `xml.etree.ElementTree` 파싱.
4. **HIRA 파라미터 이름은 소문자**: `serviceKey` (대문자 S는 401 Unauthorized).
5. **HIRA 진료과목 코드**: 내과=`01` (통합). 소화기내과 별도 코드 없음 — 병원명에 "소화기" 포함 여부로 `is_gi` 컬럼 태깅.
6. **대상 지역 필터**: `sggnm.str.startswith(tuple(target_list))`. 성남·고양·부천·안양은 구가 있어 `"성남시 분당구"` 형식이므로 exact match 안 됨.
7. **공간조인 기준**: 행정동 폴리곤 `within` (GeoPandas sjoin predicate).
8. **경쟁 지표 서브가중치**: 0.5 (밀도: 1만명당) + 0.5 (반경 500m 내). 8주차 이후 조정 예정.
9. **ODSay 캐시 영구 누적**: 행정동 중심점은 거의 불변 → 첫 실행만 ~700건 호출, 이후 0~5건. `data/cache/odsay_ichon.parquet`는 `.gitignore`에 **절대 포함 금지**.

## 실행 명령

자동 운영은 GH Actions cron이 토 03:00 KST에 수행. 수동 실행은 docs/RUNBOOK.md 참조.

브리핑 수동 주입: `python -m publishers.claude_briefing --input briefings.yaml`

## 진행 상태 (MVP 8주차 모두 완료)

1~8주차: 데이터 수집 · 공간조인 · 점수 · Notion sync · GH Actions cron · PWA · tests · docs 모두 커밋.

**Post-MVP (진행 중)**:
- ✅ T 가중치 0.2→0.1 조정 (민감도 분석 근거)
- ✅ PWA 가중치 슬라이더
- ✅ WorldPop 인구 가중 중심점 (centroid_mismatch 보정)
- ✅ Notion UI 개편 (4 view + 페이지 3-zone 마커 + 답사 리포트 템플릿 + DB 속성 확장)
- ⬜ 임대료 2차 필터 / 실패 알림 / HIRA 운영 계정 등

## 🧠 Claude 브리핑 트리거 (사용자가 매주 토요일 직접 지시)

**사용자 지시 예시**: "브리핑 업데이트해줘" / "Top 30 brief 갱신"

**Claude Code 세션에서의 절차** (이 문서가 세션 지시서):

1. **최신 Top 30 로드**: `data/scored/top30_YYYY-MM-DD.parquet` 최신 파일 + `web/data/detail/{adm_cd}.json` 30개.
2. **각 동 분석** (가능하면 `WebSearch` 병행):
   - 점수 breakdown (C/P/T + flag)
   - 동내/반경 의원 현황
   - 인구 구조 (40+ 규모·비율)
   - 지역 이슈: 재건축·재개발·지하철 연장·신규 개원 뉴스 등 (WebSearch)
3. **SWOT + summary + notes 구조 작성**. 각 동 200~400자 수준.
4. **briefings.yaml** 작성 (UTF-8). 예시:
   ```yaml
   generated_at: "2026-04-19"
   briefings:
     "경기도 남양주시 호원동":
       summary: "..."
       swot:
         strengths: ["..."]
         weaknesses: ["..."]
         opportunities: ["..."]
         threats: ["..."]
       notes: ["..."]
   ```
5. **일괄 주입**: `python -m publishers.claude_briefing --input briefings.yaml`
6. 완료 시 사용자에게 "30개 브리핑 업데이트 완료, N개 건너뜀(페이지 없음)" 요약.

**주의**:
- 브리핑은 🧠와 ✍️ 마커 사이만 교체 (답사 기록 불침범)
- WebSearch 결과의 정확성 주의 — 확신 없는 정보엔 "확인 필요" 표기
- 기존 브리핑 덮어쓰기 (diff 없이) — 히스토리 필요하면 사전에 git commit

## 주의 (세션 공통)

- 사용자는 비개발자 성향. **Bash로 가능한 건 직접 실행**, 사용자에게 터미널 명령 넘기지 말 것. 단 destructive 작업(rm -rf, force push 등)은 확인 필요.
- 웹↔데스크톱 이동 시점을 **선제적으로 알림**. 마일스톤 완료·commit 직후가 이동 적기.
- 한국어 응답. 짧고 구체적으로.
- `data/cache/` 절대 삭제 금지.

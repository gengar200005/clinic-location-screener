# 🔄 세션 재개 프롬프트

> PC·환경이 바뀌어도 (데스크톱 Claude Code ↔ 웹 claude.ai) 이 파일 내용을 Claude 대화 시작 시 붙여넣으면 맥락이 복원됩니다.
>
> **마지막 저장**: 2026-04-20 저녁 (답사 단계 진입 — 점수 모델 정밀화 완료 + 상세 UI 인터랙티브)

---

## 복사해서 붙여넣을 프롬프트 (아래 한 덩어리)

```
clinic-location-screener 프로젝트 재개합니다.

repo: github.com/gengar200005/clinic-location-screener
로컬 경로 (데스크톱): C:\Users\sieun\Desktop\IPJI
PWA: https://gengar200005.github.io/clinic-location-screener/web/
Notion Top 30 DB: https://notion.so/258c6d8940b54d6fbd4d900797d5d7b1

먼저 repo에서 다음 파일을 읽어 맥락 파악해주세요:
1. CLAUDE.md — 프로젝트 규약 + 가중치 + 진행 상태 + 🧠 브리핑 트리거 절차
2. docs/PLAN.md — 주차별 마일스톤 + 완료 체크
3. docs/SCORING.md — 현재 공식 (0.45·C + 0.45·P + 0.1·T) + 인구 가중 중심점 + 3-zone 마커

현재 상태 (2026-04-20 저녁 기준 — 답사 단계 진입):

## 점수 모델 v3 (2026-04-20 확정)
공식: Score = 0.45·C_norm + 0.45·P_norm + 0.1·T_norm

C_raw 정의 — 내과 의사 수 / 40+ 환자풀 기준으로 정밀화:
- 분자: **내과 의사 수 합** (병원명에 "내과" 포함, drTotCnt 합)
  · 근거: 1차 의료기관 내과 전문의 90%+ = 소화기. 키워드만으로 충분
  · 1인 의원 vs 5인 그룹의원 구분 (의원 수가 아닌 의사 수 가중)
- 분모: **catchment_pop_40plus** (1.5km 배후 40+ 인구 = 내과 환자풀)
- 반경: **1.5km (P와 통일)** — 이전 500m → 도심 P 인플레 해결, 동률 68→13
- 출력 컬럼: n_clinic (전체 display), n_clinic_med (내과 의원 수),
  n_doctors_med (내과 의사 수 합), n_within_radius_med (1.5km 내과 의원),
  n_doctors_within_radius_med (1.5km 내과 의사 수)

P_raw: catchment_pop_1.5km × ratio_40plus (배후세대 기반 40+ 환자풀)
T_raw: 이촌1동 기준 자차(Kakao). 컷: t_raw > 50분 Top50 export에서 제외

결과:
- Top30 sido: 서울 22 / 경기 8 (40+ 분모 반영 — 노원·강북·도봉 고령 belt 부상)
- Top10: 양천 신월7동, 강북 번3동, 노원 월계2동, 관악 삼성동, 노원 하계2동,
         성북 장위2동, 부천 소사본1동, 부천 도당동, 부천 원미1동, 부천 심곡본동

## 답사 워크플로 UI (상세 페이지)
- Top50 detail JSON (Top30에서 확장)
- 🏬 상가 추정 anchor: 1·2층 상가 cluster 기반 (소상공인진흥공단 상가업소 정보)
  · data/raw/commercial/ — 서울(535k) + 경기(233k) CSV, 1·2층만 320k
  · data/cleaned/shops_by_dong.parquet — 656 동, mean 좌표 + convex hull
  · 의원 cluster는 폴백
- ⭐ pop centroid (점수 모델 기준)와 🏬 anchor 구분
- 보라 점선 폴리곤: 1·2층 상가 convex hull (1종 근생 매물 후보 zone)
- **지도 클릭 → 🎯 anchor + 반경 1km** 우측 의원 리스트 자동 갱신
  · web/data/all_clinics.json (6669개, 1.4MB) 클라이언트 캐시
  · haversine 거리 계산, 내과 강조·정렬·홈페이지 링크 그대로
  · "동 기준 복귀" 버튼
- 내과 의원: 빨간 [내과] 태그 + 좌측 빨간 보더 + 강조 / 비내과: 회색 반투명
- 각 의원: 🌐 홈페이지(254/1899 내과는 hospUrl, 나머지 네이버 검색 폴백) + 📞 전화
- 상단 필터바: 내과만 / 내과 우선 정렬 / 카운트
- 답사 링크바: 카카오맵·네이버지도·네이버부동산(1·2층 상가 anchor 좌표)

## 메인 페이지
- 시도 탭: 전체 / 서울 / 경기
- 상위 N 토글: 30 / 50 / 100
- 기준 탭: 점수 / 통근 / 신도시 / 의료사막 / 저밀도
- 리스트·팝업: 내과 N개(의사 M) / 전체 M 병기

## 답사 산출물 (오프라인 활용)
- docs/SURVEY_CHECKLIST.md — 9 섹션 체크리스트 (출력용)
- data/scored/survey_cards/INDEX.md + 30개 동별 카드 (markdown)
- data/scored/top50_seoul_2026-04-20.csv / top50_gyeonggi_2026-04-20.csv
- data/scored/narrow_top10_2026-04-20.csv — 5개 기준

## 사용 가능 스크립트
- python -m scoring.pipeline — 점수 재계산
- python -m publishers.web_export — 웹 데이터 갱신 (detail + heatmap + boundaries + narrow + all_clinics)
- python -m scrapers.commercial_shops — 상가업소 CSV → shops_by_dong.parquet
- python -m scripts.gen_survey_cards --top 30 — 답사 카드 생성
- python -m scripts.narrow_top10 — 5개 기준 Top10 CSV
- python -m scripts.export_top50_by_sido — 서울/경기 분리 Top50
- python -m scripts.sensitivity_station_centroid — 역세권 centroid 민감도

## 진행중·보류 이슈
- R-ONE 상업용 부동산 임대료(15069766) / 공실률(15069726) — 사용자 결정 대기
  · 상권 구획도(15086933)는 JPG 이미지 → spatial join 불가 (포기)
  · 임대료는 시군구 단위라 가치 낮을 수 있음. 답사 후 매물 단계에서 별도 활용이 더 실용적
- 의료시설 입지 후보 답사 실시 (Top30 서울 belt + 경기 부천 belt)
- 답사 후 피드백 반영한 점수 weighting 재튜닝

어떻게 진행할지 알려주세요:
- "답사 후 피드백 반영" → 방문한 동 체크리스트 결과 공유하면 반영
- "임대료 데이터 받음" → data/raw/rent/에 CSV 넣어주면 통합
- "다음 동 답사 전 카드 재생성" → python -m scripts.gen_survey_cards --top N
- "웹 재배포" → python -m publishers.web_export + git push
- 그 외 새 아이디어 환영
```

---

## 환경 체크리스트 (새 PC·세션 시)

| 항목 | 확인 방법 |
|---|---|
| GitHub 로그인 | `gh auth status` (scopes: repo, workflow) |
| Notion integration | Top 30 DB 페이지 → Connections에 integration 연결됐는지 |
| GH Actions Secrets | `gh secret list --repo gengar200005/clinic-location-screener` — 7개 확인 |
| 로컬 venv (데스크톱) | `C:\Users\sieun\Desktop\IPJI\.venv\Scripts\activate` → `pytest tests/` 19 그린 |
| 웹 세션 | 파일 편집 대신 GitHub MCP / Notion MCP 경유. 로컬 실행 필요 시 데스크톱으로 복귀 |

## 핵심 파일 위치

| 용도 | 경로 |
|---|---|
| 진행 플랜 | `docs/PLAN.md` |
| 스코어 공식 | `docs/SCORING.md` |
| 데이터 출처 | `docs/DATA_SOURCES.md` |
| 운영 매뉴얼 | `docs/RUNBOOK.md` |
| 이 핸드오프 | `docs/HANDOFF.md` (항상 최신 상태 유지) |
| 세션 규약 | `CLAUDE.md` (Claude Code 자동 로드) |

## 크리티컬 금지 사항

- `data/cache/` 삭제 X (ODSay·Kakao·admin_centroid_pop 한도·정확도)
- `data/scored/*.parquet` 강제 덮어쓰기 X (히스토리)
- main force push X (기본은 fast-forward commit)
- `publishers/notion_embed.py`의 `_find_markers` 로직 건드릴 땐 마이그레이션 경로 테스트 필수 — 잘못하면 30페이지 답사 기록 날아감

## 다음 정기 이벤트

- 토 03:00 KST cron — 자동 실행 (sync + embed, 답사 기록 보존)
- 사용자 "브리핑 업데이트" 지시 시 — CLAUDE.md §브리핑 절차대로 진행

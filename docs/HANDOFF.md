# 🔄 세션 재개 프롬프트

> PC·환경이 바뀌어도 (데스크톱 Claude Code ↔ 웹 claude.ai) 이 파일 내용을 Claude 대화 시작 시 붙여넣으면 맥락이 복원됩니다.
>
> **마지막 저장**: 2026-04-20 (A안 catchment 스코어링 코드 구현 완료, 재계산 대기)

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

현재 상태 (2026-04-20 기준):
- MVP 1~8주차 완료 (매주 토 03:00 KST cron 자동 운영)
- Post-MVP: T 가중치 조정, PWA 슬라이더, WorldPop 인구 가중 중심점, Notion UI 3-zone 마커 완료
- 30개 페이지 마이그레이션 완료 (🤖 자동 · 🧠 브리핑 · ✍️ 답사 3 zone)
- **A안 catchment 스코어링 — 코드 구현 완료 (branch: claude/review-handoff-8pMTC)**
  - centroid_pop_weighted.py: 1.5km 반경 WorldPop 합 catchment_pop_1_5km 추가
  - population.py: P_raw = catchment_pop_1_5km × ratio_40plus (폴백: pop_40plus)
  - pipeline.py: density 분모도 catchment로 통일
  - 테스트 21/21 통과. 단, 실제 재계산은 admin_centroid_pop.parquet 갱신 필요
  - **데스크톱에서 해야 할 일**:
    1. `python -m scoring.centroid_pop_weighted` (WorldPop tif 필요, admin_centroid_pop.parquet 재생성 — catchment_pop_1_5km 컬럼 추가)
    2. `python -m scoring.pipeline` (새 점수 생성)
    3. Top 30 확인 → 행신2동 류 catchment 이득 동 랭킹 변화 검증
    4. `python -m publishers.notion_sync --top30 ...` + `python -m publishers.web_export`

다음 합의된 작업: A안 결과 검증 + 민감도 분석 (반경 1.0/1.5/2.0km 비교)

어떻게 진행할지 알려주세요:
- "A안 재계산" → 데스크톱에서 centroid_pop_weighted + pipeline 순서 실행, 결과 비교
- "브리핑 업데이트해줘" → 토요일 정기 브리핑 (CLAUDE.md §Claude 브리핑 트리거 절차 참조)
- "현재 상태 보여줘" → git log + 최근 Actions 실행 결과 요약
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

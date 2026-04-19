# RUNBOOK — 운영·디버깅

> 매주 토요일 03:00 KST GH Actions가 자동 실행. 사람 개입은 실패 시에만.

## 1. 정상 운영 흐름

```
금 18:00 UTC = 토 03:00 KST
└─ .github/workflows/weekly_pipeline.yml 자동 실행
   └─ scrape (HIRA·KOSIS·Kakao·ODSay) → spatial_join → score → publish
   └─ data/cache, data/scored, web/data 변경 시 commit-back
   └─ Notion DB 자동 sync, GitHub Pages 자동 갱신
```

확인:
- Actions 탭: green check
- Notion DB "🎯 Top 30 임장 후보" 30개 페이지의 "업데이트일" 갱신 확인
- PWA https://gengar200005.github.io/clinic-location-screener/web/ 헤더에 새 날짜

## 2. 로컬 실행 (1회 풀 사이클)

```bash
cd C:/Users/sieun/Desktop/IPJI
source .venv/Scripts/activate

# 0. 경계 (없을 때만)
python -m scrapers.admin_boundary
python -m scoring.spatial_join centroid

# 1. 스크래퍼 (각각 idempotent)
python -m scrapers.hira_clinic --force
python -m scrapers.population_kosis --age
python -m scrapers.kakao_car
python -m scrapers.odsay_transit

# 2. 공간조인
python -m scoring.spatial_join join-clinics --clinics data/raw/hira/hira_$(date +%F).parquet

# 3. 스코어
python -m scoring.pipeline

# 4. 퍼블리시
python -m publishers.web_export
python -m publishers.notion_sync
```

전체 첫 실행 ~18분 (ODSay cold cache), 이후 ~4분.

## 3. 디버깅: 실패 단계별

### 3a. HIRA API 장애

증상: `HTTPError 503` 또는 `INVALID_REQUEST_PARAMETER_ERROR`.

```bash
# 키 확인
echo $HIRA_KEY  # 비어 있으면 .env 또는 GH Secrets 점검

# 단일 페이지 테스트
python -m scrapers.hira_clinic --test
```

폴백: data.go.kr 월간 CSV (PLAN §리스크대응). 미구현 — 수동 다운로드 필요.

### 3b. KOSIS 응답 빈 결과

원인: 통계표 갱신 지연 (보통 매월 25일 이후).
대응: 직전 월 캐시 사용 — `data/raw/population/kosis_pop_age_*.parquet` 중 최신만 자동 선택됨.

### 3c. ODSay/Kakao 한도 초과

증상: ODSay `result=-99` 또는 Kakao `429 Too Many Requests`.

```bash
# 캐시 hit 우선 — 신규 호출 0건 가능 여부 확인
python -m scrapers.odsay_transit --max-calls 1
```

캐시 (`data/cache/*.parquet`)는 **절대 삭제 금지**. 한도 위험.

### 3d. Notion sync 실패

증상: `unauthorized` 또는 `database_id not found`.

```bash
# integration이 DB에 share 됐는지 확인
gh secret list --repo gengar200005/clinic-location-screener  # 등록 확인
```

해결: Notion DB 페이지 → 우상단 ⋯ → Connections → integration 추가.

### 3e. 공간조인에서 동 매칭 실패

증상: `clinics_by_dong` 행 수가 raw HIRA보다 크게 적음.

가능 원인:
- HIRA 좌표(`XPos/YPos`)가 EPSG:4326 아님 → `scoring/spatial_join.py` 변환 로직 점검
- 행정동 경계 버전이 옛날 → `scrapers/admin_boundary --force --version verXXX` 새 버전 시도

### 3f. GH Actions commit-back 실패

증상: `! [remote rejected] main -> main (refusing to allow a GitHub App to create or update workflow)`.

원인: workflow yaml 자체를 수정하는 commit이 자동으로 발생. (현재 workflow는 `data/`·`web/data` 만 add하므로 발생 안 함.)

증상: `Permission denied`.

원인: Settings → Actions → General → Workflow permissions가 read-only.

해결:
```bash
gh api -X PUT repos/gengar200005/clinic-location-screener/actions/permissions/workflow \
  -f default_workflow_permissions=write -F can_approve_pull_request_reviews=false
```

## 4. 부분 재실행 패턴

| 상황 | 명령 |
|---|---|
| HIRA만 실패 → 재수집 | `python -m scrapers.hira_clinic --force --date YYYY-MM-DD` 후 spatial_join + pipeline |
| 점수만 재계산 (가중치 변경) | `python -m scoring.pipeline` |
| Notion만 다시 sync | `python -m publishers.notion_sync` |
| 캐시 무시 전체 재호출 | Actions 탭 → workflow_dispatch → `force_scrape: true` |

## 5. 새 행정동 추가 (분기 1회)

vuski 새 버전 출시 시:

```bash
# 1. 새 버전 확인
curl -s https://api.github.com/repos/vuski/admdongkor/contents/ | grep -oE 'ver\d+'

# 2. 다운로드 + centroid 재빌드
python -m scrapers.admin_boundary --version verYYYYMMDD --force
python -m scoring.spatial_join centroid

# 3. ODSay 신규 동만 호출 (캐시 hit + 신규만)
python -m scrapers.odsay_transit
```

## 6. 데이터 정책 요약

| 폴더 | gitignore | 이유 |
|---|---|---|
| `data/raw/` | YES | 외부 API 스냅샷, 재수집 가능 |
| `data/cleaned/` | YES | 중간 산출물, 재생성 가능 |
| `data/cache/` | **NO (커밋 필수)** | ODSay/Kakao 한도 보호 |
| `data/scored/` | **NO (커밋 필수)** | 히스토리 |
| `web/data/` | **NO (커밋 필수)** | Pages 배포 데이터 |

## 7. 응급 롤백

PWA가 망가졌을 때:
```bash
git revert HEAD  # 또는 특정 commit
git push
# Pages가 1~2분 후 직전 버전으로 자동 복구
```

스코어 히스토리는 `data/scored/scores_YYYY-MM-DD.parquet` 에 보존 — 언제든 재배포 가능.

## 8. 모니터링 체크리스트 (월 1회)

- [ ] Actions 탭에서 4주 연속 green
- [ ] Notion DB의 Top 30이 합리적 (사용자가 모르는 동 비율 < 30%)
- [ ] PWA heatmap.json 크기 < 300 KB, boundaries.geojson < 1 MB
- [ ] ODSay/Kakao 캐시 hit률 (신규 동 없으면 100%)

# CLAUDE.md

웹/데스크톱 Claude Code 세션이 공유하는 프로젝트 컨텍스트.

## 프로젝트

**clinic-location-screener** — 서울+경기 내과 개원 입지 자동 스크리너. 서울 424 + 경기 인접 9개 시 = 약 **653개 행정동**을 대상으로 Top 30 후보지를 주간 자동 갱신.

**최종 목표 (2027-05 개원)**: PWA 히트맵 + Notion DB (Top 30) + GitHub Actions 주간 배치.

## 가중치 (확정, 변경 시 docs/PLAN.md 동시 수정)

```
Score = 0.4 · C_norm + 0.4 · P_norm + 0.2 · T_norm
  C = 경쟁 (1 − percentile_rank)     ← 낮을수록 좋음
  P = 인구·40대+ 비율 (percentile_rank)
  T = 이촌역 대중교통 소요 (1 − percentile_rank)  ← 낮을수록 좋음
```

정규화는 **percentile rank 고정**. Min-Max·Z-score로 바꾸면 히스토리 스코어 재계산 필요.

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

## API·환경 변수

| 변수 | 발급처 | 상태 |
|------|--------|------|
| `HIRA_KEY` | data.go.kr "병원정보서비스" (데이터 ID 15001698) | ✅ 발급·검증됨 |
| `ODSAY_KEY` | lab.odsay.com | ⬜ 4주차 |
| `KOSIS_KEY` | kosis.kr/openapi | ⬜ 3주차 |
| `NOTION_TOKEN`, `NOTION_DB_ID` | notion.so/my-integrations | ⬜ 5주차 |
| `MOIS_KEY` | (선택) | ⬜ |
| `KAKAO_KEY` | (선택) | ⬜ |

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

```bash
# 1주차 (1회성)
python -m scrapers.admin_boundary
python -m scoring.spatial_join centroid

# 2주차 (주 1회)
python -m scrapers.hira_clinic            # --test로 1페이지만
python -m scoring.spatial_join join-clinics --clinics data/raw/hira/hira_YYYY-MM-DD.parquet

# 전체 파이프라인 (미구현, 3주차+)
python -m scoring.pipeline --date YYYY-MM-DD
```

## 진행 상태

- ✅ 1주차: 행정동 경계 (653개) · 중심점 parquet
- ✅ 2주차: HIRA 9,564 의원 수집 · 공간조인 6,669 / 653동
- ⬜ 3주차: 인구·세대 (행안부 + KOSIS) → 경쟁·인구 점수
- ⬜ 4주차: ODSay 통근 → 가중합
- ⬜ 5주차: Notion sync
- ⬜ 6주차: GitHub Actions 배치
- ⬜ 7주차: PWA (Leaflet + Top 30 테이블)
- ⬜ 8주차: 안정화·문서화

## 주의 (세션 공통)

- 사용자는 비개발자 성향. **Bash로 가능한 건 직접 실행**, 사용자에게 터미널 명령 넘기지 말 것. 단 destructive 작업(rm -rf, force push 등)은 확인 필요.
- 웹↔데스크톱 이동 시점을 **선제적으로 알림**. 마일스톤 완료·commit 직후가 이동 적기.
- 한국어 응답. 짧고 구체적으로.
- `data/cache/` 절대 삭제 금지.

# clinic-location-screener

서울+경기 내과 개원 입지 자동 스크리너. 약 700~800개 행정동을 대상으로 경쟁·인구·통근 지표를 가중합하여 Top 30 후보지를 주간 자동 갱신한다.

## 가중치

- 경쟁 의원 밀도/반경 `40%`
- 인구·40대이상 세대 `40%`
- 이촌역 기준 대중교통 소요시간 `20%`

## 데이터 소스

- 건강보험심사평가원(HIRA) OpenAPI — 병의원 정보
- 행정안전부 주민등록 인구통계 / KOSIS — 행정동별 인구·세대
- ODSay 대중교통 API — 이촌역 기준 소요시간
- vuski/admdongkor — 행정동 경계 GeoJSON (EPSG:4326)

## 구조

```
config/        대상 지역·상수·코드 매핑
scrapers/      외부 데이터 수집
scoring/       정규화·가중합
publishers/    Notion·PWA 배포
data/          raw → cleaned → scored 3단 + cache
web/           PWA (Leaflet 히트맵 + Top 30)
.github/workflows/   주간 배치
```

## 실행 (로컬)

```bash
python -m venv .venv
.venv\Scripts\activate              # Windows
pip install -r requirements.txt
cp .env.example .env                # 키 채우기

python -m scrapers.admin_boundary   # 1회성
python -m scoring.pipeline --date 2026-04-19
python -m publishers.notion_sync
python -m publishers.web_export
```

## 자동 배치

매주 토요일 03:00 KST — `.github/workflows/weekly_pipeline.yml`

## 상세 설계

[docs/PLAN.md](docs/PLAN.md)

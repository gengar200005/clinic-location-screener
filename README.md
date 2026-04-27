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

### 데이터 신선도 한계

- **WorldPop 2020 (6년 묵음)**: 신도시·재개발 지역 ±20% 오차 가능
- **KOSIS `ratio_40plus`는 동 단위** → catchment에 그대로 적용 (인접 동 연령구조 다르면 오차)
- **HIRA 분기 갱신 vs 주 1회 수집** (변화 없는 주가 대부분)

상세는 [docs/DATA_SOURCES.md](docs/DATA_SOURCES.md) 각 절 하단 참조.

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

## 새 기기에서 시작하기

```bash
git clone https://github.com/gengar200005/clinic-location-screener.git ~/work/clinic-location-screener
cd ~/work/clinic-location-screener
python -m venv .venv
source .venv/Scripts/activate       # Windows Git Bash (cmd: .venv\Scripts\activate)
pip install -r requirements.txt
cp .env.example .env                # 키 채우기 (HIRA_KEY, KOSIS_KEY 등)
bash scripts/resume.sh              # git pull + venv + .env + pytest smoke 한 방
```

그 다음 `claude` 실행 → `/session-start`로 현재 맥락 자동 파악. 종료 시 `/session-end [한 줄 요약]`.

환경 동기화 대상 4가지: **코드**(GitHub) · **데이터**(`data/` — 일부 커밋, cache/scored 필수) · **맥락**(CLAUDE.md + SESSION_LOG.md) · **시크릿**(.env, 수동).

## 상세 설계

[docs/PLAN.md](docs/PLAN.md) · [SESSION_LOG.md](SESSION_LOG.md) · [docs/decisions/](docs/decisions/)

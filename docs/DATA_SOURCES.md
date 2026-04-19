# 데이터 소스

본 프로젝트는 **공공 OpenAPI 6종 + 1개 GeoJSON 레포** 만 사용한다. 라이선스 위반·스크래핑 우회 없음.

## 1. 행정동 경계 (vuski/admdongkor)

| 항목 | 값 |
|---|---|
| URL | `raw.githubusercontent.com/vuski/admdongkor/master/{version}/HangJeongDong_{version}.geojson` |
| 버전 | `ver20260201` (config 상수 `DEFAULT_VERSION`) |
| 라이선스 | MIT (저작자: vuski) |
| 사용 컬럼 | `adm_cd`(8자리), `adm_cd2`(10자리), `adm_nm`, `sidonm`, `sggnm`, `geometry` |
| 갱신 주기 | 분기~연간 (행정동 신설·폐지 반영) |
| 저장 위치 | `data/raw/admin_boundary/HangJeongDong_*.geojson` (.gitignore) |

전국 3,558개 동 중 **서울 424 + 경기 9시 229 = 653동**만 필터해서 사용 (`config/target_regions.yaml`).

## 2. HIRA 병의원 정보 (data.go.kr)

| 항목 | 값 |
|---|---|
| Endpoint | `apis.data.go.kr/B551182/hospInfoServicev2/getHospBasisList` |
| 인증 | Service Key (env `HIRA_KEY`, data.go.kr에서 발급) |
| 쿼터 | 개발 1만/일 · 운영 10만/일 |
| 필터 | `clCd=31` (의원) + `dgsbjtCd=01` (내과) |
| 시도 코드 | 서울 `110000` · 경기 `310000` (KOSIS·행안부와 상이, 검증됨) |
| 사용 컬럼 | `yadmNm`, `addr`, `XPos`, `YPos`(EPSG:4326), `clCdNm`, `estbDd`, `drTotCnt`, `telno` |
| 갱신 | 매주 토요일 (HIRA 자체는 분기 갱신이지만 idempotent) |
| 저장 | `data/raw/hira/hira_YYYY-MM-DD.parquet` (.gitignore) |

소화기내과 별도 코드 없음 → 병원명에 "소화기" 포함 시 `is_gi=True` 태깅.

## 3. KOSIS 행정동 인구 + 연령 (kosis.kr)

| 항목 | 값 |
|---|---|
| Endpoint | `kosis.kr/openapi/Param/statisticsParameterData.do` |
| 인증 | API Key (env `KOSIS_KEY`) |
| 통계표 | `orgId=101, tblId=DT_1B04005N` (행정구역(읍면동)별/5세별 주민등록인구) |
| 컬럼 | `C1`=adm_cd10(10자리), `C2`=연령코드, `ITM_ID=T2`=총인구수 |
| 40+ 산출 | C2 코드 `45,50,55,60,65,70,75,80,85,90,95,100,105` 13밴드 합 |
| 한도 | 요청당 40,000행 (3,622동 × 13밴드 = 47,086 → 2회 분할 호출) |
| 저장 | `data/raw/population/kosis_pop_age_YYYY-MM-DD.parquet` (.gitignore) |

## 4. ODSay 대중교통 (odsay.com)

| 항목 | 값 |
|---|---|
| Endpoint | `api.odsay.com/v1/api/searchPubTransPathT` |
| 인증 | API Key (env `ODSAY_KEY`) |
| 쿼터 | **월 5,000건 무료** |
| 출발 | 이촌역 (`ORIGIN_LNG=126.9718, ORIGIN_LAT=37.5224`) |
| 응답 | `result.path[0].info.totalTime` (분 단위) |
| 캐시 | `data/cache/odsay_commute.parquet` (**영구 커밋 필수** — 삭제 시 한도 위험) |
| 신규 동만 호출 | 캐시 hit 시 0건 소모. 첫 실행 653건, 이후 0~5건 |

## 5. Kakao Mobility 자차 (apis-navi.kakaomobility.com)

| 항목 | 값 |
|---|---|
| Endpoint | `apis-navi.kakaomobility.com/v1/directions` |
| 인증 | REST API Key (env `KAKAO_KEY`) |
| 쿼터 | 일 10,000건 |
| 출발 | 이촌1동 주민센터 |
| `departure_time` | 다음 화요일 07:30 KST (8시 도착 목표) — 누적통계 평균 |
| `priority=TIME` | 최단시간 |
| 캐시 | `data/cache/kakao_car.parquet` (영구 커밋) |

**T 점수의 주력**. ODSay는 보조 display.

## 6. Notion API (api.notion.com)

| 항목 | 값 |
|---|---|
| 인증 | Integration Secret (env `NOTION_TOKEN`, `ntn_...`) |
| DB ID | env `NOTION_DB_ID` (또는 `NOTION_DS_ID`) |
| API 버전 | `2022-06-28` (notion-client 3.x는 data_sources API 우선) |
| 동작 | upsert 기준 = `동명` (title) |
| 보존 | `임장상태`, `메모` 컬럼은 sync에서 건드리지 않음 |
| 페이지 본문 | `notion_embed.py` 가 GitHub Pages iframe 삽입 |

## 7. WorldPop 100m 격자 인구 (data.worldpop.org)

| 항목 | 값 |
|---|---|
| URL | `data.worldpop.org/GIS/Population/Global_2000_2020/2020/KOR/kor_ppp_2020.tif` |
| 라이선스 | CC BY 4.0 (등록 불필요, 자동 다운로드) |
| 데이터 | Top-down unconstrained 2020, 100m 격자 GeoTIFF, ~72MB |
| 좌표계 | EPSG:4326 (WGS84) |
| 사용처 | **인구 가중 행정동 중심점** 계산 — 산·하천·공원에 찍히는 기하 중심점 보정 |
| 저장 | `data/raw/worldpop/kor_ppp_2020.tif` (.gitignore, 분기 재다운로드) |
| 산출물 | `data/cache/admin_centroid_pop.parquet` (50KB, 영구 커밋, boundary 버전 갱신 시 재계산) |

핵심 임팩트 (2026-04-19 적용):
- 평균 219m 이동, 64개 동(~10%)이 500m+ 보정
- Top 30 flag 합 28 → 11 (centroid_mismatch 5→3, suburban_cluster 11→4, med_desert 12→4)
- 임장 좌표 = 진짜 배후세대 중심 (아파트단지 위주)

## 8. 좌표계

| 용도 | EPSG | 비고 |
|---|---|---|
| 저장 (GeoJSON, parquet) | 4326 (WGS84) | Leaflet·웹 호환 |
| 거리 계산 (반경, 통근) | 5179 (UTM-K) | 한국 측지 표준, 미터 단위 |

저장 → 계산 변환은 가능하지만 **역방향 불가**(정보 손실 없음을 보장 못함). 
스크래퍼는 4326으로 저장, scoring 단계에서 5179로 변환.

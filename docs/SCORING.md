# 스코어링 알고리즘

> **결과 = 0.45 · C + 0.45 · P + 0.1 · T** (모두 percentile rank, [0,1])

3개 지표를 raw → percentile rank → 가중합 → Top 30 순으로 처리한다. 가중치·서브가중치는 `config/constants.py`에서 변경 가능.

## C — 경쟁 (낮을수록 ↑ 점수)

```
C_raw = 0.5 · density_per_10k + 0.5 · n_clinic_within_500m
C_norm = 1 − percentile_rank(C_raw)   # 1 - rank: 경쟁 낮은 동이 높은 norm
```

| 항 | 의미 | 데이터 |
|---|---|---|
| `density_per_10k` | 동 내 내과의원 수 / (catchment_pop_1_5km / 10,000) | HIRA + WorldPop (P와 분모 통일) |
| `n_clinic_within_500m` | 동 중심점 EPSG:5179 좌표에서 반경 500m 내 의원 수 | HIRA + admin_centroid |

**서브가중치 0.5/0.5는 임의값**(8주차 이후 이상점 보며 조정).

밀도만 보면 인구 적은 동(공원·공단)이 N=1만 있어도 폭등. 반경만 보면 강남대로변처럼 의원이 군집한 곳에서 동 경계와 무관하게 잡힘. 두 항 평균이 안전한 절충.

**2026-04-20 변경**: 밀도 분모를 **동 인구 → 중심점 반경 1.5km catchment 총인구**로 전환. P_raw 와 분모 통일 — "작은 동 + 인접 대단지" 케이스에서 밀도가 실상권 인구로 정규화됨.

## P — 인구 (40+ 환자풀, 높을수록 ↑ 점수)

```
P_raw = catchment_pop_1_5km × ratio_40plus      (기본, 2026-04-20~)
P_raw = pop_40plus                              (폴백, catchment 없을 때)
P_norm = percentile_rank(P_raw)
```

**왜 40+만?**
- 20대는 급성 1회성 (감기·장염). 만성진료는 거의 0.
- 40+에서 GI/HTN/DM/검진 빈도 급증 → 소화기내과 유효 환자.
- 50+에서 국가검진(위·대장내시경) 수요 집중.

**왜 catchment 기반? (2026-04-20 A안 적용)**
행정동 경계는 인위적이어서, "작은 동 + 인접 동 대단지" 케이스가 동 단위 pop_40plus로는 과소평가됨 (행신2동 사례). catchment_pop_1_5km는 중심점 반경 1.5km WorldPop 픽셀 합으로, 실상권 규모의 배후세대를 포착.

- `catchment_pop_1_5km`: WorldPop 100m 격자 합산 (pyproj 5179 버퍼 → 4326 역변환으로 위도 왜곡 보정)
- `ratio_40plus`: 동 단위 KOSIS 비율을 그대로 적용 (catchment 내 연령 분포가 동 분포와 유사하다는 근사)

중심점은 인구 가중 좌표(이전 개선)라 배후세대 집중 지점에서 출발하고, 1.5km 반경은 소화기내과의 일반적 진료권(도보/자차 10분).

**필터**: `pop_total < 500` 동은 스코어링 제외 (공단·공원). `MIN_POPULATION` 상수. catchment 기반으로 바뀌어도 동 단위 pop_total 필터는 유지 — 동 자체가 공단·공원이면 개원 실지 자체가 불가능.

## T — 통근 (이촌역 기준, 짧을수록 ↑ 점수)

```
T_raw = Kakao Mobility 자차 분 (다음 화요일 07:30 KST 출발)
T_norm = 1 − percentile_rank(T_raw)
```

- **Kakao 주력** (자차 누적통계). 사용자 본인이 자차로 출퇴근.
- **ODSay 대중교통 (`t_transit`)는 보조 display**. 최종 점수 미반영.
- 출발 시각을 평일 07:30으로 고정 → 일·시 무관 idempotent.
- 캐시 hit 시 API 비용 0.

## 정규화: percentile rank (vs z-score)

```
rank_pct(x_i) = #{j : x_j ≤ x_i} / N
```

- **이상치 강건**: rank 기반이라 극단값 영향 없음 (인구 n_clinic 모두 로그정규).
- **해석 용이**: "상위 N%" 직관.
- **분포 가정 없음**: 정규성 가정 불필요.
- **단점**: 절대 차이 정보 소실 (1등과 2등의 차이가 0.001일 수도, 0.5일 수도).

**바꾸기 어려운 결정** — 정규화 방식 변경 시 모든 히스토리 재계산 필요.

## 최종 가중합

```
Score_i = 0.45 · C_norm + 0.45 · P_norm + 0.1 · T_norm
Top 30 = score 내림차순 상위 30
```

| 가중치 근거 |
|---|
| C·P 동등 (0.45): 경쟁과 환자풀은 양방 trade-off, 어느 한쪽도 압도적이지 않음. |
| T 0.1: 통근은 임장 시 직접 확인 가능한 부가 정보. 점수의 "tie-breaker" 수준. |
| **2026-04-19 조정**: 초안 T=0.2는 민감도 분석 결과 16/30 동을 좌우하는 결정적 변수였음. 이촌역 30분권 서울 도심을 과도하게 끌어올림 (시도 분포 21/9). T=0.1로 약화 후 15/15 균형 회복, 평균 자차 25→68분 (경기 신도시 진입). PWA 메인의 슬라이더로 임장 단계에서 동적 재가중 가능. |

가중치 변경 시 `heatmap.json` 의 normalized 값을 그대로 쓰면 클라이언트에서 슬라이더 즉시 재계산 가능 (Post-MVP).

## 행정동 중심점 — 인구 가중 (2026-04-19 적용)

기하 중심점(geometric centroid)은 동 모양에 따라 산·하천·공원에 찍힐 수 있다 (이전 centroid_mismatch_flag: 동 5개). 임장 좌표가 어긋나면 "이 위치 반경 1km 의원·인구"가 실제와 괴리.

**해법**: WorldPop 100m 격자 인구로 폴리곤 안 인구 가중 평균 좌표 계산.

```
lat_pop = Σ(pop_i · lat_i) / Σ pop_i
lon_pop = Σ(pop_i · lon_i) / Σ pop_i
```

폴백: 폴리곤 안 인구 합 0이면 (공단·공원·산) 기하 중심점 사용.

영향 범위:
- ✅ **임장 좌표** (PWA detail 페이지 지도 중심)
- ✅ **반경 500m 의원 카운트** (C 점수의 절반)
- ✅ **통근 T_raw** (Kakao directions 도착점)
- ✅ **반경 1km/2km 카운트 + flag**
- ✅ **catchment_pop_1_5km** (P_raw · density 분모, 2026-04-20 추가)

데이터 소스: docs/DATA_SOURCES.md §7. 캐시 영구 커밋 (`data/cache/admin_centroid_pop.parquet`), boundary 버전 갱신 시만 재계산.

## Catchment 인구 — 배후 상권 (2026-04-20 적용)

중심점 반경 `CATCHMENT_RADIUS_M` (기본 1.5km) 내 WorldPop 픽셀 합을 `catchment_pop_1_5km` 으로 저장. P_raw, density 분모로 공통 사용.

```
catchment_pop_1_5km(i) = Σ worldpop_j  ∀ j ∈ (dist(centroid_i, pixel_j) ≤ 1.5km)
```

- 반경 버퍼는 pyproj로 EPSG:5179(거리 정확) 상에서 계산 후 WGS84 역변환 (위도 왜곡 보정)
- 행정동 경계와 무관 → "작은 동 + 인접 대단지" 케이스에서 실상권 규모 포착
- 캐시: `admin_centroid_pop.parquet` 에 컬럼 추가, boundary 갱신 시만 재계산

**왜 1.5km?** 소화기내과 1차 진료권 기준: 도보 15분 ≈ 1km, 자차 5분 ≈ 1.5km. 너무 작으면 경계 효과 못 깨고, 너무 크면 실제 출렁 범위 밖 인구까지 합산. 추후 민감도 분석 예정.

## 해석 보조 플래그

스코어와 별도로 동의 성격을 표시:

| 플래그 | 트리거 |
|---|---|
| `med_desert` | n_clinic_2km == 0 — 의료사막 (블루오션) |
| `centroid_mismatch` | 중심점이 행정동 외부 (드문 케이스, GeoJSON 정합성 확인 필요) |
| `suburban` | n_clinic_500m == 0 AND n_clinic_2km > 30 — 신도시 변두리에 의원 군집이 있는 경우 |

PWA 메인·Notion 페이지에서 ⚠️ 뱃지로 표시.

## 이상치 처리

| 케이스 | 처리 |
|---|---|
| 인구 < 500 | 스코어링 제외 (`MIN_POPULATION`) |
| 의원 0개 | 그대로 (블루오션, 유효) |
| ODSay/Kakao 호출 실패 | T_raw=999, 자동 최하위 + 재시도 목록 |

**알려진 한계**: 한강·주요 산 폴리곤이 catchment 안에 포함되어 있어도 마스킹 안 됨 → 한강변·산악 인접 동 catchment 과대평가 가능.

## 검증 체크포인트

- Top 30에 사용자가 익숙한 동(이촌·압구정)이 포함되는가?
- 모르는 상위 동이 점수 breakdown으로 설명 가능한가?
- 서울 22 / 경기 8 분포가 합리적인가? (두 지역 비교 가능 명세)

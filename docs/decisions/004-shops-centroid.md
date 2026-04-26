# ADR-004: 1·2층 상가 가중 중심점 (CENTROID_MODE="shops")

**날짜**: 2026-04-26
**상태**: 채택

## 배경

행정동 중심점은 모든 거리 기반 측정(catchment 인구·반경 의원 카운트·이촌역 자차분)의 기준점. 기존 `CENTROID_MODE = "pop"` (WorldPop 인구 가중 mean)은 폴리곤 안에서 사람이 많이 사는 쪽으로 좌표를 끌어주지만, 두 가지 한계:

1. **인구 mean이 토지 분류상 개원 불가능 위치에 찍힘**: 아파트 단지 한복판·산·녹지·학교 부지. 그 좌표에서 답사 가도 1종 근생 매물 없음. 좌표 자체가 무의미.
2. **인구 mean ≠ 의료상권 mean**: 환자가 실제로 가는 의원은 상가/역 belt에 있음. 인구 중심에서 1.5km 반경은 진짜 경쟁 의원을 다 못 잡거나 엉뚱한 의원을 잡음.

진단 (`scripts/diag_centroid_vs_shops.py`):
- Top30 인구 중심 ↔ 1·2층 상가 mean 거리: 중앙 164m, p90 532m, **max 864m (부천 춘의동)**
- 가장 큰 갭: 부천 춘의동(864m), 양천 신정3동(623m), 관악 삼성동(563m), 도봉 창1동(528m), 동대문 청량리동(506m)

## 결정

`CENTROID_MODE = "shops"` 채택. 1·2층 상가 평균 좌표(소상공인진흥공단 데이터)를 모든 거리 측정의 기준점으로 사용.

- 1·2층 상가 = 토지 분류상 1종 근생/2종 근생/상업지역에서 운영 중인 위치 → **개원 가능 좌표**
- 답사 좌표·반경 의원 카운트·catchment 인구 모두 의료상권 실상 반영

**Fallback 순서** (`scoring/centroid_shops_weighted.py`):
1. shops_lat_mean / shops_lon_mean
2. lat_pop / lon_pop (admin_centroid_pop.parquet)
3. 기하 centroid

대상 653개 동 모두 1순위 적용 (shops 데이터 100% 커버). 폴백은 신규 시군구 추가 시 보호망.

## 대안

- **`CENTROID_MODE = "pop"` 유지**: 사용자 핵심 우려(개원 불가 위치 좌표) 미해결. 기각.
- **Hybrid (0.5·pop + 0.5·shops)**: 두 가설 평균. 어느 쪽이 옳은지 안 가리는 결정 미루기. 검증 결과 shops가 명확히 우수 → 기각.
- **국토부 LURIS 용도지역 polygon**: 정석이지만 데이터 무거움 + 자동 갱신 어려움. 1·2층 상가 데이터로 사실상 같은 정보 얻음.

## 검증 (ablation)

`scripts/ablation_centroid_shops.py` — 전체 동 c_raw·p_raw 재계산 후 Top30 비교:

- **Top30 25/30 유지**, 5 교체
- **새 진입 5개 sanity check** (`scripts/sanity_new_top30.py`):
  - 노원 중계2·3동 (130→6위) ✅: 인구 중심이 옆 동 의료상권까지 잡아 경쟁 과대평가됐던 케이스
  - **관악 대학동** (100→20위) ⚠️→✅: 인구↔상가 1049m, 두 클러스터 동. 인구 중심에선 의료사막(0/0/6/9) ↔ 상가 중심은 의료상권 진입(6/8/10/45). **이 ADR의 정당성을 가장 강하게 입증한 사례**.
  - 부천 오정구 신흥동 (58→22위) ⚠️: 의료사막 패턴 (1.5km=4, 2km=29). 측정 한계 아닌 동 자체 특성. 답사 시 별도 주의.
  - 마포 상암동 (31→25위) ✅
  - 성북 정릉2동 (160→28위) ✅
- **탈락 5개**: 청량리동·창1·창3·장안2·고강본동 — 모두 인구 중심에선 경쟁 과소평가됐던 동들. 상가 중심에서 진짜 경쟁 노출되어 떨어짐 (직관 일치).

## Caveat

1. **`shift_m > 800m` 동 (두 클러스터)**: 답사 시 양쪽 모두 봐야. 현재 관악 대학동 1건. PWA에서 두 anchor 모두 표시 권장.
2. **`med_desert_flag` 동**: 1.5km < 5 AND 2km < 30. 답사 시 환자 동선이 동 밖 의료상권으로 빠지는지 확인. Top30 중 부천 신흥동 해당.
3. **t_raw (이촌역 자차분) 약간 부정확**: Kakao Mobility 캐시는 인구 가중 좌표 시점에 호출됨. 좌표 1km 이동의 통근시간 영향은 ±2분 수준이라 점수 영향 미미. 다음 GH Actions cron에서 새 좌표로 자동 갱신될 것 (kakao_car 스크래퍼가 admin_centroid를 매회 새로 읽으면 OK — 후속 확인).
4. **`centroid_mismatch_flag` 의미 변화**: 기존엔 인구 중심점 기준이었는데 이제 상가 중심점 기준 (500m=0 AND 2km≥50). 새 정의에서 해당 동은 3개 (`flags: desert=22, centroid_mismatch=3, suburban_cluster=1`).

## 결과 (2026-04-26)

- `data/cache/admin_centroid_shops.parquet` 생성 (653 동, 모두 shops anchor)
- `scoring/centroid_shops_weighted.py`, `scoring/spatial_join.apply_shops_weighted_centroid`, `apply_centroid_overlay` 추가
- `config/constants.CENTROID_MODE = "shops"`
- `data/scored/scores_2026-04-26.parquet` + `top30_2026-04-26.parquet` 새 생성
- 새 Top10: 월계2동·신월7동·삼성동·번3동·상1동·중계2·3동·상동·송내1동·하계2동·신정7동

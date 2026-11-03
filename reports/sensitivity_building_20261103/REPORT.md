# 민감도·건물 확인 판정 재검증

작업 브랜치 `fix/sensitivity-building-evidence`. 원격 main 및 작업 기준 커밋 `6f635d5be549071ea06dd88fd4fe7fa113df8559`. 기존 로컬 e64f4cb와 병합본의 파일 차이는 없었다. 기준일 2026-11-03, 기존 자료 `f0b99325bd62`, 수정 자료 `7b5574bc07c5`.

## 민감도

같은 점수 구간에서는 각 대안에서 다시 계산한 구조점수로 정렬한다. 통근, 점수 구간, 구조점수, 후보 ID의 순서를 기본과 모든 대안에서 공통으로 적용한다. 기본 점수·순위·대표 좌표와 서울 30곳·경기 15곳의 기본 우선군은 동일하다. 전체 목록 18,676행을 비교했고, 공개 목록의 범위를 기존과 새 시나리오별 결과에서 각각 재현했다.

| 지역 | 종전 범위 | 수정 범위 | 수정 후 우선군 포함 |
|---|---:|---:|---:|
| 서울 쌍문3동 | 1~2 | 1~7 | 31/31 |
| 서울 길음2동 | 2~6 | 1~11 | 31/31 |
| 서울 상도1동 | 1~6 | 1~9 | 31/31 |
| 경기 소사본1동 | 1 | 1~8 | 30/30 |
| 경기 소사본동 | 2 | 2~18 | 28/30 |
| 경기 안산 부곡동 | 3 | 2~44 | 24/30 |

범위가 달라진 묶음은 서울 324/329, 경기 478/518이다. 우선군에 포함된 시나리오 수는 서울과 경기 각각 37개 행정동에서 달라졌다. `sensitivity_before_after.csv`에 행정동별 전후 범위와 포함 횟수를 담았다. 같은 행정동에서 대안별 대표 좌표가 바뀔 수 있다. 길음2동의 원본 건물 위치 연결, 실제 공실과 동선은 확인이 필요하다. 경기 의료매출 및 250m 생활인구는 평가 미반영 상태다.

## 건물

기존 커밋의 판정 함수를 동일한 테스트 기록으로 실행했다. 최신 공실 확인일이 비공실을 가리키는 사례, 같은 날짜의 면적·배치·승강기 충돌, 층 `3`과 `"3"`의 다섯 대조 사례 모두 종전에는 ‘현장 확인 가능’, 수정 후에는 ‘매물·공간 사전확인’이었다. 항목별 날짜를 우선하고, 제공하지 않은 필드는 기존 근거를 대체하지 않으며 명시적인 null은 확인일이 있는 미확인 기록으로 처리한다. 층의 숫자 표기는 정규화하고 건물·층·호실은 별도로 유지한다. 기록마다 출처와 날짜를 남기고, 동일일의 충돌 항목을 표시한다. 실제 건물 입력은 비어 있고 현장 확인 가능은 0건이다. 가상 기록은 테스트에서만 사용했다.

## 재현과 검사

`--legacy-dir`에 `commercial_node_registry.parquet`, `full_cohort_rough_commute_worklist.parquet`, `metro_commercial_age_catchments.parquet`, `commercial_node_evidence.parquet`가 모두 있는 폴더를 지정한다. 이번에는 기존 옆 체크아웃을 사용했다. 네 파일의 해시는 이전 검증과 같다. `input_hashes.json`과 `VALIDATION.json`에 이번 입력과 보호 파일 160개의 검사 결과를 담았다. 이전 보고서와 기준본은 별도로 보관했다.

```powershell
$legacy = '../clinic-location-screener/reports/screening_v3/2026-10-23'
& ../clinic-location-screener/.venv/Scripts/python.exe -B -m scripts.build_current_recommendations --as-of 2026-11-03 --legacy-dir $legacy
& ../clinic-location-screener/.venv/Scripts/python.exe -B -m scripts.audit_current_recommendations
& ../clinic-location-screener/.venv/Scripts/python.exe -B -m scripts.validate_recommendation_density --legacy-dir $legacy
```

같은 입력으로 두 번 실행했을 때 추천 JSON·인벤토리 CSV·시나리오 JSON의 해시가 각각 일치했다. 인벤토리 CSV는 기본 지표를 내보내므로 종전과 동일하고, 민감도 비교는 별도 CSV로 제공한다. 밀도 검증에서 사용하지 않는 중간 CSV 읽기를 제거했다. 전체 Python 130개와 Node 13개 테스트가 통과했다. 로컬 브라우저에서 세 화면의 동일 버전·기준일, 쌍문3동 수정 범위, 의원 반경 1.5km 및 매출 분기 변경을 확인했다. 충돌 기록의 실제 목록 표시는 공개 데이터에 해당 기록이 없어 테스트 자료로 검증했다.

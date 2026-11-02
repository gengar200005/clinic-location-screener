# 경기 매출 실제 원자료 확보 · 2026-11-02

키는 .env의 GYEONGGI_API_KEY 존재만 확인. 값 출력/로그/커밋 없음. 실제 API 인증 성공은 아직 검증하지 못했다. 공식 매출 API 상세페이지는 HTTP200이지만 HTML이 기본정보 입력값 중간에서 중단되어 서비스 식별자를 확보하지 못함.

대안인 공식 공개CSV를 정상 세션/Referer로 확보했다. 89,985,897바이트, SHA256 b0be0b2314db2d9d08b3f4bf8bf9895562ca9a0a3f8ad2690aead2546981f88f. MS949, 기준년월/행정동코드/중분류업종코드/매출금액 4열,1,750,297행, 키중복0. 기존 파서가 끝 빈 행 하나를 오류로 오인한 문제 수정. 비어 있지 않은 잘못된 행은 여전히 거부.

공식 코드표: https://data.gg.go.kr/portal/data/file/downloadFileData.do?infId=UIIRH9UN37UHEY1MIS4F36613535&infSeq=3&fileSeq=13156
관련 공식 지역화폐 데이터셋 첨부의2024년도_분석시스템_데이터셋_코드값.xlsx. 코드 설명의 CARD_MDCLASS_INDUTYPE_CD에서 J0 의료기관 확인. 의원 단독이 아니며 병원/치과/한의원/약국 포함 상세범위 미확인. ADMDONG_CD 시군구+동의 전체이름만 연결(공백만 정규화), 같은 동명 단독·접두사·유사매칭 없음. 2024표와 각 관측연도 경계 연속성 미검증이므로 잠정 연결이다.

208후보 중 공식 코드표 전체동명153곳·과거J0관측152곳. 최신 정의 검증된 의원 추세는0곳. 부천구 개편·분동·합동은 임의 복원하지 않음. 전체201801~202506이지만 J0는201801~202312이며 관측 누락 월 존재. 2024~2025 신규3자리 업종코드 대응 미확인. 금액단위/표본/추정방식 검증 전이라 원문 문자열 보존, 전년·전월·성장순위 산출 보류. 신규 개원 예상매출 추정 없음.

웹: gyeonggi-inventory.html 검색/과거 원문/결측 구분. revenue.html 경기 목록도 실제 관측건수와 코드 상태 표시. 완료/미완료가 구분되도록 progress 갱신. 서울매출 원자료/모델 점수 수정 없음.

재현: python -B -m scripts.build_gyeonggi_inventory --raw reports/gyeonggi_api_20261102/b0be0b2314db2d9d08b3f4bf8bf9895562ca9a0a3f8ad2690aead2546981f88f.raw --codebook reports/gyeonggi_api_20261102/codebook_2024.xlsx

raw90MB와 키파일은 로컬 보관. 관측JSON과 작은 코드표/수집로그만 검토 가능한 GitHub 변경으로 제공. 관련Python14개 통과. API인증/페이지처리/최신의원추세 연결 완료로 보고하지 않음.

"""scoring/checkup_competition.py: 주소 → 시군구 키."""
from scoring.checkup_competition import _sgg_from_addr


def test_sgg_from_addr_handles_city_with_gu():
    assert _sgg_from_addr("경기도 부천시 원미구 길주로 1") == "부천시원미구"
    assert _sgg_from_addr("서울특별시 강남구 테헤란로 152") == "강남구"
    assert _sgg_from_addr("경기도 김포시 김포한강4로 1") == "김포시"

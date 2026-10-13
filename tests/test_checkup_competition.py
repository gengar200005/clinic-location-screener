"""scoring/checkup_competition.py: 주소 → 시군구 키."""
from scoring.checkup_competition import _sgg_from_addr


def test_sgg_from_addr_handles_city_with_gu():
    assert _sgg_from_addr("경기도 부천시 원미구 길주로 1") == "부천시원미구"
    assert _sgg_from_addr("서울특별시 강남구 테헤란로 152") == "강남구"
    assert _sgg_from_addr("경기도 김포시 김포한강4로 1") == "김포시"


def test_egd_keys_dedupes_multi_device_institutions(monkeypatch, tmp_path):
    """장비 2대(2행) 기관도 시도 내 유일 이름으로 매칭돼야 한다 (2026-10-13 버그)."""
    import pandas as pd
    import scoring.checkup_competition as cc

    monkeypatch.setattr(cc, "DATA_RAW", tmp_path)          # 원본 CSV 없음 → 캐시 경로
    cache = tmp_path / "egd.parquet"
    pd.DataFrame({
        "시도명": ["경기도", "경기도"], "시군구명": ["부천시 원미구", "부천시 원미구"],
        "요양기관명": ["가온내과의원", "가온내과의원"],
    }).to_parquet(cache)
    monkeypatch.setattr(cc, "EGD_KEYS_CACHE", cache)
    full, unique_name = cc._egd_keys({"경기도"})
    assert ("경기도", "부천시원미구", "가온내과의원") in full
    assert ("경기도", "가온내과의원") in unique_name

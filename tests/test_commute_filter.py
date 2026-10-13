"""v7 통근 60분 필터 + 대상 지역 제외 규칙."""
import pandas as pd

from config.regions import is_excluded
from scoring.commute import classify_commute


def _row(car_am, car_pm, tr_am, tr_pm):
    return {"adm_cd": "x", "t_car_am": car_am, "t_car_pm": car_pm,
            "t_transit_am": tr_am, "t_transit_pm": tr_pm}


def _classify(*rows):
    return classify_commute(pd.DataFrame(rows), 60)


def test_both_directions_must_pass():
    # 자차 출근 40분이어도 퇴근 70분이면 자차 불합격, 대중교통 75분 → 제외
    out = _classify(_row(40, 70, 75, 75)).iloc[0]
    assert not out["commute_ok_car"]
    assert not out["commute_ok"]
    assert out["commute_status"] == "exceed"


def test_either_mode_is_enough():
    # 자차 출퇴근 모두 60 이내, 대중교통 초과 → 후보
    out = _classify(_row(55, 60, 80, 85)).iloc[0]
    assert out["commute_ok_car"] and not out["commute_ok_transit"]
    assert out["commute_ok"]
    assert out["t_car"] == 60 and out["t_raw"] == 60


def test_car_exceed_with_transit_unchecked_is_exceed_car():
    # 대중교통 퇴근 구간 결측 → 대중교통 판정 불가. 자차 초과 → exceed_car (수동 확인 대상)
    out = _classify(_row(70, 70, 50, None)).iloc[0]
    assert not out["commute_ok"]
    assert out["commute_status"] == "exceed_car"


def test_no_car_route_and_no_transit_is_unknown():
    out = _classify(_row(None, None, None, None)).iloc[0]
    assert out["commute_status"] == "unknown"


def test_manual_transit_override(tmp_path):
    from scoring.commute import apply_manual_transit
    wide = pd.DataFrame([_row(70, 70, None, None)])
    f = tmp_path / "m.csv"
    f.write_text("adm_cd,transit_am_min,transit_pm_min\nx,55,58\n", encoding="utf-8-sig")
    out = _classify(*apply_manual_transit(wide, f).to_dict("records")).iloc[0]
    assert out["commute_ok_transit"] and out["commute_status"] == "ok"


def test_missing_leg_but_other_mode_ok():
    out = _classify(_row(30, 35, None, None)).iloc[0]
    assert out["commute_ok"] and out["commute_status"] == "ok"


def test_gyeongbu_exclusion_prefix_match():
    assert is_excluded("경기도", "성남시분당구")
    assert is_excluded("경기도", "수원시 팔달구")
    assert is_excluded("경기도", "안성시")
    assert not is_excluded("경기도", "고양시일산서구")
    assert not is_excluded("경기도", "광명시")
    # 경기 광주시만 제외 — 다른 시도 이름과 혼동 금지
    assert not is_excluded("인천광역시", "광주시")
    assert not is_excluded("서울특별시", "중구")


def test_quota_error_keeps_completed_rows(monkeypatch, tmp_path):
    """쿼터 오류가 나도 같은 배치에서 이미 받은 결과는 캐시에 남는다."""
    import pytest
    import scrapers.commute_isochrone as ci

    monkeypatch.setattr(ci, "CACHE_PATH", tmp_path / "c.parquet")
    monkeypatch.setattr(ci, "CALL_DELAY_SEC", {"car": 0, "transit": 0})
    monkeypatch.setattr(ci, "WORKERS", {"car": 1, "transit": 1})
    monkeypatch.setenv("ODSAY_KEY", "x")
    monkeypatch.setenv("KAKAO_KEY", "x")
    calls = {"n": 0}

    def fake_odsay(key, src, dst):
        calls["n"] += 1
        if calls["n"] > 2:
            raise ci.QuotaError("Daily quota exceeded")
        return 40, "ok"

    monkeypatch.setattr(ci, "_call_odsay", fake_odsay)
    pts = pd.DataFrame({"adm_cd": list("abcde"), "adm_nm": list("abcde"),
                        "lat": [37.5] * 5, "lon": [127.0] * 5})
    with pytest.raises(ci.QuotaError):
        ci.fetch(pts, legs=("transit_am",))
    saved = ci.load_cache()
    assert len(saved) == 2 and (saved["status"] == "ok").all()

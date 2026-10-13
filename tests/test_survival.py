"""scoring/survival.py: 반경 내 개설·폐업 카운트와 표본 부족 처리."""
import pandas as pd

from scoring.survival import compute_survival

REF = pd.Timestamp("2026-09-30")


def _clinic(opened, closed_at=None, lon=127.0, lat=37.5):
    return {"opened": pd.Timestamp(opened), "closed_at": pd.Timestamp(closed_at) if closed_at else pd.NaT,
            "is_closed": closed_at is not None, "lon": lon, "lat": lat}


def test_counts_and_survival_within_radius():
    hist = pd.DataFrame([
        _clinic("2022-01-01"),
        _clinic("2023-01-01", "2025-01-01"),   # 기간 내 개설 후 폐업
        _clinic("2024-06-01"),
        _clinic("2001-01-01", "2024-01-01"),   # 오래된 의원 폐업 → n_close만
        _clinic("2022-01-01", lon=127.2),      # 반경 밖 (~17km)
    ])
    cent = pd.DataFrame({"adm_cd": ["a"], "lon": [127.0], "lat": [37.5]})
    out = compute_survival(cent, hist, REF, years=5, radius_m=1500, min_opened=3).iloc[0]
    assert out["n_open_5y"] == 3
    assert out["n_open_5y_alive"] == 2
    assert out["n_close_5y"] == 2
    assert out["net_open_5y"] == 1
    assert abs(out["survival_5y"] - 2 / 3) < 1e-9


def test_small_sample_is_nan_and_flagged():
    hist = pd.DataFrame([_clinic("2023-01-01")])
    cent = pd.DataFrame({"adm_cd": ["a"], "lon": [127.0], "lat": [37.5]})
    out = compute_survival(cent, hist, REF, years=5, radius_m=1500, min_opened=3).iloc[0]
    assert pd.isna(out["survival_5y"]) and out["survival_sample_flag"]

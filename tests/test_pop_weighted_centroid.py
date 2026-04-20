"""apply_pop_weighted_centroid: cache 있을 때 lat/lon 교체 + 5179 재계산."""
import pandas as pd
import pytest

from scoring.spatial_join import apply_pop_weighted_centroid


def _base_centroid():
    return pd.DataFrame({
        "adm_cd": ["A", "B"],
        "lat": [37.5, 37.6],
        "lon": [127.0, 127.1],
        "x_5179": [100.0, 200.0],   # 더미
        "y_5179": [300.0, 400.0],
    })


def test_no_cache_returns_unchanged(tmp_path, monkeypatch):
    """admin_centroid_pop.parquet 없으면 원본 그대로."""
    from config import constants
    monkeypatch.setattr(constants, "DATA_CACHE", tmp_path)
    df = _base_centroid()
    out = apply_pop_weighted_centroid(df)
    pd.testing.assert_frame_equal(out, df)


def test_cache_replaces_lat_lon_and_recomputes_5179(tmp_path, monkeypatch):
    from config import constants
    monkeypatch.setattr(constants, "DATA_CACHE", tmp_path)
    pop = pd.DataFrame({
        "adm_cd": ["A", "B"],
        "lat_pop": [37.51, 37.61],
        "lon_pop": [127.01, 127.11],
        "pop_sum_in_polygon": [1000.0, 2000.0],
        "pop_weighted": [True, True],
        "catchment_pop_1_5km": [50000.0, 80000.0],
    })
    pop.to_parquet(tmp_path / "admin_centroid_pop.parquet", index=False)

    df = _base_centroid()
    out = apply_pop_weighted_centroid(df)

    # lat/lon 교체됨
    assert out.set_index("adm_cd").loc["A", "lat"] == 37.51
    assert out.set_index("adm_cd").loc["A", "lon"] == 127.01
    # 5179 재계산됨 (더미 값과 다름)
    assert out.set_index("adm_cd").loc["A", "x_5179"] != 100.0
    # 보조 컬럼은 정리됨
    assert "lat_pop" not in out.columns
    assert "pop_weighted" not in out.columns
    # catchment 컬럼은 유지 (P_raw · density 분모용)
    assert "catchment_pop_1_5km" in out.columns
    assert out.set_index("adm_cd").loc["A", "catchment_pop_1_5km"] == 50000.0


def test_partial_cache_uses_geometric_for_missing(tmp_path, monkeypatch):
    """일부 동만 cache에 있을 때, 빠진 동은 기하 중심점 유지."""
    from config import constants
    monkeypatch.setattr(constants, "DATA_CACHE", tmp_path)
    pop = pd.DataFrame({
        "adm_cd": ["A"],
        "lat_pop": [37.51],
        "lon_pop": [127.01],
        "pop_sum_in_polygon": [1000.0],
        "pop_weighted": [True],
    })
    pop.to_parquet(tmp_path / "admin_centroid_pop.parquet", index=False)

    df = _base_centroid()
    out = apply_pop_weighted_centroid(df)
    # A는 교체, B는 원본 유지
    assert out.set_index("adm_cd").loc["A", "lat"] == 37.51
    assert out.set_index("adm_cd").loc["B", "lat"] == 37.6

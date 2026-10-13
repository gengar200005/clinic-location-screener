"""대상 지역 설정 로더 (config/target_regions.yaml, v7).

- context_sido: 의원 공간조인·반경 카운트 범위
- exclude_sgg: 후보 제외 시군구 (startswith)
- prefilter_km: 통근 API 호출 전 직선거리 사전 필터
"""
from __future__ import annotations

from functools import lru_cache

import yaml

from config.constants import ROOT

TARGET_REGIONS_YAML = ROOT / "config" / "target_regions.yaml"


@lru_cache(maxsize=1)
def load_region_config() -> dict:
    with open(TARGET_REGIONS_YAML, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("context_sido", [])
    cfg.setdefault("exclude_sgg", {})
    cfg.setdefault("prefilter_km", None)
    return cfg


def context_sido() -> list[str]:
    return list(load_region_config()["context_sido"])


def exclude_prefixes(sido: str) -> tuple[str, ...]:
    return tuple(load_region_config()["exclude_sgg"].get(sido) or ())


def is_excluded(sido: str, sgg: str) -> bool:
    prefixes = exclude_prefixes(sido)
    return bool(prefixes) and str(sgg).replace(" ", "").startswith(prefixes)


def prefilter_km() -> float | None:
    v = load_region_config()["prefilter_km"]
    return float(v) if v is not None else None

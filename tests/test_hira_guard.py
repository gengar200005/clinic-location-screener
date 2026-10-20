"""scrapers/hira_clinic.py: 빈 응답·부분 수집을 저장하지 않는다 (2026-10-17 사고)."""
import pytest

import scrapers.hira_clinic as hc

EMPTY = b"<response><header><resultCode>00</resultCode></header><body><items/><totalCount>0</totalCount></body></response>"


class _Resp:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        pass


def test_empty_response_raises(monkeypatch):
    monkeypatch.setattr(hc.requests, "get", lambda *a, **k: _Resp(EMPTY))
    with pytest.raises(hc.IncompleteResponse):
        hc._fetch_page.retry_with(stop=lambda *_: True)("k", "310000", 1)


def test_partial_region_raises(monkeypatch):
    def fake(key, sido, page, expect_rows=True):
        return ([{"yadmNm": "a"}] * 1000, 2500) if page < 3 else ([], 2500)
    monkeypatch.setattr(hc, "_fetch_page", fake)
    with pytest.raises(hc.IncompleteResponse):
        hc.fetch_region("k", "경기", "310000")

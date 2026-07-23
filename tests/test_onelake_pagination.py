"""OneLake listing must fail loudly rather than silently truncate.

Mocked, so it needs no tenant: a paged response would otherwise return only its
first page and quietly break a wipe, a sync or a reconciliation.
"""

from __future__ import annotations

import pytest

from weaver import Location
from weaver.fabric.onelake import FabricStore


class _Response:
    def __init__(self, headers, paths):
        self.status_code = 200
        self.headers = headers
        self._paths = paths
        self.content = b"{}"

    def json(self):
        return {"paths": self._paths}


def _store(monkeypatch, headers, paths):
    store = FabricStore(token="fake-token")
    import weaver.fabric.onelake as onelake

    def fake_request(method, url, **kwargs):
        return _Response(headers, paths)

    monkeypatch.setattr(store, "_request", fake_request)
    return store


def test_a_single_page_returns_its_entries(monkeypatch):
    store = _store(
        monkeypatch,
        headers={},
        paths=[{"name": "lh.Lakehouse/Files/a.csv", "contentLength": "10"}],
    )
    entries = store.list(Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files"))
    assert [e.location.name for e in entries] == ["a.csv"]


def test_a_continuation_token_fails_loudly(monkeypatch):
    store = _store(
        monkeypatch,
        headers={"x-ms-continuation": "next-page-token"},
        paths=[{"name": "lh.Lakehouse/Files/a.csv", "contentLength": "10"}],
    )
    with pytest.raises(NotImplementedError, match="pagination is not implemented"):
        store.list(Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files"))

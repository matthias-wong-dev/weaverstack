"""OneLake listing must fail loudly rather than silently truncate.

Mocked, so it needs no tenant: a paged response would otherwise return only its
first page and break a wipe, a sync or a reconciliation.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.fabric.onelake import OneLakeDfsClient
from weaver.locations import Location
from weaver.store import StoreNotFoundError


class _Response:
    def __init__(self, headers, paths, *, status_code=200):
        self.status_code = status_code
        self.headers = headers
        self._paths = paths
        self.content = b"{}"

    def json(self):
        return {"paths": self._paths}


def _store(monkeypatch, headers, paths, *, status_code=200):
    store = OneLakeDfsClient(token="fake-token")

    def fake_request(method, url, **kwargs):
        return _Response(headers, paths, status_code=status_code)

    monkeypatch.setattr(store, "_request", fake_request)
    return store


@weaver_test()
def test_a_single_page_returns_its_entries(monkeypatch):
    store = _store(
        monkeypatch,
        headers={},
        paths=[{"name": "lh.Lakehouse/Files/Tables/a.csv", "contentLength": "10"}],
    )
    entries = store.list(
        Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files")
    )
    assert [e.location.name for e in entries] == ["a.csv"]


@weaver_test()
def test_a_continuation_token_fails_loudly(monkeypatch):
    store = _store(
        monkeypatch,
        headers={"x-ms-continuation": "next-page-token"},
        paths=[{"name": "lh.Lakehouse/Files/Tables/a.csv", "contentLength": "10"}],
    )
    with pytest.raises(NotImplementedError, match="pagination is not implemented"):
        store.list(Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files"))


@weaver_test()
def test_a_missing_listing_is_identified_as_not_found(monkeypatch):
    store = _store(monkeypatch, headers={}, paths=[], status_code=404)

    with pytest.raises(StoreNotFoundError):
        store.list(Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files"))

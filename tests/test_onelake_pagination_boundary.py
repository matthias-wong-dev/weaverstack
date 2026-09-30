"""OneLake listing follows every continuation page.

Mocked, so it needs no tenant: a truncated listing would break a wipe, a sync or
a reconciliation.
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

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
def test_a_continuation_token_returns_all_pages(monkeypatch):
    store = OneLakeDfsClient(token="fake-token")
    urls = []
    responses = iter(
        [
            _Response(
                {"x-ms-continuation": "next/page+token"},
                [
                    {
                        "name": "lh.Lakehouse/Files/Tables/a.csv",
                        "contentLength": "10",
                    }
                ],
            ),
            _Response(
                {},
                [
                    {
                        "name": "lh.Lakehouse/Files/Tables/b.csv",
                        "contentLength": "20",
                    }
                ],
            ),
        ]
    )

    def fake_request(method, url, **kwargs):
        urls.append(url)
        return next(responses)

    monkeypatch.setattr(store, "_request", fake_request)
    entries = store.list(
        Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files")
    )

    assert [entry.location.name for entry in entries] == ["a.csv", "b.csv"]
    assert "continuation" not in parse_qs(urlsplit(urls[0]).query)
    assert parse_qs(urlsplit(urls[1]).query)["continuation"] == ["next/page+token"]


@weaver_test()
def test_a_missing_listing_is_identified_as_not_found(monkeypatch):
    store = _store(monkeypatch, headers={}, paths=[], status_code=404)

    with pytest.raises(StoreNotFoundError):
        store.list(Location("https://onelake.dfs.fabric.microsoft.com/ws/lh/Files"))

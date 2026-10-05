"""A Warehouse waits until a Lakehouse's SQL endpoint lists what it will read.

A completed metadata refresh does not mean the endpoint already lists a new
shortcut table, so the reader asks through its own connection.
"""

from __future__ import annotations

import json

import pytest
from support.weaver_test import weaver_test

from weaver.build_bundle.executors import endpoint_objects
from weaver.build_bundle.executors.base import InstallationContext, Waiting
from weaver.build_bundle.executors.endpoint_objects import EndpointObjectsExecutor
from weaver.errors import InstallError

PAYLOAD = json.dumps(
    {"objects": [["Input_Dev", "Sales", "Customer"], ["Input_Dev", "Sales", "Order"]]}
).encode()


class _Endpoint:
    """Lists what it has been told it lists, case as Fabric happens to say it."""

    def __init__(self, *listed):
        self.listed = list(listed)
        self.asked = []

    def query(self, statement):
        self.asked.append(statement)
        return [{"table_schema": s, "table_name": n} for s, n in self.listed]


class _Resolver:
    def __init__(self):
        self.refreshed = []

    def start_sql_endpoint_refresh(self, item):
        self.refreshed.append(item.name)


def _context(sql, resolver=None):
    return InstallationContext(
        resolver=resolver or _Resolver(), store=None, target=None, sql=sql
    )


@weaver_test()
def test_the_wait_ends_once_every_object_is_listed():
    endpoint = _Endpoint(("sales", "customer"))
    context = _context(endpoint)
    executor = EndpointObjectsExecutor()

    waiting = executor.execute(None, PAYLOAD, context)
    assert isinstance(waiting, Waiting)
    assert waiting.state["objects"] == [["Input_Dev", "Sales", "Order"]]

    endpoint.listed.append(("Sales", "Order"))
    done = executor.execute(None, None, context, state=waiting.state)

    assert "visible_after_seconds" in done
    assert all("[Input_Dev].INFORMATION_SCHEMA.TABLES" in s for s in endpoint.asked)


@weaver_test()
def test_a_missing_object_asks_fabric_to_refresh_again(monkeypatch):
    resolver = _Resolver()
    context = _context(_Endpoint(), resolver)
    executor = EndpointObjectsExecutor()
    monkeypatch.setattr(endpoint_objects, "REFRESH_INTERVAL", 0.0)

    executor.execute(None, PAYLOAD, context)

    assert resolver.refreshed == ["Input_Dev"]


@weaver_test()
def test_an_object_never_listed_fails_by_name(monkeypatch):
    context = _context(_Endpoint(("Sales", "Customer")))
    executor = EndpointObjectsExecutor()
    monkeypatch.setattr(endpoint_objects, "TIMEOUT", 0.0)

    with pytest.raises(InstallError, match=r"Input_Dev\.Sales\.Order"):
        executor.execute(None, PAYLOAD, context)

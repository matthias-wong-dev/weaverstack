"""The common SQL executor, independent of a driver or Fabric."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from weaver.sql import (
    AccessTokenAuthentication,
    PooledSqlExecutor,
    SqlConnectionPool,
    SqlEndpoint,
    SqlExecutionError,
)
from weaver.sql.connection import connect


class Cursor:
    def __init__(self, *, rows=(), columns=(), error=None):
        self.rows = list(rows)
        self.description = [(name,) for name in columns] or None
        self.error = error
        self.calls = []
        self.closed = False

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))
        if self.error:
            raise self.error
        return self

    def fetchall(self):
        return list(self.rows)

    def nextset(self):
        return False

    def close(self):
        self.closed = True


class Connection:
    def __init__(self, cursor):
        self.next_cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.next_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


ENDPOINT = SqlEndpoint(
    "server.fabric.microsoft.com",
    "Reporting",
    workspace_id="workspace",
    warehouse_id="warehouse",
)
AUTH = AccessTokenAuthentication(lambda: "token")


def _executor(connections):
    created = []

    def factory(endpoint, authentication):
        connection = connections[len(created)]
        created.append(connection)
        return connection

    pool = SqlConnectionPool(ENDPOINT, AUTH, connection_factory=factory)
    return PooledSqlExecutor(pool, owns_pool=True), created


def test_execute_passes_parameters_commits_and_closes_the_cursor():
    cursor = Cursor()
    connection = Connection(cursor)
    executor, _ = _executor([connection])

    executor.execute("insert into t values (?)", [7])

    assert cursor.calls == [("insert into t values (?)", (7,))]
    assert cursor.closed
    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_query_returns_dictionaries_without_committing():
    cursor = Cursor(rows=[(1, "one"), (2, "two")], columns=["id", "name"])
    connection = Connection(cursor)
    executor, _ = _executor([connection])

    rows = executor.query("select id, name from t")

    assert rows == [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]
    assert cursor.closed
    assert connection.commits == 0


def test_failure_rolls_back_normalises_the_error_and_discards_the_connection():
    broken = Connection(Cursor(error=ValueError("bad statement")))
    healthy = Connection(Cursor())
    executor, created = _executor([broken, healthy])

    with pytest.raises(SqlExecutionError, match="Reporting.*bad statement"):
        executor.execute("broken")
    executor.execute("select 1")

    assert broken.rollbacks == 1
    assert broken.closed
    assert created == [broken, healthy]


def test_each_physical_connection_requests_current_authentication_material():
    tokens = iter(("first", "second"))
    auth = AccessTokenAuthentication(lambda: next(tokens))
    seen = []

    def driver(connection_string, **kwargs):
        seen.append(kwargs["attrs_before"][1256])
        return object()

    connect(ENDPOINT, auth, connector=driver)
    connect(ENDPOINT, auth, connector=driver)

    assert len(seen) == 2
    assert seen[0] != seen[1]

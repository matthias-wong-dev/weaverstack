"""The common SQL executor, independent of a driver or Fabric."""

from __future__ import annotations

from contextlib import contextmanager

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


def test_query_returns_dictionaries_and_commits():
    cursor = Cursor(rows=[(1, "one"), (2, "two")], columns=["id", "name"])
    connection = Connection(cursor)
    executor, _ = _executor([connection])

    rows = executor.query("select id, name from t")

    assert rows == [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}]
    assert cursor.closed
    assert connection.commits == 1


def test_query_commits_before_the_cursor_closes_and_the_lease_is_released():
    events = []

    class RecordingCursor(Cursor):
        def fetchall(self):
            events.append("fetch")
            return super().fetchall()

        def close(self):
            events.append("cursor close")
            super().close()

    class RecordingConnection(Connection):
        def commit(self):
            events.append("commit")
            super().commit()

    connection = RecordingConnection(RecordingCursor(rows=[(1,)], columns=["value"]))

    class Lease:
        def __init__(self):
            self.connection = connection

        def discard(self):
            events.append("discard")

    class Pool:
        endpoint = ENDPOINT

        @contextmanager
        def lease(self):
            events.append("lease")
            try:
                yield Lease()
            finally:
                events.append("release")

    rows = PooledSqlExecutor(Pool()).query("select 1 as value")

    assert rows == [{"value": 1}]
    assert events == ["lease", "fetch", "commit", "cursor close", "release"]


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

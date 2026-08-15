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


# --- calling a procedure for its outputs ---------------------------------------


class MultiSetCursor(Cursor):
    """A cursor over several result sets, walked with ``nextset()``.

    Which is what a Warehouse load procedure now looks like from outside: the
    authored setup may run ``EXEC`` and return rows of its own, and Weaver's
    answer is the projection its *own* batch ends with.
    """

    def __init__(self, sets):
        super().__init__()
        self._sets = list(sets)
        self._index = 0
        self._apply()

    def _apply(self):
        rows, columns = self._sets[self._index]
        self.rows = list(rows)
        self.description = [(name,) for name in columns] or None

    def nextset(self):
        if self._index + 1 >= len(self._sets):
            return False
        self._index += 1
        self._apply()
        return True


LOAD_OUTPUTS = (
    ("succeeded", "bit"),
    ("rows_read", "bigint"),
    ("error_message", "varchar(4000)"),
)


def test_call_procedure_declares_locals_and_passes_them_as_outputs():
    cursor = MultiSetCursor(
        [([(True, 4, None)], ["succeeded", "rows_read", "error_message"])]
    )
    executor, _ = _executor([Connection(cursor)])

    executor.call_procedure(
        "[_].[Load Sales.Customer]",
        inputs=(("fault_tolerant", 1),),
        outputs=LOAD_OUTPUTS,
    )

    batch, parameters = cursor.calls[0]
    assert "declare @weaver_out_succeeded bit;" in batch
    assert "@fault_tolerant = ?" in batch
    assert "@succeeded = @weaver_out_succeeded output" in batch
    assert parameters == (1,)


def test_call_procedure_reads_its_own_projection_not_the_procedures_rows():
    """The reason the load result stopped being a result set.

    Authored setup returned two result sets here. Neither is the answer, and a
    caller reading "the first result set" would have reported four thousand
    rows read from a table nobody asked about.
    """

    cursor = MultiSetCursor(
        [
            ([("something the author selected",)], ["whatever"]),
            ([(4000,)], ["rows_read"]),
            ([(True, 4, None)], ["succeeded", "rows_read", "error_message"]),
        ]
    )
    executor, _ = _executor([Connection(cursor)])

    row = executor.call_procedure("[_].[Load Sales.Customer]", outputs=LOAD_OUTPUTS)

    assert row == {"succeeded": True, "rows_read": 4, "error_message": None}


def test_call_procedure_ends_the_batch_with_its_projection():
    """Last, so that nothing the procedure emits can come after it."""

    cursor = MultiSetCursor(
        [([(True, 4, None)], ["succeeded", "rows_read", "error_message"])]
    )
    executor, _ = _executor([Connection(cursor)])

    executor.call_procedure("[_].[Load Sales.Customer]", outputs=LOAD_OUTPUTS)

    batch = cursor.calls[0][0].rstrip()
    assert batch.endswith(
        "select @weaver_out_succeeded as succeeded, "
        "@weaver_out_rows_read as rows_read, "
        "@weaver_out_error_message as error_message;"
    )


def test_call_procedure_refuses_a_call_that_names_no_outputs():
    executor, _ = _executor([Connection(Cursor())])

    with pytest.raises(SqlExecutionError, match="none were named"):
        executor.call_procedure("[_].[Load Sales.Customer]")


def test_call_procedure_reports_a_procedure_that_returned_nothing():
    """Which means the installed one is not the one Weaver generated."""

    cursor = MultiSetCursor([([], [])])
    executor, _ = _executor([Connection(cursor)])

    with pytest.raises(SqlExecutionError, match="altered outside Weaver"):
        executor.call_procedure("[_].[Load Sales.Customer]", outputs=LOAD_OUTPUTS)


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

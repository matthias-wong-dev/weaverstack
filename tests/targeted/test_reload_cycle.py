"""Reload: reconstructing one table from zero, and the order it happens in.

``reload`` is an execution mode, not a second way to author a load. It runs the
ordinary authored load against changed state:

.. code-block:: text

    _.LoadStatus  → Pending
    _.Bookmark    → the row is removed
    the target    → emptied
    read()        → runs, against both

The claims here are the ordering and what the reset writes. The state has to be
durable before the clear, and the clear has to happen before ``read()``, because
an incremental source reads either the bookmark or the target.

Pure Python. The clear is one statement, so a session that records statements
proves when it was submitted, and the catalogue is a real
:class:`~weaver.catalogue.state.Catalogue` over recorded rows.
"""

from __future__ import annotations

import pytest
from support.catalogues import LOADED_AT, Recording, identity, loaded, never
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    LOAD_STATISTIC,
    LOAD_STATUS,
)
from weaver.errors import LoadError
from weaver.lakehouse import Lakehouse
from weaver.objects import Folder, Table
from weaver.spark import FabricSparkTarget

#: What the clear submits, as the Delta runtime spells it.
CLEAR = "DELETE FROM `Demo`.`Sales_LH`.`DWG`.`Customer`"


class _Spark:
    """A session that records statements and models one thing: the target's rows.

    Enough for a reload and nothing more. The clear is the only statement these
    paths submit, and what a test needs to know is when it ran relative to the
    authored source and to the catalogue writes.
    """

    def __init__(self, rows: int = 7, events: list | None = None) -> None:
        self.rows = rows
        self.events = events if events is not None else []

    def sql(self, text: str):
        self.events.append(("sql", text))
        if text.startswith("DELETE FROM "):
            self.rows = 0
        return None


class _Writer(Recording):
    """A recording writer that also says when each write happened."""

    def __init__(self, events: list, *, failing: Exception | None = None) -> None:
        super().__init__(failing=failing)
        self.events = events

    def update(self, table, row) -> None:
        super().update(table, row)
        self.events.append(("update", table.name))

    def delete(self, table, rows) -> None:
        super().delete(table, rows)
        self.events.append(("delete", table.name))

    def submit(self, table, row) -> None:
        super().submit(table, row)
        self.events.append(("submit", table.name))

    def flush(self) -> None:
        super().flush()
        self.events.append(("flush", None))


def _lakehouse() -> Lakehouse:
    """A Lakehouse a statement can name, which is what the clear needs."""

    return Lakehouse(
        name="Sales_LH",
        spark_root="abfss://ws@onelake.dfs.fabric.microsoft.com/Sales_LH",
        destination=FabricSparkTarget(workspace="Demo", lakehouse="Sales_LH"),
    )


def _table(*, static: bool = False, failing: Exception | None = None):
    """A table whose ``read()`` reports what the target held when it was called.

    ``failing`` is the instance attribute ``failure``, so a test that loads twice
    can clear it and let the second one settle.
    """

    from weaver.declaration.metadata import PYTHON, parse_document

    declared = "true" if static else "false"

    class DWG__Customer(Table):
        failure = failing

        def _document(self):
            return parse_document(
                f"""
                Table ID: DWG.Customer

                Description: One row per customer.

                Lineage: The sales system.

                Primary key: Customer id

                Incremental: true

                Static: {declared}

                Schema:
                  Customer id: string
                """,
                language=PYTHON,
            )

        def read(self):
            # What an anti-join incremental source asks: what does the target
            # already hold, and what does the bookmark say was read.
            self.spark.events.append(("read", self.spark.rows, self.bookmark()))
            if self.failure is not None:
                raise self.failure
            return None

    return DWG__Customer


def _folder():
    from weaver.declaration.metadata import PYTHON, parse_document

    class DWG__Export(Folder):
        def _document(self):
            return parse_document(
                """
                Folder ID: DWG.Export

                Description: Exported files.

                Lineage: The sales system.
                """,
                language=PYTHON,
            )

        def read(self):
            return self.staging_folder()

    return DWG__Export


def _built(cls, *, at=LOADED_AT, rows: int = 7):
    """One anchored object, its recorded catalogue, and the ordered events."""

    events: list = []
    writer = _Writer(events)
    catalogue = (
        loaded("DWG.Customer", at=at, writer=writer)
        if at is not None
        else never("DWG.Customer", writer=writer)
    )
    spark = _Spark(rows=rows, events=events)
    return cls(spark, lakehouse=_lakehouse(), catalogue=catalogue), catalogue, events


def identity_row(name: str) -> dict:
    """One object's four-part key, as _.Bookmark keys it."""

    from weaver.catalogue.claims import bookmark_row

    return bookmark_row(identity(name))


def _kinds(events) -> list:
    """The events, reduced to what happened rather than to what was written."""

    reduced = []
    for event in events:
        if event[0] == "sql":
            reduced.append("clear" if event[1].startswith("DELETE FROM ") else "sql")
        elif event[0] == "read":
            reduced.append("read")
        elif event[0] == "update":
            reduced.append(f"update {event[1]}")
        elif event[0] == "delete":
            reduced.append(f"delete {event[1]}")
        elif event[0] == "submit":
            reduced.append(f"submit {event[1]}")
        else:
            reduced.append("flush")
    return reduced


# --- the order a reload happens in --------------------------------------------


@weaver_test()
def test_the_state_is_ended_and_durable_before_the_target_is_cleared():
    """The barrier the whole mode rests on.

    A bookmark left standing over an emptied target sends the next incremental
    read at a window nothing holds, so the status is written and flushed and the
    bookmark row removed, all before the clear.
    """

    table, _catalogue, events = _built(_table())

    table.load(reload=True)

    order = _kinds(events)
    assert order[:4] == [
        f"update {LOAD_STATUS.name}",
        "flush",
        f"delete {BOOKMARK.name}",
        "clear",
    ]


@weaver_test()
def test_the_target_is_cleared_before_the_authored_source_runs():
    """A target-dependent source must not see the rows it is about to replace.

    An incremental read that anti-joins the target answers "nothing to do" for
    every row already there, so a reload that cleared afterwards would
    reconstruct an empty table.
    """

    table, _catalogue, events = _built(_table(), rows=7)

    table.load(reload=True)

    order = _kinds(events)
    assert order.index("clear") < order.index("read")
    # What the source saw: an empty target, and no bookmark to read from.
    ((held, bookmark),) = [
        (event[1], event[2]) for event in events if event[0] == "read"
    ]
    assert held == 0
    assert bookmark == BOOKMARK_SENTINEL


@weaver_test()
def test_an_ordinary_load_clears_nothing_and_keeps_its_bookmark():
    """The mode is what changes, so without it nothing about a load moves."""

    table, _catalogue, events = _built(_table(), rows=7)

    table.load()

    assert "clear" not in _kinds(events)
    ((held, bookmark),) = [
        (event[1], event[2]) for event in events if event[0] == "read"
    ]
    assert held == 7
    assert bookmark == LOADED_AT


# --- what the reset writes ----------------------------------------------------


@weaver_test()
def test_the_reset_removes_the_bookmark_row():
    """One physical shape for "no clean load has established progress".

    An absent row, which is what a build's invalidation leaves. No sentinel is
    stored: the sentinel is what an absent row reads as, and storing it would
    give the estate two ways to say the same thing.
    """

    table, catalogue, _events = _built(_table())

    table.load(reload=True)

    (removed,) = catalogue.writer.removed(BOOKMARK.name)
    assert removed == identity_row("DWG.Customer")
    assert "bookmark_datetime" not in removed


@weaver_test()
def test_a_clean_reload_then_advances_the_bookmark_as_any_load_does():
    """Reload is a mode, not a state an object stays in.

    What ends it is what ends any load: a clean run that established an instant.
    """

    table, catalogue, _events = _built(_table())

    table.load(reload=True)

    (advanced,) = catalogue.writer.rows(BOOKMARK.name)
    assert advanced["bookmark_datetime"] > BOOKMARK_SENTINEL
    assert catalogue.bookmark(identity("DWG.Customer")) > BOOKMARK_SENTINEL


@weaver_test()
def test_the_reset_leaves_load_status_pending_before_the_load_settles():
    """``Pending``, carrying the workflow that emptied the target."""

    table, catalogue, _events = _built(_table())

    table.load(reload=True)

    first, last = catalogue.writer.rows(LOAD_STATUS.name)
    assert first["result"] == "pending"
    assert first["completed_datetime"] is None
    assert last["result"] == "succeeded"
    assert first["workflow_id"] == last["workflow_id"]


@weaver_test()
def test_an_ordinary_load_writes_one_load_status_row():
    """Nothing precedes it, because nothing was ended."""

    table, catalogue, _events = _built(_table())

    table.load()

    assert [row["result"] for row in catalogue.writer.rows(LOAD_STATUS.name)] == [
        "succeeded"
    ]


# --- what a reload records ----------------------------------------------------


@weaver_test()
def test_a_reload_records_is_reload():
    """The mode the caller asked for, written by the recorder that asked."""

    table, catalogue, _events = _built(_table())

    table.load(reload=True)

    assert catalogue.writer.rows(LOAD_STATISTIC.name)[0]["is_reload"] is True


@weaver_test()
def test_an_ordinary_load_records_is_reload_false():
    table, catalogue, _events = _built(_table())

    table.load()

    assert catalogue.writer.rows(LOAD_STATISTIC.name)[0]["is_reload"] is False


@weaver_test()
def test_the_lower_load_interface_clears_but_records_nothing():
    """``_load`` is the execution primitive, so the clear is its half.

    The reset is the recorder's, which is why an orchestrated run does it and
    this does not.
    """

    table, catalogue, events = _built(_table())

    table._load(reload=True)

    assert "clear" in _kinds(events)
    assert catalogue.writer.updated == []
    assert catalogue.writer.flushes == 0


# --- Static -------------------------------------------------------------------


@weaver_test()
def test_a_reload_loads_a_static_table_that_was_already_loaded():
    """Static means "load this once", and a reload asks for it again."""

    table, catalogue, events = _built(_table(static=True))

    result = table.load(reload=True)

    assert result.is_static_skip is False
    assert "read" in _kinds(events)
    assert catalogue.writer.rows(LOAD_STATISTIC.name)[0]["is_reload"] is True


@weaver_test()
def test_a_failed_static_reload_leaves_the_next_ordinary_load_to_run():
    """The regression the absent row buys.

    A Static object is skipped once a bookmark row says a clean load has run for
    this incarnation. A reload removes that row, so a reload that emptied the
    target and then failed cannot leave the object skippable: the next ordinary
    load reruns it rather than reporting a successful load of nothing.
    """

    table, catalogue, events = _built(
        _table(static=True, failing=RuntimeError("the cluster went"))
    )

    with pytest.raises(RuntimeError):
        table.load(reload=True)

    assert "clear" in _kinds(events)
    assert catalogue.bookmark(identity("DWG.Customer")) == BOOKMARK_SENTINEL

    # The same object, loaded the ordinary way, against the state that failure
    # left. Nothing here says reload.
    events.clear()
    table.failure = None
    result = table.load()

    assert result.is_static_skip is False
    assert "read" in _kinds(events)
    assert "clear" not in _kinds(events)
    assert catalogue.writer.rows(LOAD_STATISTIC.name)[-1]["is_reload"] is False


@weaver_test()
def test_an_ordinary_load_still_skips_a_loaded_static_table():
    table, _catalogue, events = _built(_table(static=True))

    result = table.load()

    assert result.is_static_skip is True
    assert "read" not in _kinds(events)


# --- a reload that fails ------------------------------------------------------


@weaver_test()
def test_a_failed_reload_leaves_no_bookmark_and_no_settled_success():
    """The retry has to be a reload too, and the state is what says so.

    The target was emptied, so what must not survive is the account of it as
    loaded. The bookmark row is gone and nothing wrote another, so the next
    ordinary load reads the sentinel and asks its source for everything.
    """

    table, catalogue, events = _built(_table(failing=RuntimeError("the cluster went")))

    with pytest.raises(RuntimeError, match="the cluster went"):
        table.load(reload=True)

    assert "clear" in _kinds(events)
    assert catalogue.writer.removed(BOOKMARK.name) == [identity_row("DWG.Customer")]
    assert catalogue.writer.rows(BOOKMARK.name) == []
    assert catalogue.bookmark(identity("DWG.Customer")) == BOOKMARK_SENTINEL
    assert [row["result"] for row in catalogue.writer.rows(LOAD_STATUS.name)] == [
        "pending",
        "error",
    ]


@weaver_test()
def test_a_failed_reload_is_recorded_as_a_reload():
    table, catalogue, _events = _built(_table(failing=RuntimeError("the cluster went")))

    with pytest.raises(RuntimeError):
        table.load(reload=True)

    assert catalogue.writer.rows(LOAD_STATISTIC.name)[0]["is_reload"] is True


@weaver_test()
def test_a_reset_that_did_not_land_clears_nothing():
    """The flush is a barrier, so a reload cannot start on state it did not write."""

    from weaver.catalogue.flusher import FlushError
    from weaver.run.result import RunError

    events: list = []
    writer = _Writer(events, failing=FlushError("the catalogue went away"))
    catalogue = loaded("DWG.Customer", writer=writer)
    table = _table()(_Spark(events=events), lakehouse=_lakehouse(), catalogue=catalogue)

    with pytest.raises(RunError, match="not recorded"):
        table.load(reload=True)

    assert "clear" not in _kinds(events)
    assert "read" not in _kinds(events)


# --- what reload does not cover -----------------------------------------------


@weaver_test()
def test_a_folder_refuses_reload_and_says_what_it_covers():
    """A folder's contents are files, and clearing them is not this branch."""

    folder = _folder()(
        _Spark(), lakehouse=_lakehouse(), catalogue=never("Files/DWG.Export")
    )

    with pytest.raises(LoadError, match="reload covers tables"):
        folder.load(reload=True)


@weaver_test()
def test_a_refused_folder_reload_records_nothing():
    """Refused before the record opens, so nothing says a load was attempted."""

    catalogue = never("Files/DWG.Export")
    folder = _folder()(_Spark(), lakehouse=_lakehouse(), catalogue=catalogue)

    with pytest.raises(LoadError):
        folder.load(reload=True)

    assert catalogue.writer.submitted == []
    assert catalogue.writer.updated == []

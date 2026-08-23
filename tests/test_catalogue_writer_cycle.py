"""The generic contract every catalogue runtime table is written through.

``_.Log`` and ``_.Bookmark`` were the first two tables to use it, and the whole
operational-state model rests on the mechanism being *table-generic*: a table
declares whether its rows are appended or merged on a key, and the writer, the
flusher and the Catalogue all read that declaration rather than knowing which
table they are handling.

So the tables here are declared in this module. A claim made only against the
two tables Weaver ships could pass on code that special-cased them, and the
tables that will use this next do not exist yet.

.. code-block:: text

    submit  -> queued, appended, one INSERT per batch
    update  -> queued, merged on the table's own key
    flush   -> the durability barrier, and the only place a failure surfaces

Pure Python throughout. The write stream is a real
:class:`~weaver.catalogue.flusher.WarehouseFlusher` over a recorder, so the
statements asserted on are the statements a Warehouse would be sent.
"""

from __future__ import annotations

import threading

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.flusher import FlushError, FlusherKey, WarehouseFlusher
from weaver.catalogue.state import Catalogue
from weaver.catalogue.tables import BIGINT, CatalogueColumn, RuntimeTable
from weaver.catalogue.writer import CatalogueWriter

#: An append-only table, keyed by a surrogate nothing merges on.
HISTORY = RuntimeTable(
    name="History",
    key=("log_sk",),
    description="Stands for any append-only runtime table.",
    columns=(
        CatalogueColumn("log_sk", not_null=True, description="A surrogate row key."),
        CatalogueColumn("workflow_id", description="What produced the row."),
        CatalogueColumn("count", BIGINT, description="Any measured value."),
    ),
)

#: A current-state table with a two-column key, so a claim about merging cannot
#: pass by accident on a one-column one.
CURRENT = RuntimeTable(
    name="Current",
    key=("item_name", "object_name"),
    description="Stands for any keyed current-state runtime table.",
    columns=(
        CatalogueColumn("item_name", not_null=True, description="The logical item."),
        CatalogueColumn("object_name", not_null=True, description="The object."),
        CatalogueColumn("result", description="Whatever state is maintained."),
    ),
)


class Recorder:
    """Where the statements go, with the ability to block and to fail."""

    def __init__(self, fail: Exception | None = None) -> None:
        self.statements: list[str] = []
        self.fail = fail
        self.started = threading.Event()
        self.release = threading.Event()
        self.block = False

    def __call__(self, statement: str) -> None:
        self.started.set()
        if self.block:
            self.release.wait(timeout=5)
        self.statements.append(statement)
        if self.fail is not None:
            raise self.fail


def _flusher(table: RuntimeTable, execute) -> WarehouseFlusher:
    return WarehouseFlusher(
        table,
        execute=execute,
        key=FlusherKey(
            workspace="Demo", warehouse="Weaver", schema="_", table=table.name
        ),
    )


class _Streams:
    """One recorder per table, so a claim can name the stream it means."""

    def __init__(self, *, failing: dict[str, Exception] | None = None) -> None:
        self.recorders: dict[str, Recorder] = {}
        self._failing = failing or {}

    def flusher_for(self, table):
        recorder = Recorder(fail=self._failing.get(table.name))
        self.recorders[table.name] = recorder
        return _flusher(table, recorder)

    def statements(self, table: RuntimeTable) -> list[str]:
        recorder = self.recorders.get(table.name)
        return [] if recorder is None else list(recorder.statements)


@pytest.fixture
def streams():
    return _Streams()


@pytest.fixture
def writer(streams):
    return CatalogueWriter(streams.flusher_for)


# --- what the two verbs mean --------------------------------------------------


@weaver_test()
def test_submit_appends(writer, streams):
    """An appended row is an INSERT, whatever table it belongs to."""

    writer.submit(HISTORY, {"log_sk": "a", "workflow_id": "w", "count": 1})
    writer.flush()

    (statement,) = streams.statements(HISTORY)
    assert statement.startswith("INSERT INTO")
    assert "[_].[History]" in statement


@weaver_test()
def test_update_merges_on_the_tables_own_key(writer, streams):
    """A keyed row is an upsert, so a row nothing wrote yet is inserted."""

    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})
    writer.flush()

    (statement,) = streams.statements(CURRENT)
    assert statement.startswith("MERGE")
    assert "[Item name]" in statement and "[Object name]" in statement


@weaver_test()
def test_a_table_declaring_no_key_cannot_be_merged_into():
    """Merging needs an identity, and an append-only table declares none."""

    unkeyed = RuntimeTable(
        name="History",
        description="Stands for a table with no identity to merge on.",
        columns=(CatalogueColumn("workflow_id", description="Anything."),),
    )
    writer = CatalogueWriter(lambda table: _flusher(table, Recorder()))

    with pytest.raises(FlushError, match="declares no key"):
        writer.update(unkeyed, {"workflow_id": "w"})


@weaver_test()
def test_appends_and_merges_never_share_a_statement(writer, streams):
    """One is an INSERT and the other a MERGE, so a batch holds one kind."""

    writer.submit(HISTORY, {"log_sk": "a"})
    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})
    writer.flush()

    assert len(streams.statements(HISTORY)) == 1
    assert len(streams.statements(CURRENT)) == 1


# --- several tables at once ---------------------------------------------------


@weaver_test()
def test_each_table_gets_its_own_stream(writer, streams):
    """A busy run writes several tables, and one wedged table is not all of them."""

    writer.submit(HISTORY, {"log_sk": "a"})
    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})

    assert set(streams.recorders) == {HISTORY.name, CURRENT.name}


@weaver_test()
def test_a_table_nothing_was_written_to_opens_no_stream(writer, streams):
    """Opened on first use, so a run that records nothing costs nothing."""

    writer.flush()

    assert streams.recorders == {}


@weaver_test()
def test_flush_waits_for_every_table(writer, streams):
    """The barrier is the whole record, not one table's worth of it."""

    writer.submit(HISTORY, {"log_sk": "a"})
    writer.submit(HISTORY, {"log_sk": "b"})
    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})
    writer.flush()

    assert streams.statements(HISTORY)
    assert streams.statements(CURRENT)


# --- asynchrony and the barrier ----------------------------------------------


@weaver_test()
def test_a_queued_row_does_not_wait_for_the_warehouse():
    """What makes recording affordable on the critical path of a run."""

    streams = _Streams()
    writer = CatalogueWriter(streams.flusher_for)
    writer.submit(HISTORY, {"log_sk": "a"})
    recorder = streams.recorders[HISTORY.name]
    recorder.block = True

    writer.submit(HISTORY, {"log_sk": "b"})

    # Returned without the second row having been written.
    assert "b" not in "".join(recorder.statements)
    recorder.release.set()
    writer.flush()


@weaver_test()
def test_a_write_that_did_not_land_surfaces_at_flush():
    """A queued failure has nowhere else to be reported."""

    streams = _Streams(failing={CURRENT.name: RuntimeError("refused")})
    writer = CatalogueWriter(streams.flusher_for)
    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})

    with pytest.raises(FlushError, match="refused"):
        writer.flush()


@weaver_test()
def test_one_tables_failure_is_reported_even_when_another_succeeded():
    """A run told its record landed must be able to rely on all of it."""

    streams = _Streams(failing={HISTORY.name: RuntimeError("refused")})
    writer = CatalogueWriter(streams.flusher_for)
    writer.submit(HISTORY, {"log_sk": "a"})
    writer.update(CURRENT, {"item_name": "Sales", "object_name": "Customer"})

    with pytest.raises(FlushError, match="refused"):
        writer.flush()


# --- what a reader of the same catalogue sees --------------------------------


@weaver_test()
def test_a_keyed_row_is_visible_through_the_catalogue_at_once(streams):
    """For any keyed table, before the Warehouse has it.

    A caller that just recorded state and then reads it back is asking about the
    work it just did, so it must not be answered with what the row replaced.
    """

    catalogue = Catalogue({}, writer=CatalogueWriter(streams.flusher_for))

    catalogue.update(
        CURRENT, {"item_name": "Sales", "object_name": "Customer", "result": "Succeeded"}
    )

    assert [row["result"] for row in catalogue.table_rows(CURRENT)] == ["Succeeded"]


@weaver_test()
def test_a_second_update_of_one_key_replaces_the_first(streams):
    """The row for one object is the same row every time."""

    catalogue = Catalogue({}, writer=CatalogueWriter(streams.flusher_for))
    identity = {"item_name": "Sales", "object_name": "Customer"}

    catalogue.update(CURRENT, {**identity, "result": "Failed"})
    catalogue.update(CURRENT, {**identity, "result": "Succeeded"})

    assert [row["result"] for row in catalogue.table_rows(CURRENT)] == ["Succeeded"]


@weaver_test()
def test_an_appended_row_is_not_read_back_through_the_catalogue(streams):
    """History is written and never consulted, so nothing reads it back."""

    catalogue = Catalogue({}, writer=CatalogueWriter(streams.flusher_for))

    catalogue.submit(HISTORY, {"log_sk": "a"})

    assert catalogue.table_rows(HISTORY) == ()


__all__: tuple = ()

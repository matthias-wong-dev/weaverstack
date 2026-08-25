"""Freestanding and catalogue-anchored objects, and who records a load.

The user model is two constructions::

    My__Table(spark)                               # freestanding
    My__Table(spark, catalogue="Warehouse/Weaver")  # anchored

Anchoring is what gives an object a place in the estate's own record of itself.
It is resolved once, at construction, so a name the catalogue does not record
fails there rather than part-way through a load. A freestanding object runs and
records nothing.

``update_catalogue`` says who records the load. A direct load records itself; an
orchestrated run passes ``False`` and records the node itself, beside the evidence
that it settled, so one row has one writer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from support.catalogues import LOADED_AT, identity, loaded, never
from support.weaver_test import weaver_test
from support.workspaces import given_workspace, mounted_lakehouse

from weaver import Table
from weaver.catalogue.tables import (
    BOOKMARK,
    BOOKMARK_SENTINEL,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
)
from weaver.errors import ConfigError, LoadError
from weaver.runtime.load_result import LoadResult

MODULE_DOC = """Table ID: DWG.Customer

Description: Customers.

Lineage: The sales system.

Primary key: CustomerId

Schema:
  CustomerId: string
"""


class DWG__Customer(Table):
    """Its contract is attached below rather than parsed from this file."""

    reads = 0

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(MODULE_DOC, language=PYTHON)

    def read(self):
        type(self).reads += 1
        raise AssertionError("read() is not what this module is about")


class DWG__Order(DWG__Customer):
    """A second object in the same item, for what a child resolves."""

    def _document(self):
        from weaver.declaration.metadata import PYTHON, parse_document

        return parse_document(
            MODULE_DOC.replace("DWG.Customer", "DWG.Order"), language=PYTHON
        )


@pytest.fixture
def lakehouse(tmp_path):
    return mounted_lakehouse("Sales_LH", tmp_path)


# --- freestanding or anchored ---------------------------------------------------


@weaver_test()
def test_a_freestanding_object_has_no_place_in_the_catalogue(lakehouse):
    table = DWG__Customer(object(), lakehouse=lakehouse)

    assert table.installed is None
    with pytest.raises(LoadError) as raised:
        table.bookmark()
    assert "cannot read its bookmark or record one" in str(raised.value)


@weaver_test()
def test_an_anchored_object_knows_which_installed_object_it_is(lakehouse):
    table = DWG__Customer(object(), lakehouse=lakehouse).with_catalogue(
        loaded("DWG.Customer")
    )

    assert table.installed == identity("DWG.Customer")
    assert table.bookmark() == LOADED_AT


@weaver_test()
def test_anchoring_resolves_the_identity_at_construction(lakehouse):
    """Not on first use: a name the catalogue does not record fails here.

    ``with_catalogue`` is what a run calls, and it resolves the same way when the
    caller does not already know the identity.
    """

    with pytest.raises(ConfigError) as raised:
        DWG__Order(object(), lakehouse=lakehouse).with_catalogue(never("DWG.Customer"))

    assert "not an object the Weaver catalogue records as installed" in str(
        raised.value
    )


@weaver_test()
def test_a_run_supplies_the_identity_it_already_resolved(lakehouse):
    """It built the graph from the Registry, so it need not resolve twice."""

    known = identity("DWG.Order")
    table = DWG__Order(object(), lakehouse=lakehouse).with_catalogue(
        never("DWG.Customer"), identity=known
    )

    assert table.installed is known


@weaver_test()
def test_the_catalogue_is_a_constructor_argument_and_not_a_load_one():
    """Because an authored ``read()`` is called by Weaver and takes nothing.

    Whatever ``read()`` may reach has to be set before the load begins, so the
    catalogue is named where the object is made.
    """

    import inspect

    assert "catalogue" in inspect.signature(DWG__Customer.__init__).parameters
    assert "catalogue" not in inspect.signature(DWG__Customer.load).parameters


@weaver_test()
def test_a_run_anchors_without_widening_the_primitive_contract():
    """What a deployed primitive must accept is ``cls(spark, lakehouse=...)``.

    A run knows the catalogue and the identity, and an author should not have to
    accept an argument for either, so the run asks the primitive to take them
    after it is built. A class meeting only the minimal contract is left alone.
    """

    import inspect

    assert "catalogue" not in inspect.signature(_Minimal.__init__).parameters
    assert not hasattr(_Minimal(object()), "with_catalogue")
    assert _Minimal(object()).load().succeeded


class _Minimal:
    """The whole contract, and nothing else: as `tests/support/thin.py` has."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        return LoadResult(succeeded=True)


@weaver_test()
def test_anchoring_by_name_outside_a_fabric_session_says_so(lakehouse):
    """A name is resolved where authored code runs, which is a Fabric session.

    The workspace is the one the process is in; a process that is in none has no
    workspace to name and says so rather than reaching for a default.
    """

    with pytest.raises(ConfigError) as raised:
        DWG__Customer(object(), lakehouse=lakehouse, catalogue="Warehouse/Weaver")

    assert "not in one" in str(raised.value)
    assert "weaver load" in str(raised.value)


@weaver_test()
def test_a_catalogue_named_by_name_owns_the_session_it_opened(monkeypatch):
    """Nobody handed it one, so it opened one, and closing it closes that.

    A load records how far it read, so the catalogue an object anchors to has to
    be able to write for as long as the object lives. Owning the Session is what
    makes that true without a cache anywhere.
    """

    from weaver.catalogue.state import catalogue_in
    from weaver.catalogue.tables import LOG

    opened = _sessions_opened(monkeypatch)
    workspace = given_workspace(catalogue="Warehouse/Weaver")

    catalogue = catalogue_in(workspace, tables=())

    assert catalogue.session is opened[-1]
    assert catalogue.session.closed is False
    # And it can write, which is the reason it kept the Session at all.
    catalogue.submit(LOG, {"log_sk": "a", "task_type": "load"})
    catalogue.flush()

    catalogue.close()

    assert opened[-1].closed is True
    assert catalogue.session is None


@weaver_test()
def test_a_catalogue_given_a_session_borrows_it(monkeypatch):
    """It belongs to whoever opened it, so closing the catalogue leaves it open."""

    from support.sessions import given_session

    from weaver.catalogue.state import catalogue_for

    opened = _sessions_opened(monkeypatch)
    workspace = given_workspace(catalogue="Warehouse/Weaver")
    with given_session(workspace=workspace) as session:
        catalogue = catalogue_for(session, workspace, tables=())

        assert catalogue.session is session
        catalogue.close()

        assert session.closed is False
    # Nothing was opened on the catalogue's behalf, either.
    assert opened == []


@weaver_test()
def test_a_catalogue_closes_the_session_it_opened_if_the_read_fails(monkeypatch):
    """A failed construction leaves nothing open behind it."""

    import weaver.catalogue.state as state
    from weaver.catalogue.state import catalogue_in

    opened = _sessions_opened(monkeypatch)

    def refuse(*_args, **_kwargs):
        raise RuntimeError("the catalogue could not be read")

    monkeypatch.setattr(state, "catalogue_for", refuse)

    with pytest.raises(RuntimeError, match="could not be read"):
        catalogue_in(given_workspace(catalogue="Warehouse/Weaver"))

    assert opened[-1].closed is True


@weaver_test()
def test_anchoring_by_name_asks_for_a_catalogue_that_owns_its_session(monkeypatch):
    """What an authored object gets when it names a catalogue rather than one."""

    from weaver.runtime import anchor

    asked: list = []

    class _Owned:
        session = "the one it opened"

        def installed_object(self, **_kwargs):
            return identity("DWG.Customer")

    def catalogue_in(workspace, *, tables=()):
        asked.append((workspace.catalogue, tuple(table.name for table in tables)))
        return _Owned()

    import weaver.catalogue.state as state
    import weaver.sessions.host as host

    monkeypatch.setattr(host, "current_workspace_name", lambda: "Sales_WS")
    monkeypatch.setattr(state, "catalogue_in", catalogue_in)

    table = DWG__Customer(
        object(),
        lakehouse=mounted_lakehouse("Sales_LH", Path("/tmp")),
        catalogue="Warehouse/Weaver",
    )

    assert table.installed == identity("DWG.Customer")
    (named, tables) = asked[0]
    assert named == "Warehouse/Weaver"
    # The catalogue's own order, not the declaration's: what matters is which
    # tables were read, and one read costs one round trip per table either way.
    assert {getattr(table, "name", table) for table in tables} == set(
        anchor.ANCHOR_TABLES
    )


def _sessions_opened(monkeypatch) -> list:
    """Record the Sessions opened on a catalogue's behalf, and answer with fakes."""

    from support.sessions import given_session

    import weaver.sessions.host as host

    opened: list = []

    def session_for(workspace, **_kwargs):
        session = given_session(workspace=workspace)
        opened.append(session)
        return session

    monkeypatch.setattr(host, "session_for", session_for)
    return opened


# --- what a child inherits -----------------------------------------------------


@weaver_test()
def test_a_child_inherits_the_catalogue_and_resolves_its_own_identity(lakehouse):
    """The catalogue travels; the identity and the bookmark do not."""

    parent = DWG__Customer(object(), lakehouse=lakehouse).with_catalogue(
        loaded("DWG.Customer", "DWG.Order")
    )

    child = DWG__Order(parent)

    assert child.installed == identity("DWG.Order")
    assert child.bookmark() == LOADED_AT


@weaver_test()
def test_the_constructor_takes_a_catalogue_as_readily_as_a_name(monkeypatch, lakehouse):
    """A caller that has one hands it over, and nothing is read or opened again."""

    opened = _sessions_opened(monkeypatch)
    catalogue = loaded("DWG.Customer")

    table = DWG__Customer(object(), lakehouse=lakehouse, catalogue=catalogue)

    assert table._catalogue is catalogue
    assert table.installed == identity("DWG.Customer")
    assert table.bookmark() == LOADED_AT
    assert opened == []


@weaver_test()
def test_a_supplied_catalogue_is_reused_rather_than_read_again(monkeypatch, lakehouse):
    """A run and a child object both hand one over, and neither pays for a read.

    Whatever the catalogue reaches its Warehouse through came with it, so no
    Session is opened here and none is closed.
    """

    opened = _sessions_opened(monkeypatch)
    catalogue = loaded("DWG.Customer", "DWG.Order")

    parent = DWG__Customer(object(), lakehouse=lakehouse).with_catalogue(catalogue)
    child = DWG__Order(parent)

    assert parent._catalogue is catalogue
    assert child._catalogue is catalogue
    assert opened == []


@weaver_test()
def test_a_child_the_catalogue_does_not_record_is_refused(lakehouse):
    parent = DWG__Customer(object(), lakehouse=lakehouse).with_catalogue(
        loaded("DWG.Customer")
    )

    with pytest.raises(ConfigError):
        DWG__Order(parent)


# --- who records the load ------------------------------------------------------


class DWG__Loading(DWG__Customer):
    """A table whose read succeeds, so the recording is what is left to look at.

    ``staged`` stands for whatever the author's frame is: the runtime is
    replaced, so what it holds is never read. It is not ``None``, which for a
    non-incremental table is refused. See ``tests/test_objects_declaration.py``.
    """

    staged = object()

    def read(self):
        return self.staged


def _loaded(monkeypatch, table, **load):
    """One load, with the Delta runtime replaced by a stated result."""

    import weaver.runtime.table_load as runtime

    monkeypatch.setattr(
        runtime, "load_table", lambda *a, **k: LoadResult(succeeded=True, rows_read=2)
    )
    return table.load(**load)


@weaver_test()
def test_a_direct_load_records_its_whole_operational_state(monkeypatch, lakehouse):
    """Everything the load produced, recorded and flushed before it returns.

    A caller told the load succeeded relies on the record having landed, which is
    what makes the standalone interface synchronous.
    """

    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)

    result = _loaded(monkeypatch, table)

    assert result.succeeded
    assert result.bookmark_datetime is not None
    assert [name for name, _row in catalogue.writer.updated] == [
        LOAD_STATUS.name,
        BOOKMARK.name,
    ]
    assert [name for name, _row in catalogue.writer.submitted] == [
        LOG.name,
        LOAD_STATISTIC.name,
    ]
    assert catalogue.writer.flushes == 1
    # And the catalogue it recorded into now answers with what it recorded.
    assert catalogue.bookmark(identity("DWG.Loading")) == result.bookmark_datetime


@weaver_test()
def test_the_lower_interface_leaves_the_recording_to_its_caller(monkeypatch, lakehouse):
    """``_load`` is what an orchestrated run calls, so one row has one writer.

    There is no flag: which interface was called decides who records.
    """

    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)

    import weaver.runtime.table_load as runtime

    monkeypatch.setattr(
        runtime, "load_table", lambda *a, **k: LoadResult(succeeded=True, rows_read=2)
    )
    result = table._load()

    # It still reports the instant, because the run needs it to record the node.
    assert result.bookmark_datetime is not None
    assert catalogue.writer.updated == []
    assert catalogue.writer.submitted == []
    assert catalogue.writer.flushes == 0


@weaver_test()
def test_a_freestanding_object_reads(lakehouse):
    """``read()`` takes no catalogue, so authored source logic runs on its own."""

    DWG__Loading.staged = "the rows this read produced"
    table = DWG__Loading(object(), lakehouse=lakehouse)

    assert table.read() == "the rows this read produced"
    assert table.installed is None


@weaver_test()
def test_a_freestanding_object_does_not_load(monkeypatch, lakehouse):
    """A load reads a window and records how far it read, so it needs both.

    Refused where the load begins rather than where the row would be written: a
    load that recorded nothing would leave the next one to read the same window
    and report success either way.
    """

    table = DWG__Loading(object(), lakehouse=lakehouse)

    with pytest.raises(LoadError) as raised:
        _loaded(monkeypatch, table)

    assert "cannot read its bookmark or record one" in str(raised.value)
    assert "catalogue=" in str(raised.value)


@weaver_test()
def test_a_rejecting_load_moves_no_bookmark(monkeypatch, lakehouse):
    """It has not read its window, so there is nothing to record having read."""

    import weaver.runtime.table_load as runtime

    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)
    monkeypatch.setattr(
        runtime,
        "load_table",
        lambda *a, **k: LoadResult(succeeded=False, rows_read=3, rows_rejected=1),
    )

    result = table.load(fault_tolerant=True)

    assert result.bookmark_datetime is None
    # The status and the statistics are recorded, a rejecting load happened,
    # and the bookmark is not, because the window was not read.
    assert [name for name, _row in catalogue.writer.updated] == [LOAD_STATUS.name]
    assert catalogue.writer.rows(LOAD_STATUS.name)[0]["result"] == "rejected"


@weaver_test()
def test_a_static_object_reads_the_record_to_decide_whether_to_run(lakehouse):
    """A bookmark row means a clean load has run for this incarnation."""

    class DWG__Static(DWG__Customer):
        def _document(self):
            from weaver.declaration.metadata import PYTHON, parse_document

            return parse_document(f"{MODULE_DOC}\nStatic: true\n", language=PYTHON)

    skipped = DWG__Static(object(), lakehouse=lakehouse).with_catalogue(
        loaded("DWG.Static")
    )
    result = skipped.load()

    assert result.succeeded
    assert result.rows_read == 0
    # A skip is a clean success, so the absent instant is what holds it still.
    assert result.bookmark_datetime is None

    # With no row it runs, and this object refuses its own read to prove it got there.
    running = DWG__Static(object(), lakehouse=lakehouse).with_catalogue(
        never("DWG.Static")
    )
    with pytest.raises(AssertionError, match="not what this module is about"):
        running.load()


@weaver_test()
def test_a_bookmark_is_the_instant_the_load_reported(monkeypatch, lakehouse):
    """Not the clock of whoever was orchestrating: the engine that ran it took it."""

    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)
    before = datetime.now(timezone.utc)

    result = _loaded(monkeypatch, table)

    assert before <= result.bookmark_datetime <= datetime.now(timezone.utc)
    assert result.bookmark_datetime > BOOKMARK_SENTINEL

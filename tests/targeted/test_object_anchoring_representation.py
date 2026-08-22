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

import pytest
from support.catalogues import LOADED_AT, identity, loaded, never
from support.weaver_test import weaver_test
from support.workspaces import mounted_lakehouse

from weaver import Table
from weaver.catalogue.tables import BOOKMARK, BOOKMARK_SENTINEL
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
    assert "not anchored" in str(raised.value)


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
    accept an argument for either — so the run *asks* the primitive to take them
    after it is built. A class meeting only the minimal contract is left alone.
    """

    import inspect

    assert "catalogue" not in inspect.signature(_Minimal.__init__).parameters
    assert not hasattr(_Minimal(object()), "with_catalogue")
    assert _Minimal(object()).load().succeeded


class _Minimal:
    """The whole contract, and nothing else — as `tests/support/thin.py` has."""

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
def test_anchoring_by_name_holds_the_session_its_catalogue_writes_through(monkeypatch):
    """A load records its own bookmark, so the reading Session has to survive.

    Closed at the end of the anchor, an object could read the catalogue and not
    record in it — which is what the first version of this did, and what a Fabric
    session reported as a closed Session on the first merge.
    """

    from weaver.catalogue.state import catalogue_for
    from weaver.catalogue.tables import LOG
    from weaver.runtime import anchor

    opened = _anchoring_in(monkeypatch, "Sales_WS")

    session, workspace = anchor._session_for("Warehouse/Weaver")

    assert session.closed is False
    catalogue = catalogue_for(session, workspace, tables=())
    catalogue.submit(LOG, {"log_sk": "a", "task_type": "load"})
    catalogue.flush()
    assert opened == [workspace]

    # And the next object anchored to the same catalogue reaches the same one,
    # rather than paying for a Session of its own.
    again, _ = anchor._session_for("Warehouse/Weaver")

    assert again is session
    assert opened == [workspace]


@weaver_test()
def test_a_session_that_has_been_closed_is_not_handed_out_again(monkeypatch):
    """A notebook can close one, and the next anchor opens another."""

    from weaver.runtime import anchor

    _anchoring_in(monkeypatch, "Sales_WS")

    first, _ = anchor._session_for("Warehouse/Weaver")
    first.close()
    second, _ = anchor._session_for("Warehouse/Weaver")

    assert second is not first
    assert second.closed is False


def _anchoring_in(monkeypatch, workspace_name: str) -> list:
    """Anchor as though this process were in ``workspace_name``.

    The workspace a name is resolved in is the one the process runs in, which
    off a tenant is nowhere — so the two things anchoring asks its host are
    supplied here, and the Sessions it opens are recorded.
    """

    from support.sessions import given_session

    import weaver.sessions.host as host
    from weaver.runtime import anchor

    opened = []

    def session_for(workspace, **kwargs):
        opened.append(workspace)
        return given_session(workspace=workspace)

    monkeypatch.setattr(host, "current_workspace_name", lambda: workspace_name)
    monkeypatch.setattr(host, "session_for", session_for)
    monkeypatch.setattr(anchor, "_SESSIONS", {})
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
def test_a_child_the_catalogue_does_not_record_is_refused(lakehouse):
    parent = DWG__Customer(object(), lakehouse=lakehouse).with_catalogue(
        loaded("DWG.Customer")
    )

    with pytest.raises(ConfigError):
        DWG__Order(parent)


# --- who records the load ------------------------------------------------------


class DWG__Loading(DWG__Customer):
    """A table whose read succeeds, so the recording is what is left to look at."""

    staged = None

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
def test_a_direct_load_records_itself(monkeypatch, lakehouse):
    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)

    result = _loaded(monkeypatch, table)

    assert result.succeeded
    assert result.bookmark_datetime is not None
    # Merged and flushed, both: a caller told the load succeeded relies on it.
    assert [name for name, _row in catalogue.writer.updated] == [BOOKMARK.name]
    assert catalogue.writer.flushes == 1
    # And the catalogue it recorded into now answers with what it recorded.
    assert catalogue.bookmark(identity("DWG.Loading")) == result.bookmark_datetime


@weaver_test()
def test_an_orchestrated_load_leaves_the_recording_to_the_run(monkeypatch, lakehouse):
    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)

    result = _loaded(monkeypatch, table, update_catalogue=False)

    # It still reports the instant, because the run needs it to record the node.
    assert result.bookmark_datetime is not None
    assert catalogue.writer.updated == []
    assert catalogue.writer.flushes == 0


@weaver_test()
def test_a_freestanding_load_runs_and_records_nothing(monkeypatch, lakehouse):
    table = DWG__Loading(object(), lakehouse=lakehouse)

    result = _loaded(monkeypatch, table)

    assert result.succeeded
    assert table.installed is None


@weaver_test()
def test_a_rejecting_load_records_nothing(monkeypatch, lakehouse):
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
    assert catalogue.writer.updated == []


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
def test_a_static_object_with_no_catalogue_cannot_answer_its_gate(lakehouse):
    class DWG__Static(DWG__Customer):
        def _document(self):
            from weaver.declaration.metadata import PYTHON, parse_document

            return parse_document(f"{MODULE_DOC}\nStatic: true\n", language=PYTHON)

    with pytest.raises(LoadError) as raised:
        DWG__Static(object(), lakehouse=lakehouse).load()

    assert "not anchored" in str(raised.value)


@weaver_test()
def test_a_bookmark_is_the_instant_the_load_reported(monkeypatch, lakehouse):
    """Not the clock of whoever was orchestrating: the engine that ran it took it."""

    catalogue = never("DWG.Loading")
    table = DWG__Loading(object(), lakehouse=lakehouse).with_catalogue(catalogue)
    before = datetime.now(timezone.utc)

    result = _loaded(monkeypatch, table)

    assert before <= result.bookmark_datetime <= datetime.now(timezone.utc)
    assert result.bookmark_datetime > BOOKMARK_SENTINEL

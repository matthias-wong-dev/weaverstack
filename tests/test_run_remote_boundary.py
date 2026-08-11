"""One RuntimeScope per run, living where the imports have to happen.

A Python primitive is a deployed module, imported inside the Fabric session
against that session's Spark. So when the Runner moves to the desktop, the scope
that owns those imports cannot move with it — it stays remote, and only its name
crosses:

.. code-block:: text

    begin_run(run_id)        ──►  RuntimeScope.new(), stored under run_id
    dispatch(run_id, node A) ──►  same scope
    dispatch(run_id, node B) ──►  same scope
    end_run(run_id)          ──►  scope.close(), forgotten

Two halves, tested apart because they fail apart. The **registry** is what the
Fabric interpreter does with a name; the **handle** is what the desktop submits.
Neither needs Fabric: the registry is ordinary Python, and the handle is asserted
against the programs it would send.

What only Fabric can prove — that two Python nodes really share imports, that a
rebuild is really picked up by the next run — belongs in `tests/fabric`.
"""

from __future__ import annotations

import ast

import pytest

from weaver.run import remote
from weaver.run.runtime_boundary import RemoteScope, open_runtime_scope
from weaver.workspaces import FabricWorkspace, LocalWorkspace


@pytest.fixture(autouse=True)
def no_leaked_scopes():
    """The registry is module state, so a test that left one would poison the next."""

    yield
    for run_id in remote.open_runs():
        remote.end_run(run_id)


# --- the registry, as the Fabric interpreter sees it --------------------------


def test_a_run_gets_a_scope_and_is_named_by_it():
    assert remote.begin_run("run-a") == "run-a"
    assert remote.open_runs() == ("run-a",)


def test_beginning_the_same_run_twice_keeps_the_first_scope():
    """A resubmitted statement must not replace a scope whose modules are
    already imported and in use."""

    remote.begin_run("run-a")
    first = remote.scope_for("run-a")
    remote.begin_run("run-a")

    assert remote.scope_for("run-a") is first


def test_two_runs_never_share_a_scope():
    """Across runs nothing is shared at all: that is what makes a rebuilt module
    take effect on the next load rather than the next session."""

    remote.begin_run("run-a")
    remote.begin_run("run-b")

    assert remote.scope_for("run-a") is not remote.scope_for("run-b")


def test_ending_a_run_closes_its_scope_and_forgets_it():
    remote.begin_run("run-a")
    closed = []
    remote.scope_for("run-a").close = lambda: closed.append(True)

    assert remote.end_run("run-a") is True
    assert closed == [True]
    assert remote.open_runs() == ()


def test_ending_a_run_that_was_never_begun_says_so_rather_than_failing():
    """Cleanup that raised would turn a finished run into a failed one."""

    assert remote.end_run("never-started") is False


def test_dispatching_into_a_run_with_no_scope_is_diagnosed():
    from weaver.run.result import RunError

    with pytest.raises(RunError) as raised:
        remote.scope_for("run-a")

    assert "run-a" in str(raised.value)
    assert "begin_run" in str(raised.value)


# --- the handle, as the desktop submits it -----------------------------------


class _Recording:
    """A Session that records the programs it is asked to run."""

    def __init__(self, answer=None):
        self.submitted = []
        self.answer = answer

    def executes_here(self, workspace=None):
        return False

    def execute_python(self, program, *, workspace=None, timeout=None):
        self.submitted.append(program)
        return self.answer


def _fabric():
    return FabricWorkspace(
        workspace="My Workspace", weaver_lakehouse="Weaver", environment="weaver"
    )


def _sources(session):
    return {program.name: program.source for program in session.submitted}


def _dispatch(scope):
    scope.dispatch_python(
        node_id="load:Lakehouse/Sales/Sales.Customer",
        item="Lakehouse/Sales",
        target="Sales",
        schema="_/Load",
        object="Sales__Customer.py",
        expected_class="Sales__Customer",
        fault_tolerant=False,
    )


def test_opening_a_scope_where_execution_is_local_needs_no_crossing():
    from weaver.runtime.python_context import RuntimeScope

    session = _Recording()
    session.executes_here = lambda workspace=None: True

    scope = open_runtime_scope(session, workspace=LocalWorkspace(workspace="/tmp/x"))

    assert isinstance(scope, RuntimeScope)
    assert session.submitted == []
    scope.close()


def test_opening_a_scope_where_execution_is_remote_begins_one_over_there():
    session = _Recording()

    scope = open_runtime_scope(session, workspace=_fabric())

    assert isinstance(scope, RemoteScope)
    assert scope.run_id in _sources(session)["begin_run"]


def test_a_session_that_cannot_place_itself_keeps_the_imports_here():
    """Positive knowledge is required to go remote: a scope opened over there by
    mistake would run the primitive somewhere the caller never named."""

    from weaver.errors import CommandError
    from weaver.runtime.python_context import RuntimeScope

    class Unplaceable(_Recording):
        def executes_here(self, workspace=None):
            raise CommandError("no workspace")

    session = Unplaceable()
    scope = open_runtime_scope(session, workspace=None)

    assert isinstance(scope, RuntimeScope)
    assert session.submitted == []
    scope.close()


def test_every_dispatch_names_the_run_whose_scope_it_belongs_to():
    session = _Recording(answer={"succeeded": True})
    scope = open_runtime_scope(session, workspace=_fabric())

    _dispatch(scope)

    submitted = _sources(session)["dispatch_python"]
    assert scope.run_id in submitted
    assert "Sales__Customer" in submitted


def test_the_submitted_program_builds_its_session_around_the_interpreters_spark():
    """The construction every other crossing performs. A Session built inside
    the call would have to go looking for an active Spark session rather than
    being handed the one the statement is running in."""

    session = _Recording(answer={})
    scope = open_runtime_scope(session, workspace=_fabric())
    _dispatch(scope)

    submitted = _sources(session)["dispatch_python"]
    assert "NotebookSession(workspace=workspace, spark=spark)" in submitted
    assert submitted.index("workspace = FabricWorkspace") < submitted.index(
        "NotebookSession"
    ), "the workspace must be defined before the session that takes it"


@pytest.mark.parametrize(
    "name", ["begin_run", "dispatch_python", "dispatch_validation", "end_run"]
)
def test_every_submitted_program_is_valid_python(name):
    """A typo here is invisible to every local test and would ship a run that
    cannot reach Fabric at all."""

    session = _Recording(answer={})
    scope = open_runtime_scope(session, workspace=_fabric())
    _dispatch(scope)
    scope.dispatch_validation(installed={"logical": "Lakehouse/Sales/S.C"}, collect=True)
    scope.close()

    ast.parse(_sources(session)[name])


def test_closing_the_handle_ends_the_run_over_there():
    session = _Recording(answer=True)
    scope = open_runtime_scope(session, workspace=_fabric())

    scope.close()

    assert scope.run_id in _sources(session)["end_run"]


def test_closing_twice_ends_the_run_once():
    session = _Recording(answer=True)
    scope = open_runtime_scope(session, workspace=_fabric())

    scope.close()
    scope.close()

    assert [one.name for one in session.submitted].count("end_run") == 1


def test_a_cleanup_that_cannot_reach_the_session_does_not_fail_the_run():
    """If the Livy session is already gone, so is the scope — which is the
    outcome `end_run` exists to reach."""

    session = _Recording()
    scope = open_runtime_scope(session, workspace=_fabric())

    def gone(program, *, workspace=None, timeout=None):
        raise RuntimeError("the Livy session is gone")

    session.execute_python = gone

    scope.close()  # must not raise


# --- what dispatch does with each kind of scope -------------------------------


def test_a_remote_scope_is_what_sends_a_python_node_across():
    """Which of the two happens is the scope's to answer, not dispatch's."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.load_plan import LAKEHOUSE_TARGET, PhysicalObjectRef, PhysicalTargetRef
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode
    from weaver.runtime.load_result import LoadResult

    sent = []

    class Scope:
        def dispatch_python(self, **arguments):
            sent.append(arguments)
            # The row a LoadResult crosses as, in full: `from_row` is the
            # inverse of `as_row` and takes every column, not a subset.
            return LoadResult(succeeded=True, rows_read=3).as_row()

    target = PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name="Sales")
    node = RunNode(
        node_id="load:Lakehouse/Sales/Sales.Customer",
        physical_target=target,
        primitive_kind="python_table",
        logical_id=WeaverDocumentId(
            WeaverItemId("Lakehouse", "Sales"),
            ObjectId(schema="Sales", object="Customer"),
        ),
        primitive_object=PhysicalObjectRef(
            target_id="Lakehouse/Sales",
            target_kind="lakehouse",
            schema="_/Load",
            object="Sales__Customer.py",
            object_type="file",
        ),
    )

    result = dispatch_primitive(
        node,
        session=_Recording(),
        resolved=type("R", (), {"expected_class": "Sales__Customer"})(),
        open_runtime=Scope,
    )

    (arguments,) = sent
    assert arguments["item"] == "Lakehouse/Sales"
    assert arguments["target"] == "Sales"
    assert arguments["object"] == "Sales__Customer.py"
    assert arguments["expected_class"] == "Sales__Customer"
    assert result.rows_read == 3


# --- preparing is not using ---------------------------------------------------


def test_a_warehouse_only_run_never_opens_a_runtime_scope():
    """The claim that keeps a declared requirement from becoming an acquisition.

    A run of nothing but stored procedures reaches no deployed module, so it
    needs no scope — and on a desktop, opening one means a Livy session and a
    `begin_run` crossing for work that is entirely T-SQL.
    """

    from weaver.load_plan import PhysicalTargetRef, WAREHOUSE_TARGET
    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode

    opened = []

    class Sql:
        def call_procedure(self, name, *, inputs, outputs):
            return {column: 0 for column, *_ in outputs}

    class Session(_Recording):
        def sql_executor(self, target, *, workspace=None):
            return Sql()

    node = RunNode(
        node_id="load:Warehouse/Reporting/Reporting.Revenue",
        physical_target=PhysicalTargetRef(kind=WAREHOUSE_TARGET, name="Reporting"),
        primitive_kind="warehouse_procedure",
        logical_id=WeaverDocumentId(
            WeaverItemId("Warehouse", "Reporting"),
            ObjectId(schema="Reporting", object="Revenue"),
        ),
    )

    dispatch_primitive(
        node,
        session=Session(),
        resolved=type("R", (), {"expected_class": None})(),
        open_runtime=lambda: opened.append(True),
    )

    assert opened == [], "a Warehouse-only run opened a runtime scope"


def test_a_warehouse_validation_opens_no_scope_either():
    """A Warehouse validation is a procedure, and TDS reaches it from here."""

    from weaver.load_plan import PhysicalTargetRef, WAREHOUSE_TARGET
    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import (
        PROCEDURE_SHAPE,
        WeaverDocumentId,
        WeaverItemId,
    )
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode
    from weaver.test_plan import InstalledValidation

    opened = []
    item = WeaverItemId("Warehouse", "Reporting")
    installed = InstalledValidation(
        logical=WeaverDocumentId(item, ObjectId(schema="Reporting", object="Present")),
        kind="Assumption",
        target=PhysicalTargetRef(kind=WAREHOUSE_TARGET, name="Reporting"),
        artefact=WeaverDocumentId(
            item,
            ObjectId(schema="_", object="Assumption Reporting.Present"),
            shape=PROCEDURE_SHAPE,
        ),
        object_type="stored_procedure",
    )

    class Sql:
        def call_procedure(self, name, *, inputs, outputs):
            return {"violation_count": 0}

    class Session(_Recording):
        def resolver(self, workspace=None):
            return object()

        def sql_executor(self, target, *, workspace=None):
            return Sql()

    node = RunNode(
        node_id="test:Warehouse/Reporting/Reporting.Present",
        physical_target=installed.target,
        primitive_kind="warehouse_procedure",
        logical_id=installed.logical,
        installed=installed,
    )

    dispatch_primitive(
        node, session=Session(), open_runtime=lambda: opened.append(True)
    )

    assert opened == []

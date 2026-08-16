"""One run scope per run, living where the imports have to happen.

A Python primitive is a deployed module, imported inside the Fabric session
against that session's Spark. So when the Runner moves to the desktop, the scope
that owns those imports cannot move with it: it stays remote, and only its name
crosses:

.. code-block:: text

    open_scope(run_id)       ──►  RuntimeScope.new(), stored under run_id
    dispatch(run_id, node A) ──►  same scope
    dispatch(run_id, node B) ──►  same scope
    close_scope(run_id)      ──►  scope.close(), forgotten

Two halves, tested apart because they fail apart. The **registry** is what the
Fabric interpreter does with a name; the **handle** is what the desktop submits.
Neither needs Fabric: the registry is ordinary Python, and the handle is asserted
against the programs it would send.

Both handles answer the same :class:`~weaver.run.runtime_boundary.RunScope`, so
the tests below assert what dispatch does with either rather than which one it
was given.

What only Fabric can prove — that two Python nodes really share imports, that a
rebuild is really picked up by the next run — belongs in `tests/fabric`.
"""

from __future__ import annotations

import ast

import pytest
from support.workspaces import given_workspace

from weaver.errors import CommandError
from weaver.run.runtime_boundary import (
    DirectRunScope,
    FabricRunScope,
    LazyRunScope,
    open_runtime_scope,
)
from weaver.runtime import session_scopes
from weaver.sessions.base import Session
from weaver.workspaces import Workspace


@pytest.fixture(autouse=True)
def no_leaked_scopes():
    """The registry is module state, so a test that left one would poison the next."""

    yield
    for run_id in session_scopes.open_scopes():
        session_scopes.close_scope(run_id)


# --- the registry, as the Fabric interpreter sees it --------------------------


def test_a_run_gets_a_scope_and_is_named_by_it():
    assert session_scopes.open_scope("run-a") == "run-a"
    assert session_scopes.open_scopes() == ("run-a",)


def test_beginning_the_same_run_twice_keeps_the_first_scope():
    """A resubmitted statement must not replace a scope whose modules are
    already imported and in use."""

    session_scopes.open_scope("run-a")
    first = session_scopes.get_scope("run-a")
    session_scopes.open_scope("run-a")

    assert session_scopes.get_scope("run-a") is first


def test_two_runs_never_share_a_scope():
    """Across runs nothing is shared at all: that is what makes a rebuilt module
    take effect on the next load rather than the next session."""

    session_scopes.open_scope("run-a")
    session_scopes.open_scope("run-b")

    assert session_scopes.get_scope("run-a") is not session_scopes.get_scope("run-b")


def test_ending_a_run_closes_its_scope_and_forgets_it():
    session_scopes.open_scope("run-a")
    closed = []
    session_scopes.get_scope("run-a").close = lambda: closed.append(True)

    assert session_scopes.close_scope("run-a") is True
    assert closed == [True]
    assert session_scopes.open_scopes() == ()


def test_ending_a_run_that_was_never_begun_says_so_rather_than_failing():
    """Cleanup that raised would turn a finished run into a failed one."""

    assert session_scopes.close_scope("never-started") is False


def test_dispatching_into_a_run_with_no_scope_is_diagnosed():
    from weaver.errors import RuntimeScopeError

    with pytest.raises(RuntimeScopeError) as raised:
        session_scopes.get_scope("run-a")

    assert "run-a" in str(raised.value)
    assert "open_scope" in str(raised.value)


# --- the handle, as the desktop submits it -----------------------------------


class _Recording:
    """A Session that records the programs it is asked to run.

    Answers by program name unless a test supplies one, because the two
    dispatchers convert what comes back into different values: a load row for
    one, a validation judgement for the other.

    ``position`` is the real Session's, not a stand-in: it is derived from the
    two facts a host supplies, and a fake that answered it directly could report
    a position its own answers contradict.
    """

    position = Session.position

    def __init__(self, answer=None):
        self.submitted = []
        self.answer = answer

    def workspace_or_default(self, workspace=None):
        if workspace is None:
            raise CommandError("this command needs a workspace")
        return workspace

    def executes_here(self, workspace=None):
        return False

    def execute_python(self, program, *, workspace=None, timeout=None):
        self.submitted.append(program)
        if self.answer is not None:
            return self.answer
        return _answer_for(program.name)


def _answer_for(name: str):
    """What the far side would have returned for one entry point."""

    from weaver.runtime.load_result import LoadResult
    from weaver.runtime.validation_result import TestResult

    if name == "run_python_primitive":
        return LoadResult(succeeded=True).as_row()
    if name == "run_validation_primitive":
        return {"result": TestResult().to_mapping(), "diagnostics": []}
    return True


def _fabric():
    return Workspace(
        workspace="My Workspace", catalogue="Warehouse/Weaver", environment="weaver"
    )


def _sources(session):
    return {program.name: program.source for program in session.submitted}


def _row():
    """A full load row, which is what a dispatch answers with.

    ``from_row`` is the inverse of ``as_row`` and takes every column, so a
    partial mapping here would fail in the scope rather than in the test.
    """

    from weaver.runtime.load_result import LoadResult

    return LoadResult(succeeded=True).as_row()


def _node():
    """One deployed-module node, as the Runner hands it to a scope."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.load_plan import LAKEHOUSE_TARGET, PhysicalObjectRef, PhysicalTargetRef
    from weaver.run.graph import RunNode

    return RunNode(
        node_id="load:Lakehouse/Sales/Sales.Customer",
        physical_target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name="Sales"),
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


def _validation():
    """One Lakehouse validation, as the Runner hands it to a scope."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.etl import validation_artefact_id
    from weaver.load_plan import LAKEHOUSE_TARGET, PhysicalTargetRef
    from weaver.test_plan import InstalledValidation

    item = WeaverItemId("Lakehouse", "Sales")
    source = ObjectId(schema="Sales", object="Customer")
    return InstalledValidation(
        logical=WeaverDocumentId(item, source),
        kind="Test",
        target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name="Sales"),
        # Through the one function that computes it, so this fixture cannot
        # describe an artefact a build would never claim.
        artefact=validation_artefact_id(item, "Test", source),
        object_type="file",
    )


def _dispatch(scope):
    scope.dispatch_python(
        _node(), expected_class="Sales__Customer", fault_tolerant=False
    )


def test_opening_a_scope_where_execution_is_local_needs_no_crossing():
    from weaver.runtime.python_context import RuntimeScope

    session = _Recording()
    session.executes_here = lambda workspace=None: True

    scope = open_runtime_scope(session, workspace=given_workspace())

    assert isinstance(scope, DirectRunScope)
    assert isinstance(scope.runtime_scope, RuntimeScope)
    assert session.submitted == []
    scope.close()


def test_opening_a_scope_where_execution_is_remote_begins_one_over_there():
    session = _Recording()

    scope = open_runtime_scope(session, workspace=_fabric())

    assert isinstance(scope, FabricRunScope)
    assert scope.run_id in _sources(session)["open_scope"]


def test_a_session_with_no_workspace_at_all_keeps_the_imports_here():
    """Positive knowledge is required to go remote: a scope opened over there by
    mistake would run the primitive somewhere the caller never named."""

    from weaver.runtime.python_context import RuntimeScope

    session = _Recording()
    scope = open_runtime_scope(session, workspace=None)

    assert isinstance(scope, DirectRunScope)
    assert isinstance(scope.runtime_scope, RuntimeScope)
    assert session.submitted == []
    scope.close()


def test_a_configuration_failure_is_not_mistaken_for_running_locally():
    """The narrow fallback above must stay narrow.

    Catching every ``CommandError`` from ``executes_here`` turned a bad
    configuration — or a Session someone had already closed — into a local
    RuntimeScope. The run then imported primitives into the console and reported
    success against an estate it had never reached.
    """

    class Broken(_Recording):
        def executes_here(self, workspace=None):
            raise CommandError("this session is closed")

    with pytest.raises(CommandError, match="closed"):
        open_runtime_scope(Broken(), workspace=_fabric())


def test_every_dispatch_names_the_run_whose_scope_it_belongs_to():
    session = _Recording(answer=_row())
    scope = open_runtime_scope(session, workspace=_fabric())

    _dispatch(scope)

    submitted = _sources(session)["run_python_primitive"]
    assert scope.run_id in submitted
    assert "Sales__Customer" in submitted


def test_the_submitted_program_builds_its_session_around_the_interpreters_spark():
    """The construction every other crossing performs. A Session built inside
    the call would have to go looking for an active Spark session rather than
    being handed the one the statement is running in."""

    session = _Recording(answer=_row())
    scope = open_runtime_scope(session, workspace=_fabric())
    _dispatch(scope)

    submitted = _sources(session)["run_python_primitive"]
    assert "NotebookSession(workspace=workspace, spark=spark)" in submitted
    assert submitted.index("workspace = Workspace") < submitted.index(
        "NotebookSession"
    ), "the workspace must be defined before the session that takes it"


@pytest.mark.parametrize(
    "name",
    [
        "open_scope",
        "run_python_primitive",
        "run_validation_primitive",
        "close_scope",
    ],
)
def test_every_submitted_program_is_valid_python(name):
    """A typo here is invisible to every local test and would ship a run that
    cannot reach Fabric at all."""

    session = _Recording()
    scope = open_runtime_scope(session, workspace=_fabric())
    _dispatch(scope)
    scope.dispatch_validation(_validation(), collect=True)
    scope.close()

    ast.parse(_sources(session)[name])


def test_closing_the_handle_ends_the_run_over_there():
    session = _Recording(answer=True)
    scope = open_runtime_scope(session, workspace=_fabric())

    scope.close()

    assert scope.run_id in _sources(session)["close_scope"]


def test_closing_twice_ends_the_run_once():
    session = _Recording(answer=True)
    scope = open_runtime_scope(session, workspace=_fabric())

    scope.close()
    scope.close()

    assert [one.name for one in session.submitted].count("close_scope") == 1


def test_a_cleanup_that_cannot_reach_the_session_does_not_fail_the_run():
    """If the Livy session is already gone, so is the scope — which is the
    outcome closing the scope exists to reach."""

    session = _Recording()
    scope = open_runtime_scope(session, workspace=_fabric())

    def gone(program, *, workspace=None, timeout=None):
        raise RuntimeError("the Livy session is gone")

    session.execute_python = gone

    scope.close()  # must not raise


# --- what dispatch does with each kind of scope -------------------------------


def test_the_scope_is_what_runs_a_python_node():
    """Dispatch hands the node to the scope and does not decide where it runs.

    It used to look for a ``dispatch_python`` attribute and fall through to
    importing here when it was absent, so a scope that did not quite conform
    silently imported a deployed module into the console.
    """

    from weaver.run.dispatch import dispatch_primitive
    from weaver.runtime.load_result import LoadResult

    sent = []

    class Scope:
        def dispatch_python(self, node, *, expected_class, fault_tolerant):
            sent.append((node, expected_class, fault_tolerant))
            # A row, which is what a scope answers with in either position.
            return LoadResult(succeeded=True, rows_read=3).as_row()

    node = _node()
    result = dispatch_primitive(
        node,
        session=_Recording(),
        resolved=type("R", (), {"expected_class": "Sales__Customer"})(),
        open_runtime=LazyRunScope(Scope),
    )

    assert sent == [(node, "Sales__Customer", False)]
    assert result.rows_read == 3


# --- preparing is not using ---------------------------------------------------


def test_a_warehouse_only_run_never_opens_a_runtime_scope():
    """The claim that keeps a declared requirement from becoming an acquisition.

    A run of nothing but stored procedures reaches no deployed module, so it
    needs no scope — and on a desktop, opening one means a Livy session and a
    scope-opening crossing for work that is entirely T-SQL.
    """

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.load_plan import WAREHOUSE_TARGET, PhysicalTargetRef
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
        open_runtime=LazyRunScope(lambda: opened.append(True) or object()),
    )

    assert opened == [], "a Warehouse-only run opened a runtime scope"


def test_a_warehouse_validation_opens_no_scope_either():
    """A Warehouse validation is a procedure, and TDS reaches it from here."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import (
        PROCEDURE_SHAPE,
        WeaverDocumentId,
        WeaverItemId,
    )
    from weaver.load_plan import WAREHOUSE_TARGET, PhysicalTargetRef
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
        node,
        session=Session(),
        open_runtime=LazyRunScope(lambda: opened.append(True) or object()),
    )

    assert opened == []


# --- closing a scope, and the difference between two failures -----------------


def _remote_scope(session):
    """One FabricRunScope over a recording Session, already begun."""

    return open_runtime_scope(session, workspace=_fabric())


def test_a_dead_interpreter_takes_its_scope_with_it_and_says_nothing():
    """There is nothing to report: closing the scope has already happened."""

    from weaver.fabric.livy import LivyError

    class Dying(_Recording):
        def __init__(self):
            super().__init__()
            self.warnings = []
            self.counted = []

        def execute_python(self, program, *, workspace=None, timeout=None):
            self.submitted.append(program)
            if program.name == "close_scope":
                raise LivyError("Livy session entered state 'dead'")
            return None

        def warn(self, message):
            self.warnings.append(message)

    session = Dying()
    scope = _remote_scope(session)
    scope.close()

    assert session.warnings == []


def test_a_live_session_that_could_not_release_a_scope_is_reported():
    """The opposite case, and the one that used to vanish.

    A healthy Livy answering with a protocol or serialisation failure means the
    scope is *still open over there*, holding this run's imported modules. The
    next run inherits them, and a rebuilt primitive silently does not take
    effect. The completed run still succeeds — it produced its result — but
    somebody is told.
    """

    class Clumsy(_Recording):
        def __init__(self):
            super().__init__()
            self.warnings = []

        def execute_python(self, program, *, workspace=None, timeout=None):
            self.submitted.append(program)
            if program.name == "close_scope":
                raise TypeError("close_scope() got an unexpected keyword argument")
            return None

        def warn(self, message):
            self.warnings.append(message)

    session = Clumsy()
    scope = _remote_scope(session)
    scope.close()  # does not raise: the run is finished

    assert len(session.warnings) == 1
    assert "not released" in session.warnings[0]
    assert "TypeError" in session.warnings[0]

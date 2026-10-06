"""Where a run's deployed Python is imported: where the run runs.

A deployed module is imported against the Spark of the process running the run,
and a run that executes one runs in Fabric. A client sends such a run there
whole, so a client opens no scope at all.

The scope is lazy: a run that reaches no deployed module, a Warehouse-only load
or a Warehouse validation, never opens one.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test
from support.workspaces import given_workspace

from weaver.errors import CommandError
from weaver.run.result import RunError
from weaver.run.runtime_boundary import (
    DirectRunScope,
    LazyRunScope,
    open_runtime_scope,
)
from weaver.sessions.base import Session
from weaver.workspaces import Workspace


class _Recording:
    """A Session that records the programs it is asked to run.

    ``position`` is the real Session's, not a stand-in: it is derived from the
    two facts a host supplies, and a fake that answered it directly could report
    a position its own answers contradict.
    """

    position = Session.position

    def __init__(self):
        self.submitted = []

    def workspace_or_default(self, workspace=None):
        if workspace is None:
            raise CommandError("this command needs a workspace")
        return workspace

    def executes_here(self, workspace=None):
        return False

    def execute_python(self, program, *, workspace=None, timeout=None):
        self.submitted.append(program)


def _fabric():
    return Workspace(
        workspace="My Workspace", catalogue="Warehouse/Weaver", environment="weaver"
    )


def _node():
    """One deployed-module node, as the Runner hands it to a scope."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.run.graph import RunNode
    from weaver.targets import LAKEHOUSE_TARGET, PhysicalObjectRef, PhysicalTargetRef

    return RunNode(
        node_id="load:Lakehouse/Sales/Tables/Sales.Customer",
        physical_target=PhysicalTargetRef(kind=LAKEHOUSE_TARGET, name="Sales"),
        primitive_kind="python_table",
        logical_id=WeaverDocumentId(
            WeaverItemId("Lakehouse", "Sales"),
            ObjectId(schema="Sales", object="Customer"),
        ),
        primitive_object=PhysicalObjectRef(
            target_id="Lakehouse/Sales",
            target_kind="lakehouse",
            schema="_/Load/Tables",
            object="Sales__Customer.py",
            object_type="file",
        ),
    )


def _validation():
    """One Lakehouse validation, as the Runner hands it to a scope."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.etl import validation_artefact_id
    from weaver.targets import LAKEHOUSE_TARGET, PhysicalTargetRef
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


@weaver_test()
def test_opening_a_scope_where_execution_is_local_needs_no_crossing():
    from weaver.runtime.python_context import RuntimeScope

    session = _Recording()
    session.executes_here = lambda workspace=None: True

    scope = open_runtime_scope(session, workspace=given_workspace())

    assert isinstance(scope, DirectRunScope)
    assert isinstance(scope.runtime_scope, RuntimeScope)
    assert session.submitted == []
    scope.close()


@weaver_test()
def test_a_client_opens_no_scope_because_the_run_goes_to_fabric():
    """A deployed module imported on a client would run against no Spark at all."""

    session = _Recording()

    with pytest.raises(RunError, match="sends it to Fabric"):
        open_runtime_scope(session, workspace=_fabric())

    assert session.submitted == []


@weaver_test()
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


@weaver_test()
def test_a_configuration_failure_is_not_mistaken_for_running_locally():
    """The narrow fallback above must stay narrow.

    Catching every ``CommandError`` from ``executes_here`` turned a bad
    configuration, or a Session someone had already closed, into a local
    RuntimeScope. The run then imported primitives into the console and reported
    success against an estate it had never reached.
    """

    class Broken(_Recording):
        def executes_here(self, workspace=None):
            raise CommandError("this session is closed")

    with pytest.raises(CommandError, match="closed"):
        open_runtime_scope(Broken(), workspace=_fabric())


@weaver_test()
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
        def dispatch_python(
            self,
            node,
            *,
            expected_class,
            fault_tolerant,
            reload,
            ignore_stability_threshold=False,
            isolated=False,
        ):
            sent.append((node, expected_class, fault_tolerant, reload, isolated))
            # A row, which is what a scope answers with in either position.
            return LoadResult(succeeded=True, rows_read=3).as_row()

    node = _node()
    result = dispatch_primitive(
        node,
        session=_Recording(),
        resolved=type("R", (), {"expected_class": "Sales__Customer"})(),
        open_runtime=LazyRunScope(Scope),
    )

    assert sent == [(node, "Sales__Customer", False, False, False)]
    assert result.rows_read == 3


@weaver_test()
def test_a_warehouse_only_run_never_opens_a_runtime_scope():
    """The claim that keeps a declared requirement from becoming an acquisition.

    A run of nothing but stored procedures reaches no deployed module, so it
    needs no scope, and on a desktop, opening one means a Livy session and a
    scope-opening crossing for work that is entirely T-SQL.
    """

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import WeaverDocumentId, WeaverItemId
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode
    from weaver.targets import WAREHOUSE_TARGET, PhysicalTargetRef

    opened = []

    class Sql:
        def call_procedure(self, name, *, inputs, outputs):
            # Zero for every count, and no bookmark instant: this stub stands in
            # for the transport, so it answers in the shapes the transport does.
            return {
                output[0]: None if output[-1].startswith("datetime") else 0
                for output in outputs
            }

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


@weaver_test()
def test_a_warehouse_validation_opens_no_scope_either():
    """A Warehouse validation is a procedure, and TDS reaches it from here."""

    from weaver.declaration.metadata import ObjectId
    from weaver.declaration.model import (
        PROCEDURE_SHAPE,
        WeaverDocumentId,
        WeaverItemId,
    )
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode
    from weaver.targets import WAREHOUSE_TARGET, PhysicalTargetRef
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


@weaver_test()
def test_a_lakehouse_validation_beside_others_asks_for_a_session_of_its_own():
    from weaver.run.dispatch import dispatch_primitive
    from weaver.run.graph import RunNode
    from weaver.runtime.validation_result import TestResult

    installed = _validation()
    asked = []

    class Scope:
        def dispatch_validation(self, validation, *, collect, isolated=False):
            asked.append((validation, collect, isolated))
            return TestResult()

    node = RunNode(
        node_id="test:Lakehouse/Sales/Sales.Customer",
        physical_target=installed.target,
        primitive_kind="python_validation",
        logical_id=installed.logical,
        installed=installed,
    )

    for isolated in (False, True):
        dispatch_primitive(
            node,
            session=_Recording(),
            open_runtime=LazyRunScope(Scope),
            isolated=isolated,
        )

    assert asked == [(installed, False, False), (installed, False, True)]
